/*
 * Copyright 2015 Databricks Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databricks.spark.sql.perf

import java.net.InetAddress

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkContext, SparkConf}
import com.databricks.spark.sql.perf.tpcds.TPCDS
import scala.util.Try
import java.util.concurrent.TimeoutException

case class sparkConfigMap(configName: String, value: String)

case class RunTPCConfig(
    benchmarkName: String = null,
    filter: Option[String] = None,
    iterations: Int = 10000,
    dbname: String = "tpcds1g1",
    timeout: Int = 1000 * 60 * 30,
    baseline: Option[Long] = None)

/**
 * Runs a benchmark locally and prints the results to the screen.
 */
object RunTPCBenchmark {
  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[RunTPCConfig]("spark-sql-perf") {
      head("spark-sql-perf", "0.2.0")
      opt[String]('b', "benchmark")
        .action { (x, c) => c.copy(benchmarkName = x) }
        .text("the name of the benchmark to run")
        .required()
      opt[String]('f', "filter")
        .action((x, c) => c.copy(filter = Some(x)))
        .text("a filter on the name of the queries to run")
      opt[Int]('i', "iterations")
        .action((x, c) => c.copy(iterations = x))
        .text("the number of iterations to run")
      opt[String]('d', "dbname")
        .action((x, c) => c.copy(dbname = x))
        .text("the database name to run SQL queries")
      opt[Int]('t', "timeout")
          .action((x, c) => c.copy(timeout = x))
          .text("the timeout period for query execution")
      opt[Long]('c', "compare")
          .action((x, c) => c.copy(baseline = Some(x)))
          .text("the timestamp of the baseline experiment to compare with")
      help("help")
        .text("prints this usage text")
    }

    parser.parse(args, RunTPCConfig()) match {
      case Some(config) =>
        run(config)
      case None =>
        System.exit(1)
    }
  }

  def run(config: RunTPCConfig): Unit = {
    val conf = new SparkConf()

    val sc = SparkContext.getOrCreate(conf)

    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    import sqlContext.implicits._

    sqlContext.sql(s"USE ${config.dbname}")

    sqlContext.setConf("spark.sql.perf.results", new java.io.File("performance").toURI.toString)
    val benchmark = Try {
      Class.forName(config.benchmarkName)
          .newInstance()
          .asInstanceOf[Benchmark]
    } getOrElse {
	println("Class.forName ELSE ")
      Class.forName("com.databricks.spark.sql.perf." + config.benchmarkName)
          .newInstance()
          .asInstanceOf[Benchmark]
    }

    val tpcds = new TPCDS ()

    val allQueries = config.filter match {
      case Some(f) => {
          if(f.equals("all")) {
            tpcds.tpcds1_4Queries
          } else {
            val queries: Seq[Query] = f.split(",").flatMap({ query =>
                val fullqueryname = query.trim + "-v1.4"
                tpcds.tpcds1_4Queries.filter(_.name equals fullqueryname)
            })
            queries
          }
      }
      case None => tpcds.tpcds1_4Queries
    }

    println("== QUERY LIST ==")
    allQueries.foreach(q => println(q.name))
    println("== QUERY LIST END ==")

    val experiment = benchmark.runExperiment(
      executionsToRun = allQueries,
      iterations = config.iterations,
      tags = Map(
        "runtype" -> "cluster",
        "host" -> InetAddress.getLocalHost().getHostName()))

    println("== STARTING EXPERIMENT ==")
    try {
        experiment.waitForFinish(config.timeout)
    } catch {
      case e: TimeoutException => println(s"Got Timed out after (${config.timeout} ms) !!")
      case _: Throwable => println("Got Killed with other exception ")
    }

    sqlContext.setConf("spark.sql.shuffle.partitions", "1")

    val exec = sc.getConf.get("spark.executor.instances","1")
    val cores = sc.getConf.get("spark.executor.cores","1")
    val mem = sc.getConf.get("spark.executor.memory","1G")

    experiment.getCurrentRuns()
        .withColumn("result", explode($"results"))
        .select("result.*")
        .groupBy("name")
        .agg(
          min($"executionTime") as 'minTimeMs,
          max($"executionTime") as 'maxTimeMs,
          avg($"executionTime") as 'avgTimeMs,
          stddev($"executionTime") as 'stdDev,
          count($"executionTime") as 'count,
          lit(exec) as 'exec,
          lit(cores) as 'cores,
          lit(mem) as 'mem )
        .orderBy("name")
        .show(200, truncate = false)

    experiment.getCurrentRuns()
        .withColumn("result", explode($"results")).select("result.*").select("name", "executionTime")
        .orderBy("name")
        .show(2000, truncate = false)

    val sparkConfUdf = udf((kvs:Map[String, String]) =>  {
      val x: Seq[sparkConfigMap] = kvs.map(kv => {
        sparkConfigMap(kv._1, kv._2)
      }).toSeq
      x
    })

    val filterFields = List("spark.executor.instances",
                            "spark.executor.extraJavaOptions",
                            "spark.driver.memory",
                            "spark.executor.memory",
                            "spark.executor.cores",
                            "spark.yarn.executor.memoryOverhead",
                            "spark.sql.shuffle.partitions")
    
    experiment.getCurrentRuns()
      .select("configuration.sparkConf").withColumn("cfg", sparkConfUdf($"sparkConf"))
      .withColumn("cfgflat", explode($"cfg")).select("cfgflat.*").dropDuplicates()
      .filter($"configName" isin (filterFields:_*))
      .show(200, truncate = false)

    println(s"""Results: sqlContext.read.json("${experiment.resultPath}")""")

    config.baseline.foreach { baseTimestamp =>
      val baselineTime = when($"timestamp" === baseTimestamp, $"executionTime").otherwise(null)
      val thisRunTime = when($"timestamp" === experiment.timestamp, $"executionTime").otherwise(null)

      val data = sqlContext.read.json(benchmark.resultsLocation)
          .coalesce(1)
          .where(s"timestamp IN ($baseTimestamp, ${experiment.timestamp})")
          .withColumn("result", explode($"results"))
          .select("timestamp", "result.*")
          .groupBy("name")
          .agg(
            avg(baselineTime) as 'baselineTimeMs,
            avg(thisRunTime) as 'thisRunTimeMs,
            stddev(baselineTime) as 'stddev)
          .withColumn(
            "percentChange", ($"baselineTimeMs" - $"thisRunTimeMs") / $"baselineTimeMs" * 100)
          .filter('thisRunTimeMs.isNotNull)

      //data.show(truncate = false)
      data.show(200, false)
    }
  }
}
