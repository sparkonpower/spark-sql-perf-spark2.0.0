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

case class RunTPCConfig(
    benchmarkName: String = null,
    filter: Option[String] = None,
    iterations: Int = 1,
    dbname: String = "tpcds1g1",
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
     //   .setMaster("local[*]")
     //   .setAppName(getClass.getName)

    val sc = SparkContext.getOrCreate(conf)

    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    import sqlContext.implicits._

  //  val sqlContext = SQLContext.getOrCreate(sc)
  //  import sqlContext.implicits._

	//sqlContext.sql(s"USE tpcds1g1")
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

  //  benchmark.allQueries.foreach(println)
//    val allQueries = config.filter.map { f =>
//      benchmark.allQueries.filter(_.name contains f)
//    } getOrElse {
//      benchmark.allQueries
//    }

    val tpcds = new TPCDS ()

    val allQueries = config.filter.map { f =>
        if(f.equals("all")) {
          tpcds.tpcds1_4Queries
        } else {
          tpcds.tpcds1_4Queries.filter(_.name contains f)
        }
      } getOrElse {
        tpcds.tpcds1_4Queries
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
    experiment.waitForFinish(1000 * 60 * 30)

    sqlContext.setConf("spark.sql.shuffle.partitions", "1")
    experiment.getCurrentRuns()
        .withColumn("result", explode($"results"))
        .select("result.*")
        .groupBy("name")
        .agg(
          min($"executionTime") as 'minTimeMs,
          max($"executionTime") as 'maxTimeMs,
          avg($"executionTime") as 'avgTimeMs,
          stddev($"executionTime") as 'stdDev)
        .orderBy("name")
        .show(truncate = false)
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

      data.show(truncate = false)
    }
  }
}
