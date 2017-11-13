package cn.edu.zju.king

import java.io.{File, FileOutputStream}

import com.databricks.spark.sql.perf.tpcds.{TPCDS, TPCDSTables}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Setup {

  def main(args: Array[String]): Unit = {
    val dsdgenDir = "/home/king/gits/tpcds-kit/tools"

    val spark = SparkSession.builder()
      .config("spark.master", "local[2]")
      .config("spark.sql.test", "")
      .config("spark.sql.warehouse.dir", "/tmp/warehouse")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.perf.results", "/tmp/tpcds/result")
      .enableHiveSupport()
      .appName("TestSQLContext").getOrCreate()

    val location = "/tmp/tpcds/data"
    val databaseName = "benchmark"
    val scaleFactor = "1"
    val format = "windjammer"

    val sqlContext = spark.sqlContext

    val tables = new TPCDSTables(sqlContext, dsdgenDir, scaleFactor, useDoubleForDecimal = true, useStringForDate = true)

    tables.genData(location, format, overwrite = true, partitionTables = false, clusterByPartitionColumns = true, filterOutNullPartitionValues = false, tableFilter = "", numPartitions = 32)

    tables.createExternalTables(location, format, databaseName, overwrite = true, discoverPartitions = false)

    // query
    val iterations = 1
    val timeout = 24 * 60 * 60

    spark.sql(s"use $databaseName")

    val tpcds = new TPCDS(spark.sqlContext)

    val experiment = tpcds.runExperiment(tpcds.targetedPerfQueries, iterations = iterations)
    experiment.waitForFinish(timeout)

    val summary = experiment.getCurrentResults()
      .withColumn("Name", substring(col("name"), 2, 100))
      .withColumn("Runtime", (col("parsingTime") + col("analysisTime") + col("optimizationTime") + col("planningTime") + col("executionTime")) / 1000.0)
      .select("Name", "Runtime", "tables", "failure")

    // write to local file
    import java.io.PrintWriter
    val writer = new PrintWriter(s"$location/currentResult.txt", "UTF-8")
    summary.collect().foreach { row =>
      row.toSeq.foreach { cell =>
        writer.print(cell)
        writer.print("\t")
      }
      writer.println()
    }
    writer.close()

    summary.show(numRows = 1000)

    spark.stop()
  }
}
