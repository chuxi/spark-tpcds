package cn.edu.zju.king

import com.databricks.spark.sql.perf.tpcds.{TPCDS, TPCDSTables}
import org.apache.spark.sql.SparkSession


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
    val format = "parquet"

    val sqlContext = spark.sqlContext

    val tables = new TPCDSTables(sqlContext, dsdgenDir, scaleFactor, useDoubleForDecimal = true, useStringForDate = true)

    tables.genData(location, format, overwrite = true, partitionTables = false, clusterByPartitionColumns = true, filterOutNullPartitionValues = false, tableFilter = "", numPartitions = 32)

//    tables.createExternalTables(location, format, databaseName, overwrite = true, discoverPartitions = true)

    tables.createTemporaryTables(location, format)

    val iterations = 1
    val timeout = 24 * 60 * 60

    val tpcds = new TPCDS(spark.sqlContext)

    val experiment = tpcds.runExperiment(tpcds.tpcds1_4Queries, iterations = iterations)
    experiment.waitForFinish(timeout)

    spark.stop()
  }
}
