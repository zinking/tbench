package org.apache.spark.sql

import org.apache.iceberg.CatalogUtil
import org.apache.iceberg.hive.{HiveCatalog, TestHiveMetastore}
import org.apache.iceberg.spark.SparkTestBase
import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
import org.apache.spark.sql.TPCDSResource.{tableColumns, tableNames, tpcdsQueries}
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.internal.SQLConf

import java.util

object TPCDSSuites extends SparkTestBase {

  def buildLocalEnv(): Unit = {
    SparkTestBase.metastore = new TestHiveMetastore
    SparkTestBase.metastore.start()
    SparkTestBase.hiveConf = SparkTestBase.metastore.hiveConf

    SparkTestBase.spark = SparkSession.builder
      .master("local[2]")
      .config("spark.testing", "true")
      .config(SQLConf.PARTITION_OVERWRITE_MODE.key, "dynamic")
      .config("spark.sql.extensions", classOf[IcebergSparkSessionExtensions].getName)
      .config("spark.hadoop.hive.metastore.uris", SparkTestBase.hiveConf.get("hive.metastore.uris"))
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.hive.metastorePartitionPruningFallbackOnException", "true")
      .enableHiveSupport
      .getOrCreate

    SparkTestBase.catalog = CatalogUtil.loadCatalog(
      classOf[HiveCatalog].getName, "hive", new java.util.HashMap[String, String](), SparkTestBase.hiveConf
    ).asInstanceOf[HiveCatalog]

  }

  def createTable(
    spark: SparkSession, tableName: String, format: String = "iceberg", options: Seq[String] = Nil): Unit = {
    spark.sql(
      s"""
         |CREATE TABLE `$tableName` (${tableColumns(tableName)})
         |USING $format
         |${options.mkString("\n")}
       """.stripMargin)
  }

  def createTables(): Unit = {
    tableNames.foreach { tableName =>
      createTable(SparkTestBase.spark, tableName)
    }
  }

  def runquery(): Unit = {
    tpcdsQueries.filter( x => x.equals("q10")).foreach { name =>
      val queryString = resourceToString(s"tpcds/q10.sql",
        classLoader = Thread.currentThread().getContextClassLoader)
      SparkTestBase.spark.sql(queryString).collect()
    }
  }


  def main(args: Array[String]): Unit = {
    buildLocalEnv()
    createTables()
    runquery()
    println("ok")
  }


}
