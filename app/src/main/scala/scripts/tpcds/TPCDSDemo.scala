package scripts.tpcds

import org.apache.spark.sql.SparkSession
import tools.TableTools
import tpcds.action.{TPCDSQueryRunAction, TPCDSTableAction}

class TPCDSDemo(spark: SparkSession) {
  val hiveProfile = "sf1000-hive-parquet-test-3"
  val iceProfile = "sf1000-iceberg-test-3"
  val hivePProfile = "sf1000p-hive-parquet-test-3"
  val icePProfile = "sf1000p-iceberg-test-3"
  val meta = "zhnwang_tpcds_meta"
  val rssClusterRoot = "hdfs:///user/zhnwang/"
  val backupRoot = "hdfs://9.192.183.183:9000/user/zhnwang/tmp/meta"

  val qa = new TPCDSQueryRunAction(spark, meta)
  val taRss = new TPCDSTableAction(spark, meta, true, rssClusterRoot)

  def runQuery3(query: String): Unit = {
    qa.runProfileQuery(hiveProfile, query)
    qa.runProfileQuery(iceProfile, query)
  }

  def runExplainQuery3(query: String): Unit = {
    qa.runProfileQuery(hiveProfile, query, true)
    qa.runProfileQuery(iceProfile, query, true)
  }


  def run3(qid: String): Unit = {
    qa.runProfileQid(hiveProfile, qid)
    qa.runProfileQid(iceProfile, qid)
  }

  def runExplain3(qid: String): Unit = {
    qa.runProfileQid(hiveProfile, qid, true)
    qa.runProfileQid(iceProfile, qid, true)
  }

  def runP3(qid: String): Unit = {
    qa.runProfileQid(hivePProfile, qid)
    qa.runProfileQid(icePProfile, qid)
  }

  def runExplainP3(qid: String): Unit = {
    qa.runProfileQid(hivePProfile, qid)
    qa.runProfileQid(icePProfile, qid)
  }

  def regress(qid: String): Unit = {
    qa.regression(hiveProfile, iceProfile, qid)
  }

  def createRssClusterTests(): Unit = {
    taRss.createTPCDSMeta()
    taRss.createDatabase(1000, "iceberg", "", false)
    taRss.createDatabase(1000, "iceberg", "", true)
    taRss.createDatabase(1000, "parquet", "hive", false)
    taRss.createDatabase(1000, "parquet", "hive", true)
  }

  def backupMeta(): Unit = {
    taRss.exportMetaDb(backupRoot)
  }

  def main(args: Array[String]): Unit = {
    val taction = new TPCDSTableAction(spark, "zhnwang_tpcds_meta")
    taction.createDatabase(1000, "parquet", "hive", true)


    val ta = new TPCDSTableAction(spark, "zhnwang_tpcds_meta")
    val template = "ALTER TABLE tpcds_sf1000_iceberg.VAR_TABLE  SET TBLPROPERTIES ('read.parquet.vectorization.enabled'='true')"
    ta.runTpcdsTableDDLs("tpcds_sf1000_iceberg", template)

    spark.sparkContext.setLogLevel("INFO")

    new TableTools(spark, "tpcds_sf1000_hive_parquet").listTableDirs("store_sales")

    val profiles = Array("sf1000p-iceberg-tm6-0", "sf1000-iceberg-tm6-0", "sf1000p-hive-parquet-tm6-0", "sf1000-hive-parquet-tm6-0")
    val tpcdsAction = new TPCDSQueryRunAction(spark, "zhnwang_tpcds_meta")
    profiles.foreach { name =>
      tpcdsAction.runProfileQueries(name)
    }


  }

}
