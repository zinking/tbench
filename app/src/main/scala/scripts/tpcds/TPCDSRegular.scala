package scripts.tpcds

import org.apache.spark.sql.SparkSession
import tpcds.action.{QueryRunProfile, TPCDSQueryRunAction}

class TPCDSRegular(spark: SparkSession) {
  val metaDb = "zhnwang_tpcds_meta"
  val qset = "tpcds"
  val qa = new TPCDSQueryRunAction(spark, metaDb)

  val defaultEntries = Seq(
    ("sf1000p-hive-parquet", "tpcdsp_sf1000_hive_parquet"),
    ("sf1000p-iceberg", "tpcdsp_sf1000_iceberg"),
    ("sf1000-hive-parquet", "tpcds_sf1000_hive_parquet"),
    ("sf1000-iceberg", "tpcds_sf1000_iceberg")
  )

  def createRun03Profiles(suffix: String): Unit = {
    defaultEntries.foreach { case (pname, db) =>
      val profile = QueryRunProfile(pname + "-" + suffix, db, qset, 0, 3, "{}", "")
      qa.createQueryRunProfileIfNotExist(profile)
      println(s"Created Profile $profile")
    }
  }

  def run03Profiles(suffix: String): Unit = {
    defaultEntries.foreach { case (pname, _) =>
      val profile = pname + "-" + suffix
      qa.runProfileQueries(profile)
    }
  }

  def run03ExplainProfiles(suffix: String): Unit = {
    defaultEntries.foreach { case (pname, _p) =>
      val profile = pname + "-" + suffix
      qa.runProfileQueries(profile, true)
    }
  }

}
