package scripts.tpcds

import org.apache.spark.sql.SparkSession
import tpcds.action.{QueryRunProfile, TPCDSAction, TPCDSQueryRunAction}

import java.text.SimpleDateFormat
import java.util.Date

object TPCDSMonthlyRegression {

  val metaDb = "zhnwang_tpcds_meta"
  val qset = "tpcds"


  def main(args: Array[String]) {
    runRegression()
  }

  def runRegression(): Unit = {
    val month = new SimpleDateFormat("yyyyMM").format(new Date)

    val conf = TPCDSAction.getSparkConf
    conf.setAppName(s"TPC-DS_Regression_Run_$month")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    val qa = new TPCDSQueryRunAction(spark, metaDb)

    val hivePname = s"${month}-regression-sf1000-hive-parquet"
    val hiveDb = "tpcds_sf1000_hive_parquet"
    val hprofile = QueryRunProfile(hivePname, hiveDb, qset, 0, 1, "{}", "")
    qa.createQueryRunProfileIfNotExist(hprofile)
    println(s"Created Profile $hprofile")

    val icePname = s"${month}-regression-sf1000-iceberg"
    val iceDb = "tpcds_sf1000_iceberg"
    val iprofile = QueryRunProfile(icePname, iceDb, qset, 0, 1, "{}", "")
    qa.createQueryRunProfileIfNotExist(iprofile)
    println(s"Created Profile $iprofile")

    qa.regression(hivePname, icePname)


  }
}
