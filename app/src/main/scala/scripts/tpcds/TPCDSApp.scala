package scripts.tpcds

import org.apache.spark.sql.SparkSession
import tpcds.action.{TPCDSAction, TPCDSQueryRunAction}

object TPCDSApp {

  def main(args: Array[String]) {
    assert(args.length > 0, "profiles required to run")
    runQueryProfiles(args)
  }

  def runQueryProfiles(profiles: Array[String]): Unit = {
    val conf = TPCDSAction.getSparkConf
    conf.setAppName("TPC-DS Profile Run")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate();

    val tpcdsAction = new TPCDSQueryRunAction(spark, "zhnwang_tpcds_meta")
    profiles.foreach { name =>
      tpcdsAction.runProfileQueries(name)
    }

  }
}
