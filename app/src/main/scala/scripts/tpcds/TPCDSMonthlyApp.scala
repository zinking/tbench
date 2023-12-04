package scripts.tpcds

import org.apache.spark.sql.SparkSession
import tpcds.action.{MySQLAction, QueryRunProfile, TPCDSAction, TPCDSQueryRunAction}

import java.text.SimpleDateFormat
import java.util.Date

class TPCDSMonthlyApp(spark: SparkSession) {
  val regular = new TPCDSRegular(spark)

  def main(args: Array[String]): Unit = {
    runA03Monthly
  }

  /**
   * running these profiles
   * partition: hive vs parquet 3 times each
   * non-partition: hive vs parquet 3 times each
   */
  def runA03Monthly(): Unit = {
    val month = new SimpleDateFormat("yyyyMM").format(new Date)
    val suffix = s"tm-$month"
    regular.createRun03Profiles(suffix)
    regular.run03Profiles(suffix)
  }

  def runMonthly(): Unit = {
    val conf = TPCDSAction.getSparkConf
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate();

    conf.setAppName("TPC-DS Monthly Run")

    val icebergDb = "tpcds_sf1000_iceberg"
    val hiveDb = "tpcds_sf1000_iceberg"
    val month = new SimpleDateFormat("yyyyMM").format(new Date)
    val iceProfile = s"${icebergDb}_${month}"
    val hiveProfile = s"${hiveDb}_${month}"
    val queryAction = new TPCDSQueryRunAction(spark, "zhnwang_tpcds_meta")
    runMonthlyProfile(iceProfile, icebergDb, queryAction)
    runMonthlyProfile(hiveProfile, hiveDb, queryAction)
    val df = queryAction.compareProfileRuns(iceProfile, hiveProfile)

    val mysqlAction = new MySQLAction("config/benchmark_db.json")
    println("saving bench result to mysql")
    mysqlAction.saveDataFrame(df)
  }

  def runMonthlyProfile(profileName: String, db: String, action: TPCDSQueryRunAction): Unit = {
    action.getProfile(profileName) match {
      case None =>
        val profile = QueryRunProfile(profileName, db, "tpcds", 1, 3, "{}", "")
        action.createQueryRunProfile(profile)
      case _ =>
        println("runMonthlyProfile _")
    }
    action.runProfileQueries(profileName)
  }
}
