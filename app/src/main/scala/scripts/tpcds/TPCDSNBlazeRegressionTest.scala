package scripts.tpcds

import org.apache.spark.sql.SparkSession
import tpcds.action.{QueryRunProfile, TPCDSAction, TPCDSQueryRunAction}

import java.text.SimpleDateFormat
import java.util.Date

class TPCDSNBlazeRegressionTest(spark: SparkSession) {
  val metaDb = "zhnwang_tpcds_meta"
  val qset = "tpcds"
  val expName = "nblaze"
  val readFloatQuery = "select sum(col) from reader_perf_iceberg_tests2.float_t"

  val queryAction = new TPCDSQueryRunAction(spark, metaDb)

  def runQuery(qid: String): Unit = {
    val icePname = profileName(true, 1)
    queryAction.runProfileQid(icePname, qid)
  }

  def switchFlag(on: Boolean = true): Unit = {
    if (on) {
      spark.sql("set `spark.blaze.enable`=`true`")
    } else {
      spark.sql("set `spark.blaze.enable`=`false`")
    }
  }

  def sparkConf(on: Boolean = true): String = {
    if (on) {
      """{"spark.blaze.enable":"true"}"""
    } else {
      """{"spark.blaze.enable":"false"}"""
    }
  }

  def profileName(on: Boolean = true, scaleFactor: Int): String = {
    s"tpcds-sf${scaleFactor}-iceberg-native-${expName}-${on}"
  }

  def flamePath(on: Boolean = true): String = {
    s"/var/tmp/flame/fl_float_t_${expName}_${on}.html"
  }

  def runFlameCompare(): Unit = {
    import tools.ArthasRun

    switchFlag(true)
    ArthasRun.makeFlameGraph(flamePath(true)).runProfile(() => {
      spark.sql(readFloatQuery).show()
    })

    ArthasRun.makeTrace("org.apache.iceberg.spark.source.BatchDataReader open").runAsync(() => {
      spark.sql(readFloatQuery).show()
    })

    switchFlag(false)
    ArthasRun.makeFlameGraph(flamePath(false)).runProfile(() => {
      spark.sql(readFloatQuery).show()
    })
  }

  def runTestQueries(): Unit = {
    switchFlag(true)
    spark.sql(readFloatQuery).show()

    switchFlag(false)
    spark.sql(readFloatQuery).show()
  }

  def runRegression(scaleFactor: Int): Unit = {
    val date = new SimpleDateFormat("yyyyMMdd").format(new Date)

    val conf = TPCDSAction.getSparkConf
    conf.setAppName(s"TPC-DS_Regression_Run_${expName}_$date")
    val qa = new TPCDSQueryRunAction(spark, metaDb)

    val icePName = profileName(false, scaleFactor)
    val iceDb = s"tpcds_sf${scaleFactor}_iceberg"
    val iceProfile = QueryRunProfile(icePName, iceDb, qset, 0, 1, sparkConf(false), "")
    qa.createQueryRunProfileIfNotExist(iceProfile)
    println(s"Created Profile $iceProfile")

    val blazePName = profileName(true, scaleFactor)
    val blazeDb = s"tpcds_sf${scaleFactor}_iceberg"
    val blazeProfile = QueryRunProfile(blazePName, blazeDb, qset, 0, 1, sparkConf(true), "")
    qa.createQueryRunProfileIfNotExist(blazeProfile)
    println(s"Created Profile $blazeProfile")

    qa.regression(icePName, blazePName)
  }

  def runRegressionSf1(): Unit = runRegression(1)

  def runRegressionSf500(): Unit = {
    // tpcds_iceberg_500
    runRegression(500)
  }
}
