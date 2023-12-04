package scripts.tpcds

import org.apache.spark.sql.SparkSession
import tools.ArthasRun
import tpcds.action.{QueryRunProfile, TPCDSAction, TPCDSQueryRunAction}
import java.text.SimpleDateFormat
import java.util.Date

class TPCDSNArrowRegressionTest(spark: SparkSession) {

  val metaDb = "zhnwang_tpcds_meta"
  val qset = "tpcds"

  val queryAction = new TPCDSQueryRunAction(spark, metaDb)

  def runQuery(qid: String): Unit = {
    val icePname = s"20230830-narrow-regression-sf1-iceberg"
    queryAction.runProfileQid(icePname, qid)
  }

  def runFlameCompare(): Unit = {

    import org.apache.spark.sql.SparkSession
    import tools.ArthasRun
    import tpcds.action.{QueryRunProfile, TPCDSAction, TPCDSQueryRunAction}
    import java.text.SimpleDateFormat
    import java.util.Date

    val query = "select sum(col) from reader_perf_iceberg_tests2.float_t"
    val path1 = "/var/tmp/fl_float_t_iceberg.html"
    spark.sql("set `spark.sql.iceberg.parquet.use-arrow-native-reader`=`false`")
    ArthasRun.makeFlameGraph(path1).runProfile(() => {
      spark.sql(query).show()
    })

    ArthasRun.makeTrace("org.apache.iceberg.spark.source.BatchDataReader open").runAsync(() => {
      spark.sql(query).show()
    })



    val path2 = "/var/tmp/fl_float_t_narrow.html"
    spark.sql("set `spark.sql.iceberg.parquet.use-arrow-native-reader`=`true`")
    ArthasRun.makeFlameGraph(path2).runProfile(() => {
      spark.sql(query).show()
    })

//    ArthasRun.makeTrace("org.apache.iceberg.spark.source.BatchDataReader open").runAsync(() => {
//      spark.sql(query).show()
//    })

    ArthasRun.makeTrace("org.apache.spark.sql.execution.datasources.parquet.ArrowNativeParquet$ReadBuilder build").runAsync(() => {
      spark.sql(query).show()
    })


    ArthasRun.makeDecompile("org.apache.spark.sql.execution.datasources.parquet.ArrowNativeParquet$ReadBuilder").runAsync(() => {
      println("hello")
    })


  }

  def runTestQueries(): Unit = {

    val query1 = "select sum(col) from reader_perf_iceberg_tests2.float_t"

    spark.sql("set `spark.sql.iceberg.parquet.use-arrow-native-reader`=`false`")
    spark.sql("set `spark.sql.iceberg.parquet.use-spark-native-reader`=`true`")
    spark.sql("set `spark.sql.iceberg.vectorization.enabled`=`true`")
    spark.sql(query1).show()

    val query2 = "select sum(col) from reader_perf_iceberg_tests2.float_t"
    spark.sql("set `spark.sql.iceberg.parquet.use-arrow-native-reader`=`true`")
    spark.sql("set `spark.sql.iceberg.parquet.use-spark-native-reader`=`false`")
    spark.sql("set `spark.sql.iceberg.vectorization.enabled`=`false`")
    spark.sql(query2).show()

  }

  def runRegression(): Unit = {
    val date = new SimpleDateFormat("yyyyMMdd").format(new Date)

    val conf = TPCDSAction.getSparkConf
    conf.setAppName(s"TPC-DS_Regression_Run_NArrow_$date")
    val qa = new TPCDSQueryRunAction(spark, metaDb)

    val hivePname = s"${date}-narrow-regression-sf1-hive-parquet"
    val hiveDb = "tpcds_sf1_hive_parquet"
    val hprofile = QueryRunProfile(hivePname, hiveDb, qset, 0, 1, "{}", "")
    qa.createQueryRunProfileIfNotExist(hprofile)
    println(s"Created Profile $hprofile")

    val icePname = s"${date}-narrow-regression-sf1-iceberg"
    val iceDb = "tpcds_sf1_iceberg"
    val iprofile = QueryRunProfile(icePname, iceDb, qset, 0, 1, "{}", "")
    qa.createQueryRunProfileIfNotExist(iprofile)
    println(s"Created Profile $iprofile")

    qa.regression(hivePname, icePname)
  }
}
