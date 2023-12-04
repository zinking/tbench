package scripts.tpcds

import org.apache.spark.sql.{SaveMode, SparkSession}
import tpcds.action.{MySQLAction, TPCDSAction}

import java.text.SimpleDateFormat
import java.util.Date

object TPCDSRecentRuns {
  val metaDb = "zhnwang_tpcds_meta"
  val qset = "tpcds"


  def runStats(spark: SparkSession, ts: String): Unit = {
    val runStatQuery =
      s"""
        |with input as (
        |select
        |    split(profile_name, '-')[2] as pf,
        |    split(profile_name, '-')[0] as dt,
        |    split(profile_name, '-')[3] as fmt,
        |    qset,
        |    qid,
        |    run
        |from
        |    zhnwang_tpcds_meta.query_run
        |where
        |    profile_name like '202_____-%'
        |order by
        |    split(profile_name, '-')[0] desc
        |)
        |select
        |    $ts as ts,
        |    pf as profile,
        |    qid,
        |    fmt,
        |    collect_list(run) as runs,
        |    collect_list(dt) as runt,
        |    qset,
        |    '' as extra
        |from
        |    input
        |group by
        |    pf, qid, fmt, qset
        |;
        |""".stripMargin

    val df = spark.sql(runStatQuery)

    df.write
      .format("iceberg")
      .mode(SaveMode.Overwrite)
      .save("zhnwang_tpcds_meta.tpcds_recent_runs")

    println("pre persist rr into iceberg")
    df.createOrReplaceTempView("temp_recent_runs")
    println("done persist rr into iceberg")

    val mysqlQuery =
      """
        |select
        |    cast(ts as string) as ts, profile, qid, fmt, runt[0] as last_run_ts,
        |    runs[0] as run0, runs[1] as run1, runs[2] as run2, runs[3] as run3, runs[4] as run4,
        |    runs[5] as run5, runs[6] as run6, runs[7] as run7, runs[8] as run8, runs[9] as run9,
        |    runs[10] as run10, runs[11] as run11, runs[12] as run12, extra
        |from
        |    temp_recent_runs
        |;
        |""".stripMargin
    val myDf = spark.sql(mysqlQuery)

    val mysqlAction = new MySQLAction("config/benchmark_rr.json")
    println("pre saving bench result to mysql")
    mysqlAction.saveDataFrame(myDf)
    println("done saving bench result to mysql")
  }


  def main(args: Array[String]) {
    val day = new SimpleDateFormat("yyyyMMdd").format(new Date)
    val conf = TPCDSAction.getSparkConf
    conf.setAppName(s"TPC-DS_RECENT_RUN_$day")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    spark.sql("use zhnwang_tpcds_meta")
    runStats(spark, day);
  }

}
