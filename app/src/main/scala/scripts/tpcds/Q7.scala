package scripts.tpcds

import org.apache.spark.Dependency
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDD
import org.apache.spark.sql.execution.datasources.{FilePartition, FileScanRDD}
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}
import org.apache.spark.sql.{DataFrame, SparkSession}
import tpcds.action.TPCDSQueryRunAction
import tpcds.table.TPCDS14

import scala.reflect.runtime.universe.{TermName, typeOf}
import scala.reflect.runtime.{universe => ru}

class Q7 (spark: SparkSession) {
  val qa = new TPCDSQueryRunAction(spark, "zhnwang_tpcds_meta")

  lazy val queryString: String = {
    var result = ""
    new TPCDS14().queriesWithId.filter(_._2.equals("q7")).foreach { case (query, qid) =>
      result = query
    }
    result
  }

  def runBasics(): Unit = {
    val q1 = "select count(*) from tpcds_sf1000_iceberg.store_sales where ss_sold_date_sk is null"

    runtimes(q1, getDataSourceRddTask)
    val q2 = "select count(*) from tpcds_sf1000_hive_parquet.store_sales where ss_sold_date_sk is null;"
    runtimes(q2, getDataSourceRddTask)
  }

  def runIceQ7(times: Int = 1): Unit = {
    spark.sql("use tpcds_sf1000_iceberg")
    runtimes(queryString, getDataSourceRddTask, times)
  }
  def runHiveQ7(times: Int = 1): Unit = {
    spark.sql("use tpcds_sf1000_hive_parquet")
    runtimes(queryString, getFileScanRddTask, times)
  }

  def runtimes(q: String,  exec:(DataFrame) => Unit, n : Int = 10): Unit = {
    // println(q)
    Range.apply(0, n).foreach({ i =>
      val df = spark.sql(q)
      measureScans(df, exec)
    })
  }

  def measureScans(df: DataFrame, exec:(DataFrame) => Unit): Unit = {
    val ts = System.currentTimeMillis()
    // getDataSourceRddTask(df)
    exec(df)
    val te = System.currentTimeMillis()
    val cost = te - ts
    println(s"cost: $cost ms ${cost / 1000.0d}s")
  }

  def getDataSourceRddTask(df: DataFrame):Unit = {
    val plan: SparkPlan = df.queryExecution.sparkPlan
    val lplan = plan.collectLeaves().filter { p =>
      p match {
        case l : LeafExecNode => l.nodeName.contains("store_sales")
      }
    }.head

    val rdd: RDD[InternalRow] = lplan.execute()
    val m = ru.runtimeMirror(getClass.getClassLoader)
    val im = m.reflect(rdd)
    val methodParent = typeOf[RDD[InternalRow]].decl(TermName("dependencies")).asMethod
    val mm = im.reflectMethod(methodParent)
    val prdd = mm().asInstanceOf[Seq[Dependency[_]]].head.rdd.asInstanceOf[RDD[InternalRow]]

    val dsrdd = prdd.asInstanceOf[DataSourceRDD]
    val drim = m.reflect(dsrdd)
    val fdInput = typeOf[DataSourceRDD].decl(TermName("inputPartitions")).asTerm.accessed.asTerm
    val fmInput = drim.reflectField(fdInput)
    val inputPar = fmInput.get.asInstanceOf[Seq[InputPartition]]
    println(dsrdd.getClass)
    println(rdd.getClass)
    println(inputPar.length)
    fmInput.set(inputPar.take(1))
    dsrdd.collect()
  }

  def getFileScanRddTask(df: DataFrame):Unit = {
    val plan: SparkPlan = df.queryExecution.sparkPlan
    val lplan = plan.collectLeaves().filter { p =>
      p match {
        case l : LeafExecNode => l.nodeName.contains("store_sales")
      }
    }.head

    val rdd: RDD[InternalRow] = lplan.execute()
    val m = ru.runtimeMirror(getClass.getClassLoader)
    val im = m.reflect(rdd)
    val methodParent = typeOf[RDD[InternalRow]].decl(TermName("dependencies")).asMethod
    val mm = im.reflectMethod(methodParent)
    val prdd = mm().asInstanceOf[Seq[Dependency[_]]].head.rdd.asInstanceOf[RDD[InternalRow]]

    val dsrdd = prdd.asInstanceOf[FileScanRDD]
    val drim = m.reflect(dsrdd)
    val fdInput = typeOf[FileScanRDD].decl(TermName("filePartitions")).asTerm.accessed.asTerm
    val fmInput = drim.reflectField(fdInput)
    val inputPar = fmInput.get.asInstanceOf[Seq[FilePartition]]
    println(rdd.getClass)
    println(dsrdd.getClass)
    println(inputPar.length)
    fmInput.set(inputPar.take(1))

    dsrdd.collect()
  }

  def runCmp() = {


    spark.sparkContext.parallelize(Seq("")).foreachPartition(x => {
      import org.apache.commons.logging.LogFactory
      import org.apache.log4j.{Level, LogManager}

      LogManager.getRootLogger().setLevel(Level.DEBUG)
      val log = LogFactory.getLog("EXECUTOR-LOG:")
      log.debug("START EXECUTOR DEBUG LOG LEVEL")
    })

    val action = new TPCDSQueryRunAction(spark, "zhnwang_tpcds_meta")
    val hpf = "sf1000-hive-parquet-n0"
    val ipf = "sf1000-iceberg-n0"


    action.runProfileQid(hpf, "q7")
    action.runProfileQid(ipf, "q7")

    spark.sparkContext.parallelize(Seq("")).foreachPartition(x => {
      import org.apache.commons.logging.LogFactory
      import org.apache.log4j.{Level, LogManager}

      LogManager.getRootLogger().setLevel(Level.INFO)
      val log = LogFactory.getLog("EXECUTOR-LOG:")
      log.info("START EXECUTOR INFO LOG LEVEL")
    })


  }

}
