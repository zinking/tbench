package scripts.tpcds

import org.apache.commons.logging.LogFactory
import org.apache.iceberg.StatsReader
import org.apache.iceberg.stats.{StatSerializer, Statistics}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SimpleMode
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write
import tools.{CboStatsTools, Metastore}
import tpcds.action.{QueryRun, QueryRunProfile, TPCDSQueryRunAction, TPCDSTableAction}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.collection.JavaConverters.{asScalaBufferConverter, asScalaSetConverter}

class TPCDSCboTest(spark: SparkSession) {
  private val LOG = LogFactory.getLog(classOf[TPCDSCboTest])

  val dwLoc = "hdfs://9.135.73.45:9000/user/hive/warehouse/"
  val metaDb = "zhnwang_tpcds_meta"
  val db = "tpcds_iceberg_500"

  val collectionProfileName = "cboCreationT20231204"
  val collectionPartitionProfileName = "cboParCreation20231204"
  val perfProfileName = "cboPerfT7"
  val backupFolder = s"/var/tmp/tpcds/cbostats/table/$db/"

  init()

  val qset = "cbo"
  val runset = "tpcdsx72"

  val tableAction = new TPCDSTableAction(spark, metaDb, true, dwLoc)
  val queryAction = new TPCDSQueryRunAction(spark, metaDb)
  val metaTool = new Metastore(spark, "iceberg")

  def init(): Unit = {
    LOG.warn("INIT SPARK CONFs")
    setConf
  }

  def setupMeta(): Unit = {
    tableAction.createTPCDSMeta()
  }

  def setConf(): Unit = {
    spark.sql("set spark.sql.cbo.enabled=true")
    spark.sql("set spark.sql.statistics.histogram.enabled=true")
  }

  def getFullProfileName(pfName: String, dppFlag: Boolean, cboFlag: Boolean): String = {
    s"${pfName}_dff${dppFlag}_cbo${cboFlag}"
  }

  def setupCollectionProfile(): Unit = {
    val profile = QueryRunProfile(collectionProfileName, db, qset, 0, 1, "{}", "")
    queryAction.createQueryRunProfileIfNotExist(profile)
  }

  def setupPartitionCollectionProfile(): Unit = {
    val profile = QueryRunProfile(collectionPartitionProfileName, db, qset, 0, 1, "{}", "")
    queryAction.createQueryRunProfileIfNotExist(profile)
  }

  def runQueryCompare(profile: String, qid: String, explain: Boolean = false): Unit = {
    println("running cbo on")
    queryAction.runProfileQid(getFullProfileName(profile, dppFlag = true, cboFlag = true), qid, explain)
    println("running cbo off")
    queryAction.runProfileQid(getFullProfileName(profile, dppFlag = true, cboFlag = false), qid, explain)
  }

  def setupPerfProfiles(profile: String): Unit = {
    for {
      dppFlag <- Seq(true, false)
      cboFlag <- Seq(true, false)
    } yield {
      val name = getFullProfileName(profile, dppFlag, cboFlag)
      val sparkConf = getCboConfig(dppFlag.toString, cboFlag.toString)
      val pf = QueryRunProfile(name, db, runset, 1, 3, sparkConf, "")
      println(s"$pf created")
      queryAction.createQueryRunProfileIfNotExist(pf)
      pf
    }
  }

  def runQueriesPerf(profile: String): Unit = {
    for {
      dppFlag <- Seq(true, false)
      cboFlag <- Seq(true, false)
    } yield {
      println(s"running $dppFlag $cboFlag")
      queryAction.runProfileQueries(getFullProfileName(profile, dppFlag, cboFlag))
    }
  }

  def runQueriesPerfWithCBO(profile: String): Unit = {
    for {
      dppFlag <- Seq(true, false)
      cboFlag <- Seq(true)
    } yield {
      println(s"running $dppFlag $cboFlag")
      queryAction.runProfileQueries(getFullProfileName(profile, dppFlag, cboFlag))
    }
  }

  def getCboConfig(dppFlag: String, cboFlag: String): String = {
    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
    val flags = Map(
      "spark.sql.cbo.enabled" -> cboFlag,
      "spark.sql.cbo.joinReorder.enabled" -> cboFlag,
      "spark.sql.cbo.joinReorder.dp.star.filter" -> cboFlag,
      "spark.sql.cbo.starSchemaDetection" -> cboFlag,
      "spark.sql.statistics.histogram.enabled" -> cboFlag,
      "spark.sql.optimizer.dynamicPartitionPruning.enabled" -> dppFlag
    )
    write(flags)
  }

  def recordCollectCost(table: String, st: Long, ed: Long, profile: String): Unit = {
    val qid = s"${table}.collect_stats"
    val cost = ed - st
    val collectRun = QueryRun(profile, qset, qid, 0, "", cost, "", "", st, ed, "")
    queryAction.createQueryRun(collectRun)
    println(s"Created query run $collectRun")
  }

  def runCollectStats(): Unit = {
    val queryRuns = queryAction.getQueryRun(collectionProfileName)

    def hasCollectRan(table: String): Boolean = {
      val qid = s"${table}.collect_stats"
      queryRuns.contains(qid)
    }

    val dml = "CALL iceberg.system.collect_stats(table => 'tpcds_iceberg_500.VAR_TABLE', allColumns => true)"
    tableAction.runTpcdsTableDMLs(
      db, dml,
      (table) => hasCollectRan(table),
      (table, st, ed) => recordCollectCost(table, st, ed, collectionProfileName))
  }

  def collectPartitionStats(table: String): Unit = {
    val dml = s"CALL iceberg.system.collect_stats(" +
      s"table => 'tpcds_iceberg_500.$table', allColumns => true, collectMode => 'partition', overwrite => true)"
    LOG.warn("new version...")
    try {
      val st = System.currentTimeMillis()
      tableAction.runDDL(dml)
      val ed = System.currentTimeMillis()
      recordCollectCost(table, st, ed, collectionPartitionProfileName)
    } catch {
      case e: Throwable =>
        println(e.getMessage)
        e.printStackTrace()
    }
  }

  def runCollectPartitionStats(): Unit = {
    val queryRuns = queryAction.getQueryRun(collectionPartitionProfileName)
    def collectPartitionTableStats(table: String): Unit = {
      val tb = metaTool.loadTableWithPlan(db, table)
      if (!tb.spec().isUnpartitioned) {
        LOG.warn(s"collecting partition level stats for $table")
        collectPartitionStats(table)
      } else {
        LOG.warn(s"$table is not partitioned, skipping")
      }
    }
    tableAction.runTpcdsTableActions(db, (table) => collectPartitionTableStats(table))
  }

  def backupStats(): Unit = {
    def backupTableStat(table: String): Unit = {
      val tb = metaTool.loadTableWithPlan(db, table)
      val reader = new StatsReader(tb)
      val stats = reader.tableStats()
      val ser = StatSerializer.toJson(stats)
      val serbytes = ser.getBytes(StandardCharsets.UTF_8)
      val fileName = tb.name()
      val path = s"$backupFolder/$fileName"
      Files.write(Paths.get(path), serbytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
    }
    tableAction.runTpcdsTableActions(db, (table) => backupTableStat(table))
  }

  def compareStats(): Unit = {
    def compareTableStat(table: String): Unit = {
      val tb = metaTool.loadTableWithPlan(db, table)
      LOG.warn("table schema:")
      LOG.warn(tb.schema().toString)
      val reader = new StatsReader(tb)
      val combinedStats: Statistics = reader.tableStats()
      val fileName = tb.name()
      val path = s"$backupFolder/$fileName"
      val targetStr = Files.readAllLines(Paths.get(path), StandardCharsets.UTF_8).asScala.mkString("\n")
      val target = StatSerializer.fromJson(targetStr)
      val diff = CboStatsTools.cmpStats(combinedStats, target)
      LOG.warn("stats diff")
      LOG.warn(diff)
    }

    compareTableStat("store_sales")
    //    tableAction.runTpcdsTableActions(db, (table) => {
    //      try {
    //        compareTableStat(table)
    //      } catch {
    //        case e: Exception =>
    //          LOG.error("error compare stats", e)
    //      }
    //    })
  }

  def showPartitionStats(table: String): Unit = {
    val tb = metaTool.loadTableWithPlan(db, table)
    val reader = new StatsReader(tb)
    reader.allPartitionStats().entrySet().asScala.take(3).foreach { entry =>
      val par = entry.getKey
      val stats = entry.getValue
      val parStatsDisplay = CboStatsTools.showStats(stats)
      LOG.warn("partition: " + par)
      LOG.warn(parStatsDisplay)
    }
  }

  def showTableStats(db: String, table: String): Unit = {
    val tb = metaTool.loadTableWithPlan(db, table)
    val reader = new StatsReader(tb)
    val statsDisplay = CboStatsTools.showStats(reader.tableStats())
    LOG.warn("table stats " + table)
    LOG.warn(statsDisplay)
  }

  def showPartitionNullCount(table: String): Unit = {
    val tb = metaTool.loadTableWithPlan(db, table)
    val reader = new StatsReader(tb)
    reader.allPartitionStats().entrySet().asScala.take(3).foreach { entry =>
      val par = entry.getKey
      val stats = entry.getValue
      val parStatsDisplay = CboStatsTools.showNullCount(stats, 1L)
      LOG.warn(s"partition: ${par} ${parStatsDisplay}")
    }
  }

  def main0(args: Array[String]): Unit = {
    val dm = new scripts.tpcds.TPCDSCboTest(spark)
    dm.setupCollectionProfile()
    dm.runCollectStats()
  }

  def main1(args: Array[String]): Unit = {
    val dm = new scripts.tpcds.TPCDSCboTest(spark)
//    dm.setupPartitionCollectionProfile()
//    dm.backupStats()
    dm.collectPartitionStats("store_sales")
    dm.runCollectPartitionStats()
  }

  def main11(args: Array[String]): Unit = {
    val dm = new scripts.tpcds.TPCDSCboTest(spark)
    dm.collectPartitionStats("store_sales")
    //select sum(case when ss_sold_time_sk is null then 1 else 0 end) as ncnt from tpcds_iceberg_500.store_sales
    //select sum(case when ss_sold_time_sk is null then 1 else 0 end) as ncnt from tpcds_iceberg_500.store_sales where ss_sold_date_sk=2451664
    //select count(1) from tpcds_iceberg_500.store_sales where ss_sold_date_sk is null
    //64795511 true value
    //diff shows nullCount: 32402679| 64795511
    //PartitionData{ss_sold_date_sk=2451664} null count: 10288
    //10288
//    spark - sql > select count (distinct ss_sold_date_sk) from tpcds_iceberg_500.store_sales;
//    count(DISTINCT ss_sold_date_sk)
//    1823
    dm.showPartitionNullCount("store_sales")
  }

  def main2(args: Array[String]): Unit = {
    val dm = new scripts.tpcds.TPCDSCboTest(spark)
    dm.compareStats()
    dm.showPartitionStats("store_sales")
  }

  def main3(args: Array[String]): Unit = {
    val dm = new scripts.tpcds.TPCDSCboTest(spark)
//    dm.setupPerfProfiles()
    dm.runQueryCompare("cboPerfT11", "q11", true)
    dm.runQueryCompare("cboPerfT11", "q64")
    dm.runQueryCompare("cboPerfT11", "q11")
    val queryAction = new TPCDSQueryRunAction(spark, "zhnwang_tpcds_meta")
    queryAction.exportComparison("cboPerfT15_dfftrue_cbotrue", "cboPerfT13_dfftrue_cbofalse")
    queryAction.exportComparison("cboPerfT15_dfffalse_cbotrue", "cboPerfT13_dfffalse_cbofalse")
  }

  def main4(): Unit = {
    val dm = new scripts.tpcds.TPCDSCboTest(spark)
    dm.setupPerfProfiles("cboPerfT13")
//    dm.runQueriesPerf("cboPerfT11")
    val qr = new TPCDSQueryRunAction(spark, "zhnwang_tpcds_meta")
    qr.runProfileQid("cboPerfT3_cbo_off", "q3")
    import org.apache.log4j.{Level, LogManager}
    LogManager.getRootLogger().setLevel(Level.INFO)
  }

  def main5(): Unit = {
    val queryAction = new TPCDSQueryRunAction(spark, "zhnwang_tpcds_meta")
    queryAction.runProfileQid("cboPerfT13_dfftrue_cbotrue", "q19", false)
    queryAction.runProfileQid("cboPerfT13_dfftrue_cbofalse", "q19", false)
    queryAction.runProfileQid("cboPerfT13_dfftrue_cbotrue",  "q85", true)
    queryAction.runProfileQid("cboPerfT13_dfftrue_cbofalse",  "q85", true)
    queryAction.regression("cboPerfT13_dfftrue_cbotrue", "cboPerfT13_dfftrue_cbofalse", "q85")
    val df2 = spark.sql("")
    df2.queryExecution.explainString(SimpleMode)
  }

  def main6(): Unit = {
    val stub = new arthas.ArthasStub();
    var session = arthas.ArthasSession.getSession
    arthas.ArthasCapture.capturedObjs.clear
//    var target = "org.apache.iceberg.TableMetadata$Builder build"
//    var watchParams = "'{@arthas.ArthasCapture@capture(target)}'"
//    var watchCmd = s"watch $target $watchParams -x 2"
    //var scCmd = s"sc org.apache.iceberg.TableMetadata"
    //scCmd = "sc -d demo.MathGame"
    var cmd = "jad org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation"
    session.makeAsyncCommand(cmd)
    session.pullResults
    //spark.sql("");
    val queryAction = new TPCDSQueryRunAction(spark, "zhnwang_tpcds_meta")
    queryAction.runProfileQid("cboPerfT13_dfftrue_cbotrue", "q19", false)
    session.pullResults
    session.interrupt
  }

  def main7(): Unit = {

    var session = arthas.ArthasSession.getSession
    var cmd = "jad org.apache.spark.sql.util.ArrowUtils$";
    session.makeAsyncCommand(cmd)
    session.pullResults
    session.pullResults
    session.interrupt

  }

}
