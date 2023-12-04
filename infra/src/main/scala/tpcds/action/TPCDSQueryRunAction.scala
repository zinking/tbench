package tpcds.action

import org.apache.commons.logging.LogFactory
import org.apache.spark.sql.{DataFrame, SparkSession}

class TPCDSQueryRunAction(spark: SparkSession, meta: String) extends TPCDSQueryProfileAction(spark, meta) {
  private val LOG = LogFactory.getLog(classOf[TPCDSQueryRunAction])

  case class CostItem(st: Long, ed: Long) {
    def cost: Long = ed - st
  }
  case class Run(config: QueryRunProfile, qid: String, query: String, df: Option[DataFrame] = None,
                 heats: Seq[CostItem] = Seq.empty, runs: Seq[CostItem] = Seq.empty) {
    def queryRun: QueryRun = {
      val hts = heats.map(_.cost).toList
      val htss = convert2Json(hts)
      val htm = getMedian[Long, Double](hts).toLong

      val rts = runs.map(_.cost).toList
      val rtss = convert2Json(rts)
      val rtm = getMedian[Long, Double](rts).toLong
      val st: Long = heats.headOption.map(_.st).getOrElse(
        runs.headOption.map(_.st).getOrElse(0)
      )
      val ed: Long = runs.lastOption.map(_.ed).getOrElse(
        heats.lastOption.map(_.ed).getOrElse(0)
      )
      QueryRun(config.name, config.qset, qid, htm, htss, rtm, rtss, "", st, ed, getUserName)
    }
  }

  case class ProfileRun(config: QueryRunProfile, explain: Boolean = false) {
    def run(qid: String, query: String): Unit = {
      try {
        if (explain) {
          val entry = collectExplain(config, qid, query)
          LOG.warn(s"query explain: $entry")
          createQueryRun(entry.queryRun)
        } else {
          val entry = collectMetrics(config, qid, query)
          LOG.warn(s"query run: $entry")
          createQueryRun(entry.queryRun)
        }
      } catch {
        case e: Exception =>
          LOG.error(s"ran error $qid ${e.getMessage}", e)
      }
    }
    def run(): Unit = {
      LOG.warn(s"running profile $config")
      applySparkConf(config.sparkConf)
      useDatabase(config.database)

      val queryRuns = getQueryRun(config.name)
      getTPCDSQueries(config).foreach { case (query, qid) =>
        queryRuns.get(qid) match {
          case Some(qrun) =>
            LOG.warn(s"skip already ran $qrun")
          case None =>
            run(qid, query)
        }
      }
    }
  }

  private def retryOnError(run: Run): Run = {
    retry(3) {
      val dataFrame = runOnlySQL(run.query)
      dataFrame.collect()
      run.copy(df = Some(dataFrame))
    }
  }

  def runTPCDSQuery(query: String): Unit = {
    runOnlySQL(query).collect()
  }

  def runProfileQuery(profile: String, query: String, explain: Boolean = false): Unit = {
    getProfile(profile) match {
      case Some(qpf) =>
        LOG.warn(s"running profile $qpf")
        val qid = s"Q${System.currentTimeMillis()}"
        val profileRun = ProfileRun(qpf, explain)
        profileRun.run(qid, query)

      case None =>
        LOG.warn(s"no profile $profile found")
    }
  }

  def runProfileQid(profile: String, qid: String, explain: Boolean = false): Unit = {
    getProfile(profile) match {
      case Some(qpf) =>
        LOG.warn(s"running profile $qpf")
        applySparkConf(qpf.sparkConf)
        useDatabase(qpf.database)
        val profileRun = ProfileRun(qpf, explain)
        getTPCDSQueries(qpf).filter(_._2.equals(qid)).foreach { case (query, qid) =>
          profileRun.run(qid, query)
        }
      case None =>
        LOG.warn(s"no profile $profile found")
    }
  }

  def runProfileQueries(profile: String, explain: Boolean = false): Unit = {
    getProfile(profile) match {
      case Some(qpf) =>
        LOG.warn(s"running profile $qpf $explain")
        val profileRun = ProfileRun(qpf, explain)
        profileRun.run()

      case None =>
        LOG.warn(s"no profile $profile found")
    }
  }

  case class Regression(src: QueryRunProfile, tgt: QueryRunProfile) {
    def name: String = {
      s"cmp_${src.name}_${tgt.name}"
    }

    def compare(): Unit = {
      // assumptions are that these profiles are comparable
      LOG.warn(s"running regression using $src as source and $tgt as target")
      val queryRuns = getQueryRun(name)
      getTPCDSQueries(src).foreach { case (query, qid) =>
        queryRuns.get(qid) match {
          case Some(qrun) =>
            LOG.warn(s"skip already ran $qrun")
          case None =>
            try {
              compare(qid, query)
            } catch {
              case e: Exception =>
                LOG.error(s"ran error $qid ${e.getMessage}", e)
            }
        }
      }
    }

    def compare(qid: String, query: String): Unit = {
      val st = System.currentTimeMillis()
      val srcRun = collectMetrics(src, qid, query)
      val tgtRun = collectMetrics(tgt, qid, query)
      val same = DataFrameDiff.compare(qid, srcRun.df.get, tgtRun.df.get)
      val ed = System.currentTimeMillis()
      val cmpResult = if (same) {
        "Y"
      } else {
        "N"
      }
      val diffrun = QueryRun(name, src.qset, qid, 0, "", ed - st, "", cmpResult, st, ed, getUserName)
      createQueryRun(diffrun)
      createQueryRun(srcRun.queryRun)
      createQueryRun(tgtRun.queryRun)
    }
  }

  def regression(srcPrf: String, tgtPrf: String): Unit = {
    val srcPrfOption = getProfile(srcPrf)
    val tgtPrfOption = getProfile(tgtPrf)
    (srcPrfOption, tgtPrfOption) match {
      case (Some(src), Some(tgt)) =>
        val regression = Regression(src, tgt)
        regression.compare()
      case _ =>
        LOG.warn(s"skipping regression as $srcPrf or $tgtPrf profile does not exist")
    }
  }

  def regression(srcPrf: String, tgtPrf: String, qid: String): Unit = {
    val srcPrfOption = getProfile(srcPrf)
    val tgtPrfOption = getProfile(tgtPrf)
    (srcPrfOption, tgtPrfOption) match {
      case (Some(src), Some(tgt)) =>
        val regression = Regression(src, tgt)
        LOG.warn(s"running regression using $srcPrf as source and $tgtPrf as target for qid $qid")
        getTPCDSQueries(src).filter(_._2.equals(qid)).foreach { case (query, qid) =>
          regression.compare(qid, query)
        }

      case _ =>
        LOG.warn(s"skipping regression as $srcPrf or $tgtPrf profile does not exist")
    }
  }

  def collectExplain(qpf: QueryRunProfile, qid: String, query: String): Run = {
    applySparkConf(qpf.sparkConf)
    useDatabase(qpf.database)
    val df = runOnlySQL(query)
    df.explain(true)
//    LOG.warn(df.queryExecution.analyzed.toJSON)
//    LOG.warn(df.queryExecution.optimizedPlan.toJSON)
//    LOG.warn(df.queryExecution.executedPlan.toJSON)
    Run(qpf, qid, query)
  }
  def collectMetrics(qpf: QueryRunProfile, qid: String, query: String): Run = {
    applySparkConf(qpf.sparkConf)
    useDatabase(qpf.database)

    val qrun = Run(qpf, qid, query)
    val htRun = Range.apply(0, qpf.heatn).foldLeft(qrun){ case (lastRun, _) =>
      // run the ith heating
      val st = System.currentTimeMillis()
      val curRun = retryOnError(lastRun)
      val ed = System.currentTimeMillis()
      curRun.copy(heats = curRun.heats :+ CostItem(st, ed))
    }

    Range.apply(0, qpf.runn).foldLeft(htRun) { case (lastRun, _) =>
      // run the ith heating
      val st = System.currentTimeMillis()
      val curRun = retryOnError(lastRun)
      val ed = System.currentTimeMillis()
      curRun.copy(runs = curRun.runs :+ CostItem(st, ed))
    }
  }

  def compareProfileRuns(p1: String, p2: String): DataFrame = {
    val query =
      s"""
        |with p1 as (
        |    select
        |        qset, qid, run
        |    from
        |        ${meta}.query_run
        |    where
        |        profile_name = '$p1'
        |),
        |p2 as (
        |    select
        |        qset, qid, run
        |    from
        |        ${meta}.query_run
        |    where
        |        profile_name = '$p2'
        |)
        |select
        |    '${p1}_vs_${p2}' as name,
        |    p1.qid,
        |    p1.run as lrun,
        |    p2.run as rrun,
        |    p1.run - p2.run as delta
        |from
        |    p1
        |    join
        |    p2
        |        on p1.qid = p2.qid and p1.qset = p2.qset
        |""".stripMargin
    runSQL(query)
  }

  def exportComparison(p1: String, p2: String): Unit = {
    val df = compareProfileRuns(p1, p2)
    val file = s"/tmp/cmp_${p1}_${p2}"
    df.repartition(1).write.format("csv").save(file)
    LOG.warn(s"file written to ${file}")
  }

}
