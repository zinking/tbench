package tpcds.action

import com.google.common.annotations.VisibleForTesting
import org.apache.commons.logging.LogFactory
import org.apache.spark.sql.SparkSession
import tpcds.table.{TPCDS14, TPCDS27, TPCDSMod}

class TPCDSQueryProfileAction(spark: SparkSession, meta: String) extends TPCDSAction(spark, meta) {
  private val LOG = LogFactory.getLog(classOf[TPCDSQueryProfileAction])

  def createQueryRunProfileIfNotExist(entry: QueryRunProfile): Boolean = {
    getProfile(entry.name) match {
      case Some(pf) =>
        LOG.warn(s"${pf.name} profile already exists")
        true
      case None =>
        createQueryRunProfile(entry)
    }
  }

  def createQueryRunProfile(entry: QueryRunProfile): Boolean = {
    import spark.implicits._
    val df = Seq(entry).toDF
    try {
      appendEntry[QueryRunProfile](df)
      true
    } catch {
      case e: Exception =>
        LOG.error(s"create query profile failed: ${e.getMessage}")
        false
    }
  }

  @VisibleForTesting
  def getProfile(qn: String): Option[QueryRunProfile] = {
    import spark.implicits._
    val tn = tableName[QueryRunProfile]
    val exql = s"select * from $meta.$tn where name = '$qn' limit 1"
    val df = runSQL(exql)
    val ddf = dfCamelToCase[QueryRunProfile](df)
    val ds = ddf.as[QueryRunProfile].collect()
    ds.headOption
  }

  @VisibleForTesting
  def createQueryRun(entry: QueryRun): Unit = {
    import spark.implicits._
    println(s"create query run $entry")
    appendEntry[QueryRun](Seq(entry).toDF)
  }

  /**
   * record single query run in separate table
   *
   * @param entry query run entry
   */
  def createSingleQueryRun(entry: QueryRun): Unit = {
    import spark.implicits._
    val tn = tableName[QueryRun] + "_single"
    appendTableEntry[QueryRun](tn, Seq(entry).toDF)
  }

  @VisibleForTesting
  def getQueryRun(profile: String): Map[String, QueryRun] = {
    import spark.implicits._
    val tn = tableName[QueryRun]
    val exql = s"select * from $meta.$tn where profile_name = '$profile'"
    val df = runSQL(exql)
    val ddf = dfCamelToCase[QueryRun](df)
    ddf.as[QueryRun].collect().map { qr =>
      (qr.qid, qr)
    }.toMap
  }

  def getTPCDSQueries(qpf: QueryRunProfile): Seq[(String, String)] = {
    qpf.qset match {
      case "tpcds" => new TPCDS14().queriesWithId
      case "tpcdsx72" => new TPCDS14().queriesWithId.filterNot(_._2 == "q72")
      case "tpcds-v2.7.0" => new TPCDS27().queriesWithId
      case "tpcds-modifiedQueries" => new TPCDSMod().queriesWithId
      case _ =>
        LOG.warn(s"empty query list returned for ${qpf.qset}")
        List.empty
    }
  }

}
