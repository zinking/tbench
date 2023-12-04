package tpcds.action

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.Serialization.write

import scala.reflect.runtime.universe._

class TPCDSAction(spark: SparkSession, meta: String) {
  private val LOG = LogFactory.getLog(classOf[TPCDSAction])
  protected def camelToUnderscores(name: String, i: Int = 0): String = {
    val p = "[A-Z\\d]".r.replaceAllIn(name, {m =>
      "_" + m.group(0).toLowerCase()
    })

    if (i > 0) {
      p.drop(i)
    } else {
      p
    }
  }

  protected def tableName[T](implicit tag: TypeTag[T]): String = {
    val klass = typeOf[T].toString
    val kklass = klass.split("\\.").last
    camelToUnderscores(kklass, 1)
  }

  def useDatabase(db: String): Unit = {
    val uql = s"use $db"
    runDDL(uql)
  }

  def runDDL(ql: String): Unit = {
    println(ql)
    val df = spark.sql(ql)
    df.show()
  }

  def runSQL(ql: String): DataFrame = {
    println(ql)
    spark.sql(ql)
  }

  def runOnlySQL(ql: String): DataFrame = {
    spark.sql(ql)
  }

  def dfCaseToCamel[T](df: DataFrame)(implicit tag: TypeTag[T]): DataFrame = {
    val schema = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]
    var ddf: DataFrame = df
    schema.fields.foreach { fd =>
      val curName = fd.name
      val newName = camelToUnderscores(fd.name)
      if (!curName.equals(newName)) {
        ddf = ddf.withColumnRenamed(curName, newName)
      }
    }
    ddf
  }

  def dfCamelToCase[T](df: DataFrame)(implicit tag: TypeTag[T]): DataFrame = {
    val schema = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]
    var ddf: DataFrame = df
    schema.fields.foreach { fd =>
      val curName = fd.name
      val newName = camelToUnderscores(fd.name)
      if (!curName.equals(newName)) {
        ddf = ddf.withColumnRenamed(newName, curName)
      }
    }
    ddf
  }

  def appendEntry[T](df: DataFrame)(implicit tag: TypeTag[T]): Unit = {
    val tn = tableName[T]
    val ndf = dfCaseToCamel[T](df)
    ndf.writeTo(s"$meta.$tn").append()
  }

  def appendTableEntry[T](tn: String, df: DataFrame)(implicit tag: TypeTag[T]): Unit = {
    val ndf = dfCaseToCamel[T](df)
    ndf.writeTo(s"$meta.$tn").append()
  }


  def getUserName: String = {
    UserGroupInformation.getCurrentUser.getUserName
  }

  def setSparkConf(k: String, v: String): Unit = {
    SQLConf.get.setConfString(k, v)
    LOG.warn(s"set ${k}=${v};")
  }

  protected def getMedian[T: Ordering, F] (seq: Seq[T]) (implicit conv: T => F, f: Fractional[F]): F = {
    val sortedSeq = seq.sorted
    if (seq.isEmpty) return 0d.asInstanceOf[F]
    if (seq.size % 2 == 1) sortedSeq(sortedSeq.size / 2)  else {
      val (up, down) = sortedSeq.splitAt(seq.size / 2)
      import f._
      (conv(up.last) + conv(down.head)) / fromInt(2)
    }
  }


  @annotation.tailrec
  final def retry[T](n: Int)(fn: => T): T = {
    util.Try { fn } match {
      case util.Success(x) => x
      case _ if n > 1 => retry(n - 1)(fn)
      case util.Failure(e) => throw e
    }
  }

  def applySparkConf(conf: String): Unit = {
    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
    try {
      val entries = JsonMethods.parse(conf).extract[Map[String, String]]
      entries.foreach { case (k, v) =>
        setSparkConf(k, v)
      }
    } catch {
      case e: Exception =>
        println(e.getMessage)
        e.printStackTrace()
    }
  }

  def convert2Json(ms: List[Long]): String = {
    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
    write(ms)
  }

}

object TPCDSAction {
  def getSparkConf = {
    new SparkConf()
      .setAppName("TPCDSAppAction")
      .set("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
      .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .set("spark.sql.catalog.iceberg_catalog.type", "hive")
      .set("spark.sql.catalog.iceberg_catalog.uri", "thrift://ss-qe-oms.tencent-distribute.com:8096")
      .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .set("spark.hadoop.hive.metastore.uris", "thrift://ss-qe-oms.tencent-distribute.com:8100")
      .set("spark.sql.catalogImplementation", "hive")
      .set("spark.sql.database.default", "oms")
      .set("spark.hive.client.loader.isolated", "false")
      .set("spark.hadoop.hive.exec.dynamic.partition", "true")
      .set("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
  }

  def getSparkConf2 = {
    new SparkConf()
      .setAppName("TPCDSAppAction")
      .set("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
      .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .set("spark.sql.catalog.iceberg_catalog.type", "hive")
      .set("spark.sql.catalog.iceberg_catalog.uri", "thrift://9-29-173-60:9083")
      .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .set("spark.hadoop.hive.metastore.uris", "thrift://9-29-173-60:9083")
      .set("spark.sql.catalogImplementation", "hive")
      .set("spark.sql.database.default", "oms")
      .set("spark.hive.client.loader.isolated", "false")
      .set("spark.hadoop.hive.exec.dynamic.partition", "true")
      .set("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
  }


}

case class TableCreation(db: String, scale: Int, tb: String, format: String, hive: String, cmd: String, gmt_create: Long, gmt_end: Long, creator: String)

case class QueryRunProfile(name: String, database: String, qset: String, heatn: Int, runn: Int, sparkConf: String, compare: String)

case class QueryRun(profileName: String, qset: String, qid: String, heat: Long, heatd: String, run: Long, rund: String, compare: String, gmt_create: Long, gmt_end: Long, creator: String)
