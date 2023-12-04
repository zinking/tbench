package tpcds.action

import io.trino.tpcds.Table
import org.apache.commons.lang3.StringUtils
import org.apache.commons.logging.LogFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import tpcds.table.{TPCDSCatalog, TPCDSTable}

import scala.reflect.runtime.universe._

class TPCDSTableAction(
  spark: SparkSession,
  meta: String,
  createDb: Boolean = false,
  dwLoc: String = "" ) extends TPCDSAction(spark, meta) {
  private val LOG = LogFactory.getLog(classOf[TPCDSTableAction])


  def tableCreated(db: String, tb: String): Boolean = {
    // need to check the log table to verify table is created intact
    val tn = tableName[TableCreation]
    val exql = s"select * from $meta.$tn where db = '$db' and tb = '$tb'"
    val rdf = runSQL(exql)
    rdf.collect().length > 0
  }

  private def getTPCDSTables: List[Table] = {
    val allTables: List[Table] = Table.values.toList
    allTables.filter((table: Table) => !table.getName.startsWith("s_") && !table.getName.equals("dbgen_version"))
  }

  def createMetaTable[T]()(implicit tag: TypeTag[T]): Unit = {
    val schema = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]
    val sschema = StructType(
      schema.fields.map { fd =>
        fd.copy(name = camelToUnderscores(fd.name))
      }
    )
    val ddl = sschema.toDDL
    val table = tableName[T]
    val tableSql = s"""create table if not exists $meta.$table ( $ddl ) using iceberg"""
    runDDL(tableSql)
  }

  def createTPCDSMeta(): String = {
    if (createDb) {
      val sqlMetaDb = s"create database if not exists $meta LOCATION '$dwLoc'"
      runDDL(sqlMetaDb)
    }
    createMetaTable[TableCreation]()
    createMetaTable[QueryRunProfile]()
    createMetaTable[QueryRun]()

    spark.sql("create table if not exists zhnwang_tpcds_meta.query_run_single " +
      "( `profile_name` STRING,`qset` STRING,`qid` STRING,`heat` BIGINT,`heatd` STRING," +
      "`run` BIGINT,`rund` STRING,`compare` STRING," +
      "`gmt_create` BIGINT,`gmt_end` BIGINT,`creator` STRING ) using iceberg")
    "{}"
  }

  def exportMetaDb(backBase: String): Unit = {
    val allTables = Seq(
      "table_creation",
      "query_run_profile",
      "query_run",
      "query_run_single"
    )

    allTables.foreach { tb =>
      exportTableData(tb, backBase)
    }
  }

  def exportTableData(tb: String, backBase: String): Unit = {
    val df = spark.sql(s"select * from ${meta}.${tb}")
    val path = s"$backBase/$tb"
    df.coalesce(1)
      .write
      .option("header", "true")
      .option("sep", ",")
      .mode("overwrite")
      .csv(path)
  }

  def runTpcdsTableDDLs(db: String, ddl: String): Unit = {
    val tableVariable = "VAR_TABLE"
    assert(ddl.contains(tableVariable), "query template must contain VAR_TABLE")
    useDatabase(db)
    getTPCDSTables.foreach { table =>
      val tn = table.getName
      val newDDL = ddl.replaceFirst(tableVariable, tn)
      try {
        runDDL(newDDL)
      } catch {
        case e: Throwable =>
          println(e.getMessage)
          e.printStackTrace()
      }
    }
  }

  def runTpcdsTableActions(db: String, action: (String) => Unit): Unit = {
    useDatabase(db)
    getTPCDSTables.foreach { table =>
      LOG.warn(s"running on table $table")
      action(table.getName)
    }
  }

  def runTpcdsTableDMLs(
    db: String, dml: String,
    hasRun: String => Boolean,
    metricAction: (String, Long, Long) => Unit
  ): Unit = {
    val tableVariable = "VAR_TABLE"
    assert(dml.contains(tableVariable), "query template must contain VAR_TABLE")
    useDatabase(db)
    getTPCDSTables.foreach { table =>
      val tn = table.getName
      if (hasRun(tn)) {
        println(s"${tn} has ran, skipped")
      } else {
        val newDDL = dml.replaceFirst(tableVariable, tn)
        try {
          val st = System.currentTimeMillis()
          runDDL(newDDL)
          val ed = System.currentTimeMillis()
          metricAction(tn, st, ed)
        } catch {
          case e: Throwable =>
            println(e.getMessage)
            e.printStackTrace()
        }
      }
    }
  }

  def getTableDDL(db: String, tb: String, sf: Int, hive: String, format: String, partition: Boolean): String = {
    val fmtClause = hive match {
      case "hive" => s"stored as $format"
      case "" => s"using $format"
      case _ => "using iceberg"
    }

    val partColsOpt = TPCDSTable.tablePartitionColumns.get(tb)
    val partClause = partColsOpt match {
      case Some(partCols) if partition =>
        val colStr = partCols.mkString(",")
        s"partitioned by(${colStr})"
      case _ =>
        ""
    }
    val tbProps = if ("iceberg".equalsIgnoreCase(format) && "".equals(hive)) {
      "TBLPROPERTIES ('read.parquet.vectorization.enabled' = 'true', 'write.parquet.compression-codec' = 'snappy')"
    } else {
      ""
    }

    val orderClause = partColsOpt match {
      case Some(partCols) if partition =>
        val colStr = partCols.mkString(",")
        s"order by ${colStr}"
      case _ =>
        ""
    }

    s"create table if not exists $db.$tb $fmtClause $partClause $tbProps as select * from tpcds.sf$sf.$tb $orderClause"
  }

  def createDatabase(sf: Int, format: String, hive: String, partition: Boolean = false): String = {
    import spark.implicits._
    // let's name db like: tpcds_sf10_parquet
    val prefix = if (partition) "tpcdsp" else "tpcds"
    val db = if (StringUtils.isEmpty(hive)) {
      s"${prefix}_sf${sf}_$format"
    } else {
      s"${prefix}_sf${sf}_${hive}_$format"
    }

    if (createDb) {
      val loc = s"$dwLoc/$db"
      val createDbDdl = s"CREATE DATABASE IF NOT EXISTS ${db} location '$loc'"
      runDDL(createDbDdl)
    }

    val catalog = classOf[TPCDSCatalog].getName
    setSparkConf("spark.sql.catalog.tpcds", catalog)

    getTPCDSTables.foreach { table =>
      val tb = table.getName.toLowerCase
      if (tableCreated(db, tb)) {
        println(s"create $db.$tb ok, skip exist")
      } else {

        // requires tpcds catalog
        val cql = getTableDDL(db, tb, sf, hive, format, partition)
        try {
          val gmtCreate = System.currentTimeMillis()
          runDDL(cql)
          val gmtEnd = System.currentTimeMillis()

          val user = getUserName
          val oktc = TableCreation(db, sf, tb, format, hive, cql, gmtCreate, gmtEnd, user)
          appendEntry[TableCreation](Seq(oktc).toDF)
          println(s"create $db.$tb ok")
        } catch {
          case e: Exception =>
            println(s"create $db.$tb error ${e.getCause}")
        }
      }
    }
    "{}"
  }

}
