package tpcds.table

import io.trino.tpcds.Table
import io.trino.tpcds.column.ColumnType
import io.trino.tpcds.column.ColumnType.Base._
import org.apache.spark.sql.connector.catalog.SupportsRead
import org.apache.spark.sql.connector.catalog.TableCapability
import org.apache.spark.sql.connector.catalog.{Table => SparkTable}
import org.apache.spark.sql.connector.expressions.Expressions
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import TPCDSTable.tablePartitionColumns

import java.util
import java.util.Optional
import scala.collection.JavaConverters._

class TPCDSTable(tbl: String, scale: Int) extends SparkTable with SupportsRead {

  // When true, use CHAR VARCHAR; otherwise use STRING
  // TODO: make it configurable
  val useAnsiStringType: Boolean = false

  val tpcdsTable: Table = Table.getTable(tbl)

  override def name: String = s"`sf$scale`.`$tbl`"

  override def schema: StructType = {
    // TODO tpcdsTable.notNullBitMap does not correct, set nullable follows
    //      https://tpc.org/TPC_Documents_Current_Versions/pdf/TPC-DS_v3.2.0.pdf
    StructType(
      tpcdsTable.getColumns.zipWithIndex.map { case (c, i) =>
        StructField(c.getName, toSparkDataType(c.getType))
      })
  }

  override def partitioning: Array[Transform] = {
    tablePartitionColumns.get(tbl)
      .map { _ map Expressions.identity }
      .getOrElse(Array.empty[Transform])
  }

  override def capabilities(): util.Set[TableCapability] =
    Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new TPCDSBatchScan(tpcdsTable, scale, schema)
  }

  def toSparkDataType(tpcdsType: ColumnType): DataType = {
    (tpcdsType.getBase, tpcdsType.getPrecision.asScala, tpcdsType.getScale.asScala) match {
      case (INTEGER, None, None) => IntegerType
      case (IDENTIFIER, None, None) => LongType
      case (DATE, None, None) => DateType
      case (DECIMAL, Some(precision), Some(scale)) => DecimalType(precision, scale)
      case (VARCHAR, Some(precision), None) =>
        if (useAnsiStringType) VarcharType(precision) else StringType
      case (CHAR, Some(precision), None) =>
        if (useAnsiStringType) CharType(precision) else StringType
      case (t, po, so) =>
        throw new IllegalArgumentException(s"Unsupported TPC-DS type: ($t, $po, $so)")
    }
  }

  implicit final class RichOptional[T](val optional: Optional[T]) {
    def asScala: Option[T] = optional match {
      case null => null
      case _ => if (optional.isPresent) Option(optional.get) else None
    }
  }
}

object TPCDSTable {
  val tablePartitionColumns: Map[String, Array[String]] = Map(
    "catalog_sales" -> Array("cs_sold_date_sk"),
    "catalog_returns" -> Array("cr_returned_date_sk"),
    "inventory" -> Array("inv_date_sk"),
    "store_sales" -> Array("ss_sold_date_sk"),
    "store_returns" -> Array("sr_returned_date_sk"),
    "web_sales" -> Array("ws_sold_date_sk"),
    "web_returns" -> Array("wr_returned_date_sk"))
}

