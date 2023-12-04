package tpcds.table

import io.trino.tpcds._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters._

case class TPCDSTableChuck(table: String, scale: Int, parallelism: Int, index: Int)
  extends InputPartition

// TODO with SupportsReportStatistics
//      https://tpc.org/TPC_Documents_Current_Versions/pdf/TPC-DS_v3.2.0.pdf
class TPCDSBatchScan(
                      @transient table: Table,
                      scale: Int,
                      schema: StructType) extends ScanBuilder with Scan with Batch with Serializable {

  private val rowCountPerTask: Int = 1000000

  private val rowCount: Long = new Scaling(scale).getRowCount(table)

  private val parallelism: Int =
    if (table.isSmall) 1
    else math.max(
      SparkSession.active.sparkContext.defaultParallelism,
      (rowCount / rowCountPerTask.toDouble).ceil.toInt)

  override def build: Scan = this

  override def toBatch: Batch = this

  override def description: String =
    s"Scan TPC-DS sf$scale.${table.getName}, count: $rowCount, parallelism: $parallelism"

  override def readSchema: StructType = schema

  override def planInputPartitions: Array[InputPartition] =
    (1 to parallelism).map { i => TPCDSTableChuck(table.getName, scale, parallelism, i) }.toArray

  def createReaderFactory: PartitionReaderFactory = (partition: InputPartition) => {
    val chuck = partition.asInstanceOf[TPCDSTableChuck]
    new TPCDSPartitionReader(chuck.table, chuck.scale, chuck.parallelism, chuck.index, schema)
  }
}

class TPCDSPartitionReader(
                            table: String,
                            scale: Int,
                            parallelism: Int,
                            index: Int,
                            schema: StructType) extends PartitionReader[InternalRow] {

  private val chuckInfo: Session = {
    val opt = new Options
    opt.table = table
    opt.scale = scale
    opt.parallelism = parallelism
    opt.toSession.withChunkNumber(index)
  }

  private lazy val dateFmt: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  private val iterator = Results
    .constructResults(chuckInfo.getOnlyTableToGenerate, chuckInfo)
    .iterator.asScala
    .map { _.get(0).asScala } // the 1st row is specific table row
    .map { row =>
      row.zipWithIndex.map { case (v, i) =>
        (v, schema(i).dataType) match {
          case (null, _) => null
          case (Options.DEFAULT_NULL_STRING, _) => null
          case (v, IntegerType) => v.toInt
          case (v, LongType) => v.toLong
          case (v, DateType) => LocalDate.parse(v, dateFmt).toEpochDay.toInt
          case (v, StringType) => UTF8String.fromString(v)
          case (v, CharType(_)) => UTF8String.fromString(v)
          case (v, VarcharType(_)) => UTF8String.fromString(v)
          case (v, DecimalType()) => Decimal(v)
          case (v, dt) => throw new IllegalArgumentException(s"value: $v, type: $dt")
        }
      }
    }
    .map { row => InternalRow.fromSeq(row.toSeq) }

  private var currentRow: InternalRow = _

  override def next(): Boolean = {
    val hasNext = iterator.hasNext
    if (hasNext) currentRow = iterator.next()
    hasNext
  }

  override def get(): InternalRow = currentRow

  override def close(): Unit = {}
}

