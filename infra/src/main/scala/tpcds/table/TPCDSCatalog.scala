package tpcds.table

import io.trino.tpcds.Table
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.connector.catalog.TableChange
import org.apache.spark.sql.connector.catalog.{Table => SparkTable}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._

// TODO: implement SupportsNamespaces
class TPCDSCatalog extends TableCatalog {

  val tables: Array[String] = Table.getBaseTables.asScala
    .map(_.getName).filterNot(_ == "dbgen_version").toArray

  val scales: Array[Int] = Array(0, 1, 10, 100, 300, 1000, 3000, 10000, 30000, 100000)

  val databases: Array[String] = scales.map("sf" + _)

  override def name: String = "tpcds"

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {}

  override def listTables(namespace: Array[String]): Array[Identifier] = namespace match {
    case Array(db) if databases contains db => tables.map(Identifier.of(namespace, _))
    case _ => throw new NoSuchNamespaceException(namespace.mkString("."))
  }

  override def loadTable(ident: Identifier): SparkTable = (ident.namespace, ident.name) match {
    case (Array(db), table) if databases contains db =>
      new TPCDSTable(table.toLowerCase, scales(databases indexOf db))
    case (_, _) => throw new NoSuchTableException(ident)
  }

  override def createTable(
                            ident: Identifier,
                            schema: StructType,
                            partitions: Array[Transform],
                            properties: util.Map[String, String]): SparkTable =
    throw new UnsupportedOperationException

  override def alterTable(ident: Identifier, changes: TableChange*): SparkTable =
    throw new UnsupportedOperationException

  override def dropTable(ident: Identifier): Boolean =
    throw new UnsupportedOperationException

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit =
    throw new UnsupportedOperationException
}

