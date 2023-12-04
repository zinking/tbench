package tools

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.iceberg.{CatalogUtil, Table}
import org.apache.iceberg.catalog.{Catalog, TableIdentifier}
import org.apache.iceberg.hive.HiveCatalog
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation


class Metastore(spark: SparkSession, catalogName: String = "iceberg_catalog") {
  val uris = "thrift://ss-qe-oms.tencent-distribute.com:8100"
  val conf = new Configuration();

  def getMetastoreClient(): HiveMetaStoreClient = {
    conf.set(HiveConf.ConfVars.METASTOREURIS.varname, uris);
    val hiveConf: HiveConf = new HiveConf(conf, classOf[HiveConf]);
    new HiveMetaStoreClient(hiveConf);
  }

  def loadTableWithCatalog(dbName: String, tableName: String): Table = {
    val identifier: TableIdentifier = TableIdentifier.of(dbName, tableName);
    val emptyMap = new java.util.HashMap[String, String]();
    val catalog: Catalog = CatalogUtil.loadCatalog(classOf[HiveCatalog].getName(), catalogName, emptyMap, conf);

    catalog.loadTable(identifier);
  }

  def loadTableWithPlan(dbName: String, tableName: String): Table = {
    val df = spark.sql(s"select * from $dbName.${tableName} limit 1")
    val leaf = df.queryExecution.optimizedPlan.collectLeaves.head.asInstanceOf[DataSourceV2ScanRelation]
    val scan = leaf.scan
    val batchScan = if (scan.getClass.getSimpleName.equals("SparkBatchQueryScan")) {
      scan
    } else {
      val batchProxy = java.lang.reflect.Proxy.getInvocationHandler(scan)
      ReflectionTools.getField(batchProxy, "actualBatchQueryScan")
    }
    val tb = ReflectionTools.callParentMethod0(batchScan, "table")
    tb.asInstanceOf[Table]
  }
}
