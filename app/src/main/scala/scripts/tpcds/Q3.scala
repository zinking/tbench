package scripts.tpcds

import org.apache.spark.sql.SparkSession
import tpcds.action.TPCDSQueryRunAction
import tpcds.table.TPCDS14


class Q3(spark: SparkSession) {
  val qa = new TPCDSQueryRunAction(spark, "zhnwang_tpcds_meta")

  lazy val queryString: String = {
    var result = ""
    new TPCDS14().queriesWithId.filter(_._2.equals("q3")).foreach { case (query, qid) =>
      result = query
    }
    result
  }

  def timeNs[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    result
  }

  def timeMs[R](block: => R): R = {
    val t0 = System.currentTimeMillis()
    val result = block    // call-by-name
    val t1 = System.currentTimeMillis()
    println("Elapsed time: " + (t1 - t0) + "ms")
    result
  }

  def arthas(): Unit = {

//    val stub = new ArthasStub();
    val q3 = new Q3(spark);
    spark.sql("use tpcdsp_sf1000_iceberg")
//    q3.timeMs({ spark.sql("explain " + q3.queryString).show()})

    // org.apache.spark.sql.execution.command.ExplainCommand.run
    // val target = "org.apache.spark.sql.execution.QueryExecution explainString"
    // val target = "org.apache.spark.sql.execution.command.ExplainCommand run"
    // val target = "org.apache.iceberg.spark.source.SparkBatchScan getMetaData"
    // val target = "org.apache.spark.sql.execution.datasources.v2.ExtendedBatchScanExec simpleString"

//    val session = ArthasSession.getSession
    var target = "org.apache.spark.sql.execution.command.ExplainCommand run"
//    target = "org.apache.spark.sql.execution.datasources.v2.ExtendedBatchScanExec nodeName"
//    target = "org.apache.spark.sql.execution.QueryExecution explainString"
//    target = "org.apache.spark.sql.execution.QueryExecution org$apache$spark$sql$execution$QueryExecution$$explainString"
//    target = "org.apache.spark.sql.execution.QueryExecution simpleString"
//    target = "org.apache.spark.sql.catalyst.plans.QueryPlan$ append"
    target = "org.apache.iceberg.spark.source.SparkBatchScan planInputPartitions"
    val thresh:Long = 5000
    var args = s"'#cost>$thresh'"
    args = ""
    var cmd = ""
//  cmd = s"trace $target $args"
//    args = "'{params, returnObj}'  -x 2 -b"
//    args = ""
//    cmd = s"watch $target $args"
    args = ""
    cmd = s"stack $target $args"
//    session.makeAsyncCommand(cmd)
//    session.pullResults()
//    q3.timeMs({ spark.sql("explain " + q3.queryString).show()})
//    session.pullResults()
//    session.interrupt()
//    session.close()
  }

}
