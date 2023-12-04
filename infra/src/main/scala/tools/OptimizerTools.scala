package tools

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.SizeInBytesOnlyStatsPlanVisitor
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.SparkPlan

class OptimizerTools(spark: SparkSession) {

  def visitScanStats(plan: LogicalPlan): Unit = {
    plan transform {
      case p: LeafNode =>
        val stats = SizeInBytesOnlyStatsPlanVisitor.visit(p)
        println(s"${p.nodeName}:${stats}\n")
        p
      case f: Filter =>
        val stats = SizeInBytesOnlyStatsPlanVisitor.visit(f)
        println(s"${f.nodeName}:${stats}\n")
        f
    }

    def getScanNode(plan: SparkPlan, table: String): SparkPlan = {
      val hd = plan.collectLeaves().filter { p =>
        p match {
          case l : LeafExecNode => l.nodeName.contains("store")
        }
      }.head
      println(hd.getClass)
      hd
    }

  }

}
