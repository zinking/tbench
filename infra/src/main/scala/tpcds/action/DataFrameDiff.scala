package tpcds.action

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.round
import org.apache.spark.sql.types.{DecimalType, DoubleType, FloatType}

object DataFrameDiff {

  def compare(qid: String, srcDf: DataFrame, tgtDf: DataFrame): Boolean = {
    if (!srcDf.schema.equals(tgtDf.schema)) {
      println(s"$qid schema differ")
      return false
    }

    var src = srcDf
    var tgt = tgtDf
    srcDf.schema.fields.foreach { field =>
      field.dataType match {
        case s: DecimalType =>
        case s: FloatType =>
        case s: DoubleType =>
          src = src.withColumn(field.name, round(src.col(field.name), 3))
          tgt = tgt.withColumn(field.name, round(tgt.col(field.name), 3))
        case _ => Unit
      }
    }

    if (src.count() != tgt.count()) {
      println(s"$qid rows count differ: src ${src.count()} vs tgt ${tgt.count()}")
      return false
    }

    val diff = tgt.except(src)
    if (diff.count() == 0) {
      println(s"$qid result same")
      true
    } else {
      println(s"$qid result differ, details below:")
      println(s"\n\n\n$qid difference\n\n\n")
      diff.show()
      println(s"$qid src\n\n")
      src.show()
      println(s"$qid tgt\n\n")
      tgt.show()
      false
    }
  }

}
