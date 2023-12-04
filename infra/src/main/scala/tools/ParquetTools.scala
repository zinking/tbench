package tools

import org.apache.spark.sql.SparkSession

class ParquetTools(spark: SparkSession, file: String) {
  val fsTool: HDFSTools = new HDFSTools(spark)
}
