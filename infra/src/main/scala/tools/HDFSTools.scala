package tools

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.SparkSession
import java.util.Date

class HDFSTools(spark: SparkSession) {
  def getfs(path: String): FileSystem = {
    val hadoopConf = spark.sessionState.newHadoopConf()
    val fsPath = new Path(path)
    fsPath.getFileSystem(hadoopConf)
  }

  def cp(frm: String, tgt: String): HDFSTools = {
    val frmPath = new Path(frm)
    val tgtPath = new Path(tgt)
    val fsl1 = getfs(frm)
    val fsl2 = getfs(tgt)
    FileUtil.copy(fsl1, frmPath, fsl2, tgtPath, false, fsl1.getConf)
    this
  }

  def ls(path: String): HDFSTools = {
    val hadoopConf = spark.sessionState.newHadoopConf()
    val t1path = new Path(path)
    val t1paths = Array(t1path)
    val fs = getfs(path)

    val leafs = fs.listStatus(t1paths)

    var totalBytes = 0L
    var count = 0L
    def fmtDate(ts: Long): String = {
      import java.text.SimpleDateFormat
      val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      formatter.format(new Date(ts))
    }

    leafs.foreach { leaf =>
      val flen = leaf.getLen
      val flenMb = flen / 1024 / 1024
      val path = leaf.getPath
      val mdt = fmtDate(leaf.getModificationTime)
      val adt = fmtDate(leaf.getAccessTime)
      println(s"${flen} ${flenMb} ${mdt} ${adt} ${path}")
      totalBytes += flen
      count += 1
    }
    val totalMb = totalBytes / 1024 / 1024
    println(s"summary total: $totalBytes Bytes,  $totalMb MB, count: $count, avg: ${totalMb / count} Mb")
    this
  }

  def cat(path: String): HDFSTools = {
    val t1path = new Path(path)
    val fs = getfs(path)
    val inputStream = fs.open(t1path)

    val theString = IOUtils.toString(inputStream, "UTF-8")
    println(theString)
    this
  }

  def catAvro(path: String, num: Int = 100): HDFSTools = {
    val fileDF = spark.read.format("avro").load(path)
    fileDF.show(num)
    fileDF.registerTempTable("temp")
    this
  }

  def catParquet(path: String, num: Int = 100): HDFSTools = {
    val fileDF = spark.read.format("parquet").load(path)
    fileDF.show(num)
    this
  }
}
