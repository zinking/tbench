package tools

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.iceberg.CombinedScanTask
import org.apache.iceberg.FileScanTask
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown

import scala.collection.JavaConverters

class TableTools(spark: SparkSession, db: String, root: String = "hdfs://qy-teg-2-v3/user/tdw/warehouse") {

  def getfs(path: String): FileSystem = {
    val hadoopConf = spark.sessionState.newHadoopConf()
    val fsPath = new Path(path)
    fsPath.getFileSystem(hadoopConf)
  }

  def listTableDirs(tb: String, extra: String = ""): Unit = {
    val tb1 = TableIdentifier(tb, Some(db))
    val tm1 = spark.sessionState.catalog.getTableMetadata(tb1)

    val location = tm1.location.toString + "/" + extra
    val hdfsTools = new HDFSTools(spark)
    hdfsTools.ls(location)
  }

  def getTableScanObject(tb: String): Scan = {
    val dbtb = s"$db.$tb"
    val df = spark.read.format("iceberg").load(dbtb)
    val plan = df.queryExecution.analyzed
    val v2relation = V2ScanRelationPushDown(plan)
    val v2scanRelation = v2relation.asInstanceOf[DataSourceV2ScanRelation]
    v2scanRelation.scan
  }

  def getScanTasks(tb: String): Seq[CombinedScanTask] = {
    val scanObj = getTableScanObject(tb)
    val callResult = ReflectionTools.callMethod0(scanObj, "tasks")
    val tasks = callResult.asInstanceOf[java.util.ArrayList[CombinedScanTask]]
    JavaConverters.asScalaIterator(tasks.iterator()).toSeq
  }

  def getScanFiles(tb: String): Seq[FileScanTask] = {
    def getFiles(combTask: CombinedScanTask): Seq[FileScanTask] = {
      JavaConverters.asScalaIterator(combTask.files().iterator()).toSeq
    }
    getScanTasks(tb).flatMap(x => getFiles(x))
  }

  def tasksHead(tasks: Seq[FileScanTask], headn: Int = 5): Unit = {
    tasks.take(headn).foreach { file =>
      val df = file.file()
      val sz = df.fileSizeInBytes()
      val line = s"${sz}Bytes ${sz / 1024 / 1024} MB ${df.path()}"
      println(line)
    }
  }

  def verifyTasksExists(tasks: Seq[FileScanTask]): Unit = {
    val fs = getfs(root)
    tasks.foreach { task =>
      val path = task.file().path()
      val fsPath = new Path(path.toString)
      if (!fs.exists(fsPath)) {
        println(s"$path not exist in home")
      } else {
        print(".")
      }
    }
  }

  def uniqueFileTaskPaths(tasks: Seq[FileScanTask]): Set[String] = {
    tasks.map(x => x.file().path().toString).toSet
  }
}
