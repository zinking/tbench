/*
 * This Scala source file was generated by the Gradle 'init' task.
 */
package tpcds.action

import scala.Console.println
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

object TraceCat {
  val usage =
    """
  Usage: TraceCat [--file /var/tmp/trace.log] [--thresh threshold] /var/tmp/trace.log
  """

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println(usage)
    }

    def nextArg(map: Map[String, String], list: List[String]): Map[String, String] = {
      list match {
        case Nil => map
        case "--file" :: value :: tail =>
          nextArg(map ++ Map("file" -> value), tail)
        case "-f" :: value :: tail =>
          nextArg(map ++ Map("file" -> value), tail)
        case "--thresh" :: value :: tail =>
          nextArg(map ++ Map("thresh" -> value), tail)
        case "-t" :: value :: tail =>
          nextArg(map ++ Map("thresh" -> value), tail)
        case string :: Nil =>
          nextArg(map ++ Map("file" -> string), list.tail)
        case unknown :: _ =>
          println("Unknown option " + unknown)
          Map.empty
      }
    }
    val options = nextArg(Map(), args.toList)
    val file = options.getOrElse("file", "")
    val thresh = options.getOrElse("thresh", "5")
    traceCat( file, thresh.toInt)
  }

  def traceCat(file: String, thresh: Int): Unit = {
    val entries: Iterator[Entry] = if (file.nonEmpty) {
      Source.fromFile(file)
        .getLines
        .flatMap(toEntry)
    } else {
      Source.fromInputStream(System.in)
        .getLines()
        .flatMap(toEntry)
    }

    val callStack = new mutable.Stack[Entry]
    val logBuffer = new ListBuffer[Entry]

    def same(se: Entry, ce: Entry): Boolean = {
      se.method.equals(ce.method) && se.depth == ce.depth
    }
    while (entries.hasNext) {
      if (callStack.isEmpty) {
        logBuffer.map(toStr).foreach(println)
        logBuffer.clear()
        callStack.push(entries.next())
      } else {
        val top = callStack.top
        val cur = entries.next()
        if (same(top, cur)) {
          callStack.pop()
          val st = cur.ed - cur.st
          if (st > thresh) {
            logBuffer.insert(0, cur)
          }
        } else {
          callStack.push(cur)
        }
      }
    }
  }

  def toEntry(line: String): Option[Entry] = {
    val segs = line.split(",")
    if (segs.size < 6) {
      None
    } else {
      val error = if (segs.size > 6) {
        segs(6)
      } else {
        ""
      }

      Some(Entry(
        segs(0), segs(1), segs(2).toInt, segs(3), segs(4).toLong, segs(5).toLong, error
      ))
    }
  }

  def toStr(e: Entry): String = {
    val tab = "-" * e.depth
    val ts = e.ed - e.st
    s"${e.trace} ${e.ip} |$tab${e.method} $ts ms"
  }

}

case class Entry(
  ip: String,
  trace: String,
  depth: Int,
  method: String,
  st: Long,
  ed: Long,
  error: String
)
