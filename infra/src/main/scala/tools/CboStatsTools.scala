package tools

import org.apache.iceberg.stats.{ColumnStat, Histogram, Statistics}

import scala.collection.JavaConverters.asScalaSetConverter

object CboStatsTools {

  def showNullCount(stats: Statistics, col: Long): String = {
    if (stats.columnStats().containsKey(col)) {
      s"null count: ${stats.columnStats().get(col).nullCount()}"
    } else {
      "NO COLUMN"
    }
  }
  def showStats(stats: Statistics): String = {
    def showHistogram(hist: Histogram): String = {
      if (hist == null) {
        return "No Histogram"
      }

      val bins = hist.bins().map { bin =>
        s"[${bin.lo}, ${bin.hi}], ${bin.ndv}"
      }.mkString("|")
      s"""
         |height: ${hist.height}
         |bins:
         | ${bins}
         |""".stripMargin
    }

    def showColumnStat(col: ColumnStat): String = {
      val histStr = showHistogram(col.histogram())
      s"""
         |distinctCount: ${col.distinctCount}
         |min: ${col.min}
         |max: ${col.max}
         |avgLen: ${col.avgLen}
         |maxLen: ${col.maxLen}
         |nullCount: ${col.nullCount}
         |Histogram:
         |${histStr}
         |""".stripMargin
    }

    def showColumns(stats: Statistics): String = {
      val cols = stats.columnStats()
      cols.entrySet().asScala.map { entry =>
        val id = entry.getKey
        val colStat: ColumnStat = entry.getValue
        val colStr = showColumnStat(colStat)
        s"""
           |column ${id}
           |${colStr}
           |""".stripMargin
      }.mkString("\n")
    }

    val columns = showColumns(stats)
    s"""
       |sizeInBytes: ${stats.sizeInBytes}
       |rowCount: ${stats.rowCount}
       |columns:
       |${columns}
       |""".stripMargin
  }




  def cmpStats(combined: Statistics, target: Statistics): String = {
    def cmpHistogram(combined: Histogram, target: Histogram): String = {
      if (combined == null && target == null) {
        return s"No Histogram"
      } else if (combined == null && target != null) {
        return s"missing Histogram | OK"
      }

      val combinedBins = combined.bins()
      val targetBins = target.bins()

      case class BinItem(llo: Double, rlo: Double, lndv: Long, rndv: Long)
      val zero = BinItem(0.0d, 0.0d, 0L, 0L)

      val binDiffs = combinedBins.zip(targetBins).foldLeft(List(zero)) { case (accu, (lbin, rbin)) =>
        val last = accu.last
        val llo = Math.max(lbin.lo(), last.llo)
        val rlo = Math.max(rbin.lo(), last.rlo)
        val lndv = last.lndv + lbin.ndv()
        val rndv = last.rndv + rbin.ndv()
        accu :+ BinItem(llo, rlo, lndv, rndv)
      }.zipWithIndex.dropWhile { case (_, index: Int) => (index % 10) != 0 }.map { case (item, index) =>
        s"""
           |${index}
           |lo: ${item.llo} | ${item.rlo}
           |ndv: ${item.lndv} | ${item.rndv}
           |""".stripMargin
      }.mkString("\n")
      s"""
         |height: ${combined.height}| ${target.height()}
         |bins: ${combined.bins.length}| ${target.bins.length}
         |binDiff:
         |${binDiffs}
         |""".stripMargin
    }

    def cmpColumnStat(combined: ColumnStat, target: ColumnStat): String = {
      val histDiff = cmpHistogram(combined.histogram(), target.histogram())
      s"""
         |distinctCount: ${combined.distinctCount()}| ${target.distinctCount()}
         |min: ${combined.min()}| ${target.min()}
         |max: ${combined.max()}| ${target.max()}
         |nullCount: ${combined.nullCount()}| ${target.nullCount()}
         |avgLen: ${combined.avgLen()}| ${combined.avgLen()}
         |maxLen: ${combined.maxLen()}| ${target.maxLen()}
         |Histogram:
         |$histDiff
         |""".stripMargin
    }

    def cmpColumns(combined: Statistics, target: Statistics): String = {
      val combinedColumns = combined.columnStats()
      val targetColumns = target.columnStats()
      val columnsCmp = targetColumns.entrySet().asScala.map{ entry =>
        val id = entry.getKey
        val targetColumnStat = entry.getValue
        if (combinedColumns.containsKey(id)) {
          val combinedColumnStat = combinedColumns.get(id)
          val columnDiff = cmpColumnStat(combinedColumnStat, targetColumnStat)
          s"""
             |column $id
             |${columnDiff}
             |""".stripMargin
        } else {
          s"column $id  missing |  OK"
        }
      }.mkString("\n")

      s"""
         |columnStats/columnCount: ${combinedColumns.size()}| ${targetColumns.size()}
         |${columnsCmp}
         |""".stripMargin
    }

    val columnDiff = cmpColumns(combined, target)
    s"""
       |Compare (Combined | Target)
       |sizeInBytes: ${combined.sizeInBytes}| ${target.sizeInBytes}
       |rowCount: ${combined.sizeInBytes}| ${target.sizeInBytes}
       |columnStats:
       |${columnDiff}
       |""".stripMargin
  }

}
