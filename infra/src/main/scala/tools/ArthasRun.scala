package tools

import arthas.{ArthasCapture, ArthasSession}

case class ArthasRun(cmd: String) {

  def runAsync(callee: () => Unit): Unit = {
    val session = ArthasSession.getSession
    ArthasCapture.capturedObjs.clear()
    session.makeAsyncCommand(cmd)
    session.pullResults()
    callee()
    session.pullResults()
    session.interrupt()
    session.close()
  }

  def run(callee: () => Unit): Unit = {
    val session = ArthasSession.getSession
    ArthasCapture.capturedObjs.clear()
    session.makeCommand(cmd)
    session.pullResults()
    callee()
    session.pullResults()
    session.interrupt()
    session.close()
  }

  def runProfile(callee: () => Unit): Unit = {
    val session = ArthasSession.getSession
    ArthasCapture.capturedObjs.clear()
    session.makeAsyncCommand("profiler start")
    session.pullResults()

    callee()
    session.makeAsyncCommand("profiler getSamples")
    session.pullResults()

    session.makeAsyncCommand("profiler status")
    session.pullResults()

    session.makeAsyncCommand(s"profiler stop --file $cmd")
    session.pullResults()

    session.interrupt()
    session.close()
  }

}

object ArthasRun {
  import arthas._
  val stub = new ArthasStub()

  def makeWatch(target: String): ArthasRun = {
    makeWatch(target, "params, returnObj", "")
  }

  def makeWatch(target: String, params: String, condition: String): ArthasRun = {
    val watch = s"watch $target '$params' '$condition' -x 3 -s"
    ArthasRun(watch)
  }

  def makeTrace(target: String): ArthasRun = {
    val trace = s"trace $target"
    ArthasRun(trace)
  }

  def makeDecompile(target: String): ArthasRun = {
    val sc = s"jad $target"
    ArthasRun(sc)
  }

  def makeFlameGraph(path: String): ArthasRun = {
    ArthasRun(path)
  }
}