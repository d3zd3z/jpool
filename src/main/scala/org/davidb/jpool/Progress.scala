//////////////////////////////////////////////////////////////////////
// Simple progress meter.

package org.davidb.jpool

import java.util.Date
import org.davidb.logging.{Logger, Loggable}

// Something that can log data and deduped data.
trait DataProgress {
  def addData(count: Long)
  def addDup(count: Long)
}

object NullProgress extends DataProgress {
  def addData(count: Long) {}
  def addDup(count: Long) {}
}

// A progress meter is anything that can give status periodically.  It
// should return an array of strings for the lines of the meter.
trait ProgressMeter {
  def formatMeter(): Array[String]

  // It can expect to be able to call this update method to inform the
  // meter framework that the meter is potentially changed.
  protected def update()
}

object ProgressMeter {
  // Convert a large number into a nice human readable format.
  // Largest result would be 1023.9GiB or 9 characters.
  def humanize(num: Double): String = {
    var answer = num
    var unit = units
    while (answer > 1024.0) {
      answer /= 1024.0
      unit = unit.tail
    }
    "%6.1f%s" format (answer, unit.head)
  }
  val units = List("B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB")
}

// The Progress object drives the progress meter.
object Progress { prog =>
  def open(): Progress = synchronized {
    if (meter ne null)
      error("Progress meter already opened")
    Logger.pushWrapper(logTag, logMessage _)
    val m = new BackupMeter
    meter = m
    m
  }
  def close() = synchronized {
    if (meter eq null)
      error("Progress meter not opened")
    show(true)
    meter = null
    Logger.popWrapper(logTag)
  }

  class BackupMeter extends BackupProgressMeter {
    // TODO: How to make these available.
    def show() = prog.show()
    def close() = prog.close()
    def update() = prog.update()
  }

  trait Updater extends ProgressMeter {
    def update() { prog.update() }
  }
  private var meter: ProgressMeter = null

  private val stdout = System.console.writer // TODO: redirected stdout.
  private val logTag = new Object
  private def logMessage(thunk: => Unit) = synchronized {
    val oldLines = linesPrinted
    clear()
    thunk
    if (oldLines > 0)
      show(true)
  }

  private def update() {
    if (timeForUpdate())
      show()
  }
  private var linesPrinted = 0
  private def clear() = synchronized {
    if (linesPrinted > 0) {
      Console.printf("\033[%dA\033[J", linesPrinted)
      linesPrinted = 0
    }
  }
  private def show() { show(false) }
  private def show(force: Boolean) = synchronized {
    clear()
    val lines = meter.formatMeter()
    for (line <- lines)
      Console.printf("%s\n", line)
    linesPrinted = lines.length
    Console.flush()
  }

  private var nextUpdate = new Date().getTime() + 1000
  private def timeForUpdate(): Boolean = {
    val now = new Date().getTime()
    val doit = (now >= nextUpdate)
    if (doit)
      nextUpdate = now + 1000
    doit
  }
}

trait Progress extends DataProgress {
  def addSkip(count: Long)
  def addNode(count: Long)
  def addNode() { addNode(1) }
  def close()
  def reset()
  def show()
}

// Backup data progress meter.
abstract class BackupProgressMeter extends ProgressMeter with Progress {
  import ProgressMeter.humanize

  def addData(count: Long) = synchronized { data += count; update() }
  def addDup(count: Long) = synchronized { dup += count; update() }
  def addSkip(count: Long) = synchronized { skip += count; update() }
  def addNode(count: Long) = synchronized { nodes += count; update() }
  def reset() = synchronized {
    data = 0L
    dup = 0L
    skip = 0L
    nodes = 0L
  }

  private var data = 0L
  private var dup = 0L
  private var skip = 0L
  private var nodes = 0L
  def formatMeter() = Array(
    "[data: %s, dup: %s, skip: %s, total: %s, nodes: %s]" format(
      humanize(data), humanize(dup), humanize(skip),
      humanize(data + dup + skip),
      nodes))
}
