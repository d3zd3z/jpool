/**********************************************************************/
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
  // Return the current state formatted.  The 'forced' argument is a
  // hint about whether this update was forced by something else
  // (true), or a result of the periodic update.
  protected[jpool] def formatState(forced: Boolean): Array[String]

  // Internally, the meter also has an update function available,
  // which can be registered.
  protected def update() {
    if (fn.isEmpty)
      sys.error("Progress meter not registered")
    (fn.get)()
  }
  private var fn: Option[() => Unit] = None
  private[jpool] def setUpdater(theFn: () => Unit) {
    if (fn.isDefined)
      sys.error("ProgressState is already defined")
    fn = Some(theFn)
  }
  private[jpool] def clearUpdater() {
    if (fn.isEmpty)
      sys.error("Attempt to clearUpdater when already cleared")
    fn = None
  }
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

  def register(theMeter: ProgressMeter) = synchronized {
    // TODO: Handle more than one meter.
    require (meter eq null)
    Logger.pushWrapper(logTag, logMessage _)

    theMeter.setUpdater(update _)
    meter = theMeter
  }
  def unregister(theMeter: ProgressMeter) = synchronized {
    require (meter ne null)
    require (meter eq theMeter)
    show(true)
    meter = null
    Logger.popWrapper(logTag)
  }

  private var meter: ProgressMeter = null

  private val hasConsole = System.console ne null
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
    if (hasConsole && linesPrinted > 0) {
      Console.printf("\033[%dA\033[J", linesPrinted)
      linesPrinted = 0
    }
  }
  private def show() { show(false) }
  private def show(force: Boolean): Unit = synchronized {
    if (!hasConsole) return
    clear()
    val lines = meter.formatState(force)
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

// Backup data progress meter.
class BackupProgressMeter extends ProgressMeter with DataProgress {
  import ProgressMeter.humanize

  def addData(count: Long) = synchronized { data += count; update() }
  def addDup(count: Long) = synchronized { dup += count; update() }
  def addSkip(count: Long) = synchronized { skip += count; update() }
  def addNode() { addNode(1) }
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
  def formatState(forced: Boolean) = Array(
    "[data: %s, dup: %s, skip: %s, total: %s, nodes: %s]" format(
      humanize(data), humanize(dup), humanize(skip),
      humanize(data + dup + skip),
      nodes))
}
