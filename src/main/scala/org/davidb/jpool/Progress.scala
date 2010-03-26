//////////////////////////////////////////////////////////////////////
// Simple progress meter.

package org.davidb.jpool

import java.util.Date
import org.davidb.logging.Logger

object Progress extends AnyRef with Logger {
  // API used by things writing data.

  // Ordinary data written (as in chunk data).
  def addData(count: Long) = synchronized { state.data += count; check }

  // Dedupped data.
  def addDup(count: Long) = synchronized { state.dup += count; check }

  // Data entirely skipped (file is already known).
  def addSkip(count: Long) = synchronized { state.skip += count; check }

  // Indicate files added.
  def addNode() { addNode(1) }
  def addNode(count: Long) = synchronized { state.nodes += count; check }

  def reset() {
    state.reset()
    nextUpdate = None
  }

  def open() = synchronized {
    if (opened)
      error("Progress meter already opened")
    opened = true
    Logger.pushWrapper(logTag, logMessage _)
  }
  def close() = synchronized {
    if (!opened)
      error("Progress meter not opened")
    show(true)
    nextUpdate = None
    linesPrinted = 0
    opened = false
    Logger.popWrapper(logTag)
  }

  // Information display.
  abstract class Info {
    // Return an info display consisting of an array of lines to
    // display the information.
    def getInfo: Array[String]
  }

  class BackupInfo {
    var data = 0L
    var dup = 0L
    var skip = 0L
    var nodes = 0L
    def getInfo: Array[String] = {
      Array("[data: %s, dup, %s, skip: %s, total: %s, nodes: %s]" format(
        humanize(data), humanize(dup), humanize(skip),
        humanize(data + dup + skip),
        nodes))
    }
    def reset() {
      data = 0L
      dup = 0L
      skip = 0L
      nodes = 0L
    }
  }
  private val state = new BackupInfo

  //////////////////////////////////////////////////////////////////////
  // Implementation.

  private var opened = false
  private val stdout = System.console.writer

  private def check {
    if (!opened)
      error("Progress meter has not been opened")
    if (timeForUpdate()) {
      show()
    }
  }

  // Called by the logger to wrap messages.
  private val logTag = new Object
  private def logMessage(thunk: => Unit) = synchronized {
    val oldLines = linesPrinted
    if (oldLines > 0)
      clear
    thunk
    if (oldLines > 0)
      show(true)
  }

  private var linesPrinted = 0
  // Print the update nicely.
  private def clear() = synchronized {
    if (linesPrinted > 0) {
      Console.printf("\033[%dA\033[J", linesPrinted)
      linesPrinted = 0
    }
  }

  def show() { show(false) }
  def show(force: Boolean) = synchronized {
    clear()
    val lines = state.getInfo
    for (line <- lines)
      Console.printf("%s\n", line)
    linesPrinted = lines.length
    Console.flush()
  }

  private var nextUpdate: Option[Long] = None

  // Is it time for the next update?
  private def timeForUpdate(): Boolean = {
    val now = new Date().getTime()
    nextUpdate match {
      case Some(when) if (now < when) =>
        false
      case _ =>
        nextUpdate = Some(now + 1000L)
        nextUpdate != None
    }
  }

  // Convert a large number into a nice human readable format.
  // Largest result would be 1023.9GiB or 9 characters.
  private def humanize(num: Double): String = {
    var answer = num
    var unit = units
    while (answer > 1024.0) {
      answer /= 1024.0
      unit = unit.tail
    }
    "%6.1f%s" format (answer, unit.head)
  }
  private val units = List("B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB")
}
