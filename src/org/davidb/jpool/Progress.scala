//////////////////////////////////////////////////////////////////////
// Simple progress meter.

package org.davidb.jpool

import java.util.Date
import org.davidb.logging.Logger

object Progress extends AnyRef with Logger {
  // API used by things writing data.

  // Ordinary data written (as in chunk data).
  def addData(count: Long) = synchronized { data += count; check }

  // Dedupped data.
  def addDup(count: Long) = synchronized { dup += count; check }

  // Data entirely skipped (file is already known).
  def addSkip(count: Long) = synchronized { skip += count; check }

  // Indicate files added.
  def addNode() { addNode(1) }
  def addNode(count: Long) = synchronized { nodes += count; check }

  // def reset()

  //////////////////////////////////////////////////////////////////////
  // Implementation.
  private var data = 0L
  private var dup = 0L
  private var skip = 0L
  private var nodes = 0L

  private def check {
    if (timeForUpdate()) {
      show()
    }
  }
  // Print the update nicely.
  def show() = synchronized {
    info("data: %s, dup: %s, skip: %s, total: %s, nodes: %d",
      humanize(data), humanize(dup), humanize(skip),
      humanize(data + dup + skip),
      nodes)
  }

  // Intervals for checking, this is a def to avoid keeping a ref to
  // head.  The basic idea is to print results more quickly at the
  // beginning and then settle on a slower interval to avoid
  // overwhelming things.
  private def intervals = Stream.concat(Stream.make(3, 10L), Stream(30L),
    Stream.make(3, 60L), Stream.const(360L)) map (_ * 1000L)
  private def updateTimes(nodes: Stream[Long], accum: Long): Stream[Long] = {
    if (nodes.isEmpty) Stream.empty
    else {
      val sum = nodes.head + accum
      Stream.cons(sum, updateTimes(nodes.tail, sum))
    }
  }

  private var nextUpdates: Stream[Long] = _

  // Is it time for the next update?
  private def timeForUpdate(): Boolean = {
    val now = new Date().getTime()
    if (nextUpdates eq null)
      nextUpdates = updateTimes(intervals, now)

    if (now >= nextUpdates.head) {
      nextUpdates = nextUpdates.tail
      true
    } else
      false
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
