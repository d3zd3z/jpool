/**********************************************************************/
// Validation of 1 or more pool files.
//
// Usage:
//   Check poolpath poolpath ...

package org.davidb.jpool
package tools

object Check {
  def main(args: Array[String]) {
    printf("TODO: Port check to Scala 2.10\n")
  }
}

/* TODO: Rewrite with Akka

package org.davidb.jpool
package tools

import scala.actors.Actor
import scala.actors.Actor._
import java.io.File
import java.util.Date
import org.davidb.logging.Loggable
import org.davidb.jpool.pool.PoolFile

import java.util.concurrent.atomic.AtomicInteger

object Check extends AnyRef with Loggable {

  class CheckMeter extends ProgressMeter {
    import ProgressMeter.humanize

    private val start = new Date().getTime

    // TODO: Generalize the rate computation, and make the value decay
    // over time.
    def formatState(forced: Boolean): Array[String] = {
      val now = new Date().getTime
      val rate = {
        val delta = now - start
        if (delta < 3000)
          "???"
        else
          humanize((totalBytes.toDouble * 1000 / delta).round)
      }
      Array("Checking: \"%s\"" format rtrim(path, 50),
        "    this: %s, " format (humanize(bytes)),
        "   total: %s, %d errors, %s/sec" format (humanize(totalBytes), errorCount.intValue, rate))
    }

    private def rtrim(text: String, len: Int): String = {
      if (text.length <= len + 3)
        text
      else
        "..." + text.substring(text.length - len + 3)
    }

    private var path: String = ""
    private var length = 0L
    private var bytes = 0L
    private var totalBytes = 0L

    def setPath(path: String, length: Long) = synchronized {
      this.path = path
      this.length = length
      bytes = 0
    }

    def addData(count: Long) = synchronized {
      bytes += count
      totalBytes += count
      update()
    }
    def reset() = synchronized {
      bytes = 0L
      totalBytes = 0L
    }
  }

  val errorCount = new AtomicInteger(0)

  def main(args: Array[String]) {
    val tmpChunk = Chunk.make("blob", "")
    val numValidators = 2 * Runtime.getRuntime.availableProcessors
    for (i <- 1 to numValidators) {
      val v = new Validator
      v.start
      // Seed the validator with trivial work.
      v ! CheckValid(tmpChunk, tmpChunk.hash, self)
    }

    val meter = new CheckMeter
    ProgressMeter.register(meter)
    args.foreach(scan (_, meter))
    ProgressMeter.unregister(meter)

    for (i <- 1 to numValidators) {
      receive {
        case r: Response =>
          r.self ! CheckStop
      }
    }

    val ec = errorCount.intValue()
    if (ec == 0)
      logger.info("No errors in pool files")
    else if (ec == 1)
      logger.warn("Found 1 errors in pool files")
    else
      logger.warn("Found %d errors in pool files" format ec)
  }

  // TODO: Generalize the progress meter so that this display can give
  // more meaningful information.
  def scan(path: String, meter: CheckMeter) {
    val pf = new PoolFile(new File(path))
    val size = pf.size
    meter.setPath(path, size)
    while (pf.position < size) {
      val pos = pf.position
      // TODO: Handle errors in the read (bad header), possibly
      // finding other data.
      val (chunk, hash) = pf.readUnchecked(pos)
      meter.addData(pf.position - pos)
      receive {
        case v: Response =>
          // logger.info("Got a validator")
          v.self ! CheckValid(chunk, hash, self)
      }
    }
  }

  case class CheckValid(chunk: Chunk, hash: Hash, actor: Actor)
  case object CheckStop

  case class Response(self: Actor)

  class Validator extends Actor {
    def act() {
      react {
        case item: CheckValid =>
          try {
            if (item.hash != item.chunk.hash) {
              logger.warn("Chunk hash mismatch")
              errorCount.incrementAndGet()
            }
          } catch {
            case e: Exception =>
              logger.warn("Exception in chunk: " + e.toString)
              errorCount.incrementAndGet()
          }
          item.actor ! Response(this)
          act()
        case CheckStop =>
      }
    }
  }
}
*/
