//////////////////////////////////////////////////////////////////////
// Validation of 1 or more pool files.
//
// Usage:
//   Check poolpath poolpath ...

package org.davidb.jpool.tools

import scala.actors.Actor
import scala.actors.Actor._
import java.io.File
import org.davidb.logging.Loggable
import org.davidb.jpool.pool.PoolFile

import java.util.concurrent.atomic.AtomicInteger

object Check extends AnyRef with Loggable {

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

    val meter = new BackupProgressMeter
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
  def scan(path: String, meter: BackupProgressMeter) {
    meter.reset()
    logger.info("Scanning: %s" format path)
    val pf = new PoolFile(new File(path))
    val size = pf.size
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
