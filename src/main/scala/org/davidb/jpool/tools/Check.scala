//////////////////////////////////////////////////////////////////////
// Validation of 1 or more pool files.
//
// Usage:
//   Check poolpath poolpath ...

package org.davidb.jpool.tools

import java.io.File
import org.davidb.logging.Loggable
import org.davidb.jpool.pool.PoolFile

object Check extends AnyRef with Loggable {
  def main(args: Array[String]) {
    val meter = Progress.open()
    args.foreach(scan (_, meter))
    meter.close()
  }

  // TODO: Generalize the progress meter so that this display can give
  // more meaningful information.
  def scan(path: String, meter: Progress) {
    meter.reset()
    logger.info("Scanning: %s" format path)
    val pf = new PoolFile(new File(path))
    val size = pf.size
    while (pf.position < size) {
      val pos = pf.position
      val (chunk, hash) = pf.readUnchecked(pos)
      meter.addData(pf.position - pos)
      if (hash != chunk.hash)
        logger.warn("Chunk hash mismatch in %s at %d" format(path, pos))
    }
  }
}
