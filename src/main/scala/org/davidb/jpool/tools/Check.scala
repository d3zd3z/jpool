//////////////////////////////////////////////////////////////////////
// Validation of 1 or more pool files.
//
// Usage:
//   Check poolpath poolpath ...

package org.davidb.jpool.tools

import java.io.File
import org.davidb.logging.Logger
import org.davidb.jpool.pool.PoolFile

object Check extends AnyRef with Logger {
  def main(args: Array[String]) {
    Progress.open()
    args.foreach(scan _)
    Progress.close()
  }

  // TODO: Generalize the progress meter so that this display can give
  // more meaningful information.
  def scan(path: String) {
    Progress.reset()
    info("Scanning: %s", path)
    val pf = new PoolFile(new File(path))
    val size = pf.size
    while (pf.position < size) {
      val pos = pf.position
      val (chunk, hash) = pf.readUnchecked(pos)
      Progress.addData(pf.position - pos)
      if (hash != chunk.hash)
        warn("Chunk hash mismatch in %s at %d", path, pos)
    }
  }
}
