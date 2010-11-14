// Generate a backup key for a fresh pool.
// Should be run on a pool that has never had data written to it.

package org.davidb.jpool
package tools

import java.net.URI

import org.davidb.logging.Loggable

object GenKey extends AnyRef with Loggable {

  def main(args: Array[String]) {
    if (args.length != 1) {
      logger.error("Usage: GenKey jpool:file:///path")
      exit(1)
    }

    val p = pool.PoolFactory.getInstance(new URI(args(0)))
    p match {
      case p2: pool.FilePool =>
        p2.makeKeyPair()
      case _ =>
        logger.error("Can only generate keypairs for local file-based pools")
        exit(1)
    }
    p.close()
  }

}
