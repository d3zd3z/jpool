//////////////////////////////////////////////////////////////////////
// Command line restore.
//
// Usage:
//   Restore jpool:file:///path hash > tarball

package org.davidb.jpool.tools

import java.net.URI
import java.nio.channels.Channels
import org.davidb.jpool.pool.{TarRestore, PoolFactory}

object Restore {
  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Usage: Restore jpool:file:///path hash > tarball")
      System.exit(1)
    }

    val pool = PoolFactory.getInstance(new URI(args(0)))
    val stdout = Channels.newChannel(System.out)
    val restorer = new TarRestore(pool, stdout)
    restorer.decode(Hash.ofString(args(1)))
    restorer.finish()
    pool.close
  }
}
