//////////////////////////////////////////////////////////////////////
// Command line backup storage.
//
// Usage:
//   Dump jpool:file:///path path

package org.davidb.jpool.tools

import jpool.pool.{TreeSave, PoolFactory}
import java.net.URI

object Dump {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: Dump jpool:file:///path dumpdir")
      System.exit(1)
    }

    val pool = PoolFactory.getStoreInstance(new URI(args(0)))

    val saver = new TreeSave(pool, args(1))
    val hash = saver.store()
    printf("%s%n", hash)
    pool.close()
  }
}
