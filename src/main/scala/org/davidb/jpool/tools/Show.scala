//////////////////////////////////////////////////////////////////////
// Command line backup listing.
//
// Usage:
//   Show jpool:file:///path hash

package org.davidb.jpool
package tools

import org.davidb.jpool.pool.{TreeWalk, PoolFactory}
import java.net.URI

object Show {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: Show jpool:file:///path hash")
      System.exit(1)
    }

    val pool = PoolFactory.getInstance(new URI(args(0)))

    val lister = new TreeWalk(pool)
    lister.show(Hash.ofString(args(1)))
    pool.close()
  }
}
