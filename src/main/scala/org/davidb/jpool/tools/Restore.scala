//////////////////////////////////////////////////////////////////////
// Command line restore.
//
// Usage:
//   Restore jpool:file:///path hash > tarball

package org.davidb.jpool.tools

import java.net.URI
import java.nio.channels.Channels
import org.davidb.jpool.pool.{Back, TarRestore, TreeRestore, PoolFactory}

import org.davidb.logging.Logger

object Restore extends AnyRef with Logger {
  def main(args: Array[String]) {
    if (args.length != 3) {
      logError("Usage: Restore jpool:file:///path hash --tar > tarball")
      logError("               jpool:file:///path hash path")
      exit(1)
    }

    val pool = PoolFactory.getInstance(new URI(args(0)))
    val hash = Hash.ofString(args(1))
    val back = Back.load(pool, hash)
    if (args(2) == "--tar") {
      // Tar snapshots don't have a kind property, or it should be set to
      // 'tar' if it does have one.
      if (back.props.getProperty("kind", "tar") != "tar") {
        logError("Specified --tar, but indicate backup hash is not a tar snapshot")
        exit(1)
      }
      val stdout = Channels.newChannel(System.out)
      val restorer = new TarRestore(pool, stdout)
      restorer.decode(hash)
      restorer.finish()
    } else {
      // Otherwise, this should be a 'snapshot' backup.
      if (back.props.getProperty("kind") != "snapshot") {
        logError("Specified backup is not a snapshot backup")
        exit(1)
      }

      val restorer = new TreeRestore(pool)
      restorer.restore(hash, args(2))
      Progress.show()
    }
    pool.close
  }
}