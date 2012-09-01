/**********************************************************************/
// Command line restore.
//
// Usage:
//   Restore jpool:file:///path hash > tarball

package org.davidb.jpool
package tools

import java.net.URI
import java.nio.channels.Channels
import org.davidb.jpool.pool.{Back, TarRestore, TreeRestore, PoolFactory}

import org.davidb.logging.Loggable

object Restore extends AnyRef with Loggable {
  def main(args: Array[String]) {
    if (args.length != 3) {
      logger.error("Usage: Restore jpool:file:///path hash --tar > tarball")
      logger.error("               jpool:file:///path hash path")
      sys.exit(1)
    }

    val meter = new BackupProgressMeter
    ProgressMeter.register(meter)
    val pool = PoolFactory.getInstance(new URI(args(0)))
    pool.setProgress(meter)
    val hash = Hash.ofString(args(1))
    val back = Back.load(pool, hash)
    if (args(2) == "--tar") {
      // Tar snapshots don't have a kind property, or it should be set to
      // 'tar' if it does have one.
      if (back.props.getProperty("kind", "tar") != "tar") {
        logger.error("Specified --tar, but indicate backup hash is not a tar snapshot")
        sys.exit(1)
      }
      val stdout = Channels.newChannel(System.out)
      val restorer = new TarRestore(pool, stdout, meter)
      restorer.decode(hash)
      restorer.finish()
    } else {
      // Otherwise, this should be a 'snapshot' backup.
      if (back.props.getProperty("kind") != "snapshot") {
        logger.warn("Specified backup is not a snapshot backup")
      }

      val restorer = new TreeRestore(pool, meter)
      restorer.restore(hash, args(2))
      ProgressMeter.unregister(meter)
    }
    pool.close
  }
}
