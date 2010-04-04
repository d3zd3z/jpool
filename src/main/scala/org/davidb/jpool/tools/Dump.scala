//////////////////////////////////////////////////////////////////////
// Command line backup storage.
//
// Usage:
//   Dump jpool:file:///path path

package org.davidb.jpool.tools

import org.davidb.logging.Loggable

import jpool.pool.{TreeSave, PoolFactory}
import java.net.URI
import java.util.Properties

object Dump extends AnyRef with Loggable {
  def main(args: Array[String]) {
    if (args.length < 3) {
      logger.error("Usage: Dump jpool:file:///path dumpdir key=value key=value")
      System.exit(1)
    }

    val meter = new BackupProgressMeter
    ProgressMeter.register(meter)
    val pool = PoolFactory.getStoreInstance(new URI(args(0)))
    pool.setProgress(meter)
    val props = new Properties
    props.setProperty("_date", new java.util.Date().getTime.toString)
    scanProperties(args drop 2, props)

    val saver = new TreeSave(pool, args(1), meter)
    val hash = saver.store(props)
    logger.info("backup saved: %s" format hash)
    ProgressMeter.unregister(meter)
    pool.close()
  }

  private def scanProperties(args: Array[String], props: Properties) {
    for (arg <- args) {
      arg.split("=", 2) match {
        case Array(key, value) => props.setProperty(key, value)
        case _ =>
          logger.error("Illegal key=valu argument '%s'" format arg)
          System.exit(1)
      }
    }
  }
}
