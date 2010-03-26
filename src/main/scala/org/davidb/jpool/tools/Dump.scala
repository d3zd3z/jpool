//////////////////////////////////////////////////////////////////////
// Command line backup storage.
//
// Usage:
//   Dump jpool:file:///path path

package org.davidb.jpool.tools

import org.davidb.logging.Logger

import jpool.pool.{TreeSave, PoolFactory}
import java.net.URI
import java.util.Properties

object Dump extends AnyRef with Logger {
  def main(args: Array[String]) {
    if (args.length < 3) {
      logError("Usage: Dump jpool:file:///path dumpdir key=value key=value")
      System.exit(1)
    }

    Progress.open()
    val pool = PoolFactory.getStoreInstance(new URI(args(0)))
    val props = new Properties
    props.setProperty("_date", new java.util.Date().getTime.toString)
    scanProperties(args drop 2, props)

    val saver = new TreeSave(pool, args(1))
    val hash = saver.store(props)
    info("backup saved: %s", hash)
    Progress.close()
    pool.close()
  }

  private def scanProperties(args: Array[String], props: Properties) {
    for (arg <- args) {
      arg.split("=", 2) match {
        case Array(key, value) => props.setProperty(key, value)
        case _ =>
          logError("Illegal key=valu argument '%s'", arg)
          System.exit(1)
      }
    }
  }
}
