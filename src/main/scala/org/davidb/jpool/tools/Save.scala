//////////////////////////////////////////////////////////////////////
// Command line backup storage.
//
// Usage:
//   Save jpool:file:///path key=value key=value key=value ... < tarball

package org.davidb.jpool.tools

import java.net.URI
import java.util.Properties
import java.nio.channels.Channels
import org.davidb.jpool.pool.{TarSave, PoolFactory}

object Save {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: Save jpool:file:///path key=value key=value ... < tarball")
      System.err.println("Must specify at least one key=value pair")
      System.exit(1)
    }

    val pool = PoolFactory.getStoreInstance(new URI(args(0)))
    val meter = new BackupProgressMeter
    ProgressMeter.register(meter)
    pool.setProgress(meter)

    val props = new Properties
    props.setProperty("_date", new java.util.Date().getTime.toString)
    scanProperties(args drop 1, props)

    val stdin = Channels.newChannel(System.in)
    val saver = new TarSave(pool, stdin)
    val hash = saver.store(props)
    println(hash)
    pool.close
    ProgressMeter.unregister(meter)
  }

  private def scanProperties(args: Array[String], props: Properties) {
    for (arg <- args) {
      arg.split("=", 2) match {
        case Array(key, value) => props.setProperty(key, value)
        case _ =>
          System.err.print("Illegal key=value argument '%s'%n", arg)
          System.exit(1)
      }
    }
  }
}
