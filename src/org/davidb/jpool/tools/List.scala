//////////////////////////////////////////////////////////////////////
// Listing backups stored in pool.
//
// Usage:
//    List jpool:file:///path

package org.davidb.jpool.tools

import java.net.URI
import java.text.{SimpleDateFormat}
import java.util.{Date, Properties, TimeZone}
import org.davidb.jpool.pool.{TarRestore, PoolFactory}
import scala.collection.jcl

object List {
  def main(args: Array[String]) {
    if (args.length != 1) {
      System.err.println("Usage: List jpool:file:///path")
      System.exit(1)
    }

    val pool = PoolFactory.getInstance(new URI(args(0)))

    for (hash <- pool.getBackups) {
      val (props, _) = TarRestore.lookupHash(pool, hash)
      printf("%s %s%n", hash, sanitize(props))
    }
    pool.close
  }

  def sanitize(props: Properties): String = {
    val allKeys = (jcl.Set(props.stringPropertyNames) - "_date" - "hash").toArray
    val result = new StringBuilder

    val date = new Date(props.getProperty("_date").toLong)
    val fmt = new SimpleDateFormat("yyyy-MM-dd")
    result.append(fmt.format(date))

    scala.util.Sorting.stableSort(allKeys)
    for (key <- allKeys) {
      result.append(' ')
      result.append(key)
      result.append('=')
      result.append(props.getProperty(key))
    }

    result.toString
  }
}
