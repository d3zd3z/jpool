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

    val sanity = for {
      hash <- pool.getBackups
      (props, _) = TarRestore.lookupHash(pool, hash)
      (date, sane) = sanitize(props) }
      yield (date, sane, hash)
    pool.close
    for ((date, sane, hash) <- sanity.toList.sort((a, b) => datelt(a._1, b._1))) {
      printf("%s %s%n", hash, sane)
    }
  }

  private final def datelt(a: Date, b: Date): Boolean = {
    (a compareTo b) < 0
  }

  def sanitize(props: Properties): (Date, String) = {
    val allKeys = (jcl.Set(props.stringPropertyNames) - "_date" - "hash").toArray
    val result = new StringBuilder

    val rawDate = props.getProperty("_date").toLong
    val date = new Date(rawDate)
    // val fmt = new SimpleDateFormat("yyyy-MM-dd hh:mm")
    val fmt = new SimpleDateFormat("yyyy-MM-dd")
    result.append(fmt.format(date))

    scala.util.Sorting.stableSort(allKeys)
    for (key <- allKeys) {
      result.append(' ')
      result.append(key)
      result.append('=')
      result.append(props.getProperty(key))
    }

    (date, result.toString)
  }
}
