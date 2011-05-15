/**********************************************************************/
// Listing backups stored in pool.
//
// Usage:
//    List jpool:file:///path

package org.davidb.jpool.tools

import java.net.URI
import java.text.{SimpleDateFormat}
import java.util.{Date, Properties, TimeZone}
import org.davidb.jpool.pool.{Back, TarRestore, PoolFactory}
import org.davidb.logging.Loggable
import scala.collection.JavaConversions._

object ListCommand extends AnyRef with Loggable {
  def main(args: Array[String]) {
    if (args.length < 1) {
      logger.error("Usage: List jpool:file:///path {key=value ...}")
      System.exit(1)
    }

    val pool = PoolFactory.getInstance(new URI(args(0)))
    val matcher = buildMatcher(args drop 1)

    val sanity = for {
      hash <- pool.getBackups
      val back = Back.load(pool, hash)
      if (matcher(back.props))
      (date, sane) = sanitize(back.props) }
      yield (date, sane, hash)
    pool.close
    for ((date, sane, hash) <- sanity.toList.sortWith((a, b) => datelt(a._1, b._1))) {
      printf("%s %s%n", hash, sane)
    }
  }

  private final def datelt(a: Date, b: Date): Boolean = {
    (a compareTo b) < 0
  }

  def sanitize(props: Properties): (Date, String) = {
    val allKeys = (Set.empty[String] ++ props.stringPropertyNames - "_date" - "hash").toArray
    val result = new StringBuilder

    val rawDate = {
      val dateStr = props.getProperty("_date")
      if (dateStr eq null)
        0
      else
        dateStr.toLong
    }
    val date = new Date(rawDate)
    val fmt = new SimpleDateFormat("yyyy-MM-dd_hh:mm")
    // val fmt = new SimpleDateFormat("yyyy-MM-dd")
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

  val dateGen = new SimpleDateFormat("yyyy-MM-dd_hh:mm")

  // Convert the properties specified in the command line to a
  // matching function that will return true if a set of properties
  // matches the user-requested filter.
  def buildMatcher(args: Seq[String]): (Properties => Boolean) = {
    def split(arg: String): (Properties => Boolean) = arg.split("=", 2) match {
      case Array(key, value) => (props => props.getProperty(key) == value)
      case _ =>
        logger.error("Illegal key=value argument '%s'%n" format arg)
        exit(1)
    }
    val pairs = args.map(split _)
    (props => pairs forall (_(props)))
  }
}
