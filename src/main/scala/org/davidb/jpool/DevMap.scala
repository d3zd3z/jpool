//////////////////////////////////////////////////////////////////////
// Persistent device ID mapping.
// Linux-specific implementation, using 'blkid' output.
//
// There are two systems on Linux generally used for device mapping,
// both with potential problems.  One is the 'blkid' program that is
// part of e2fsprogs.  This seems to be pretty much universally
// available.  The other is 'udev' which is used on more modern
// systems.  Both have some disadvantages:
//
// The main disadvantage to 'blkid' is that it cannot update it's
// database unless run as root.  Since many backups are run as root,
// this normally isn't a problem, but it can cause user-run dumps to
// misidentify devices.
//
// 'udev' has a problem in that it tends to not update its information
// upon device removal and insertion.  Because of this, we're going to
// start with just using blkid.
//
// TODO: Generalize this code so that other platforms can use their
// own detection (e.g. using disktool or native APIs on OSX).

package org.davidb.jpool

import scala.util.parsing.combinator._
import scala.util.parsing.input._

import org.davidb.logging.Loggable

class DevMap extends scala.collection.Map[Long, String] with Loggable {
  override def size: Int = theMap.size
  def get(key: Long): Option[String] = theMap.get(key)
  def iterator: Iterator[(Long, String)] = theMap.iterator
  def - (key: Long) = error("Cannot delete from devmap")
  def + [B >: String] (kv: (Long, B)) = error("Cannot add to devmap")

  object BlkIDParse extends RegexParsers {
    override val whiteSpace = "".r
    val dev: Parser[String] = """/dev/[/0-9a-zA-Z\.-]+""".r <~ """:\s+""".r
    val key: Parser[String] = """[A-Z]+""".r
    val value: Parser[String] = "=\"" ~> """[^"]*""".r <~ """"\s*""".r
    val pair: Parser[(String, String)] = (key ~ value) ^^ (_ match {
        case ~(k, v) => (k, v)
      })

    val pairs: Parser[Map[String, String]] = rep(pair) ^^ (Map() ++ _)
    val line: Parser[(String, Map[String, String])] =
      dev ~ pairs ^^ (_ match {
        case ~(k, v) => (k, v)
      })

    // TODO: Use a single parser for all of this, rather than a
    // separate one for each line.
    def parseLine(text: String): (String, Map[String, String]) = {
      val reader = new CharSequenceReader(text, 0)
      val result = line(reader)
      if (!result.successful)
        error("Parse error on blkid output '%s'" format text)
      result.get
    }
  }

  def parseIDs: Map[Long, String] = {
    var warns = 0
    var result = Map[Long, String]()
    for (line <- Proc.runAndCapture("/sbin/blkid")) {
      val (dev, mp) = BlkIDParse.parseLine(line)
      try {
        if (mp contains "UUID") {
          val stat = Linux.stat(dev)
          val rdev = stat("rdev").toLong
          val uuid = mp("UUID")
          if ((result contains rdev) && result(rdev) != uuid) {
            logger.error("blkid output contains conflicting values")
            logger.error("device %d maps to %s" format (rdev, result(rdev)))
            logger.error("and to %s" format uuid)
            error("Fix blkid")
          }
          result += (rdev -> mp("UUID"))
        }
      } catch {
        case e: NativeError =>
          warns += 1
          if (warns == 1)
            logger.warn("blkid: %s" format e.getMessage())
      }
    }

    if (warns > 1)
      logger.warn("blkid: %d additional warnings" format(warns - 1))
    result
  }
  val theMap = parseIDs
}
