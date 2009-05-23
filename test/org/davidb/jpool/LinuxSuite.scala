//////////////////////////////////////////////////////////////////////
// Test native JNI.

package org.davidb.jpool

import java.io.IOException
import org.scalatest.Suite
import scala.collection.mutable.ListBuffer

class LinuxSuite extends Suite {
  def testNative {
    assert(Linux.message == "Hello world")
  }

  def testDirs {
    val usrBinSet = Set() ++ Linux.readDir("/usr/bin")
    val findSet = Set() ++ findReadDir("/usr/bin")
    assert(usrBinSet === findSet)
  }

  def testBadDir {
    intercept[IOException] {
      Linux.readDir("/invalidDir/stuff")
    }
  }

  def testBadStat {
    intercept[IOException] {
      Linux.lstat("/invalidDir/stuff")
    }
  }

  private def checkName(name: String, kind: String) {
    val stat = Linux.lstat(name)
    // printf("lstat(%s): %s%n", name, stat)
    checkStat(name, stat, kind)
  }

  def testStats {
    // TODO: Device nodes and such aren't that portable.
    checkName("/bin/ls", "REG")
    checkName("/dev/null", "CHR")
    checkName("/dev/sda", "BLK")
    checkName(".", "DIR")
    // TODO: Once we have code to make these, test them.
    // checkName("/etc/make.profile", "LNK")
  }

  // Duplicate the native readdir, using find.
  private def findReadDir(path: String): List[(String, Long)] = {
    val result = new ListBuffer[(String, Long)]
    for (line <- Proc.runAndCapture("find", path, "-maxdepth", "1", "-printf", "%P\\t%i\\n")) {
      line.split("\t", 2) match {
        case Array(name, num) =>
          // Find generates a blank name for '.', which
          // Linux.readDir skips.
          if (name.length > 0)
            result += (name, num.toLong)
        case _ =>
          printf("Unknown output from find: '%s'%n", line)
      }
    }
    result.toList
  }

  private def checkStat(path: String, info: Map[String, String], kind: String) {
    assert(info("*kind*") === kind)
    val fields = Proc.runAndCapture("stat", "--format", "%s %Y %Z %h %u %g %f %i %d %t %T", path) match {
      case Array(line) => line.split(" ")
      case _ => error("Invalid response from 'stat' call")
    }
    def hexField(n: Int) = java.lang.Long.parseLong(fields(n), 16)
    // printf("fields: %s", fields.toList)
    assert(fields(0) === info("size"))
    assert(fields(1) === info("mtime"))
    assert(fields(2) === info("ctime"))
    assert(fields(3) === info("nlink"))
    assert(fields(4) === info("uid"))
    assert(fields(5) === info("gid"))
    assert((hexField(6) & 07777).toString === info("mode"))
    assert(fields(7) === info("ino"))
    assert(fields(8) === info("dev"))
    if (kind == "CHR" || kind == "BLK") {
      assert((hexField(9) * 256 + hexField(10)).toString === info("rdev"))
    }
  }
}
