//////////////////////////////////////////////////////////////////////
// Test native JNI.

package org.davidb.jpool

import java.io.{BufferedReader, InputStreamReader, IOException}
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

  // Duplicate the native readdir, using find.
  private def findReadDir(path: String): List[(String, Long)] = {
    val proc = new ProcessBuilder("find", path, "-maxdepth", "1", "-printf", "%P\\t%i\\n").start
    drainError(proc)
    val findOut = new BufferedReader(new InputStreamReader(proc.getInputStream))
    val result = new ListBuffer[(String, Long)]
    def run() {
      val line = findOut.readLine()
      if (line ne null) {
        line.split("\t", 2) match {
          case Array(name, num) =>
            // Find generates a blank name for '.', which
            // Linux.readDir skips.
            if (name.length > 0)
              result += (name, num.toLong)
            run()
          case _ =>
            printf("Unknown output from find: '%s'%n", line)
        }
      }
    }
    run()
    assert(proc.waitFor() === 0)
    result.toList
  }

  private def drainError(proc: Process) {
    val child = new Thread {
      val input = new BufferedReader(new InputStreamReader(proc.getErrorStream))
      override def run() {
        val line = input.readLine()
        if (line ne null) {
          printf("find: '%s'%n", line)
          run()
        }
      }
    }
    child.setDaemon(true)
    child.start()
  }
}
