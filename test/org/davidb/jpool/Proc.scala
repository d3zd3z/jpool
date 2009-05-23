//////////////////////////////////////////////////////////////////////
// Process based utilities for tests.

package org.davidb.jpool

import java.io.{BufferedReader, InputStreamReader}
import scala.collection.mutable.ArrayBuffer

object Proc {
  // Run the program capturing all of the lines of output and
  // returning them.  Stderr is printed as a message prefixed by
  // args(0).  Raises an assertion failure if the program returns a
  // non-zero exit status.
  def runAndCapture(args: String*): Array[String] = {
    val proc = new ProcessBuilder(args: _*).start
    drainError(proc, args(0))
    val procOut = new BufferedReader(new InputStreamReader(proc.getInputStream))
    val result = new ArrayBuffer[String]
    def run() {
      val line = procOut.readLine()
      if (line ne null) {
        result += line
        run()
      }
    }
    run()
    assert(proc.waitFor() == 0)
    result.toArray
  }

  def drainError(proc: Process, prefix: String) {
    val child = new Thread {
      val input = new BufferedReader(new InputStreamReader(proc.getErrorStream))
      override def run() {
        val line = input.readLine()
        if (line ne null) {
          printf("%s: '%s'%n", prefix, line)
          run()
        }
      }
    }
    child.setDaemon(true)
    child.start()
  }
}
