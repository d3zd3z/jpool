//////////////////////////////////////////////////////////////////////
// My logger trait.

package org.davidb.logging

import org.apache.log4j
import java.io.Writer

object Logger {
  // If there is nothing happening, just printing the messages out is
  // sufficient.
  private val stdout = System.console.writer

  // Something else can set a wrapper that will be invoked around the
  // write operation.
  private def blankWrapper(thunk: => Unit) { thunk }
  private var wrapper = blankWrapper _

  // Setting a wrapper.  The wrap function will be called with a
  // thunk, which must be called at some point.
  def clearWrapper() { wrapper = blankWrapper _ }
  def setWrapper(wrap: (=> Unit) => Unit) { wrapper = wrap }

  private class LogWriter extends Writer {
    private val buffer = new StringBuilder
    def write(buf: Array[Char], offset: Int, length: Int) {
      buffer.append(buf, offset, length)
    }
    def flush() {
      wrapper {
        stdout.write(buffer.toString)
        stdout.flush()
        buffer.setLength(0)
      }
    }
    def close() { }
  }
  private val writer = new LogWriter

  private var setupDone = false
  protected def setupLogger = synchronized {
    if (!setupDone) {
      val root = log4j.Logger.getRootLogger
      val layout = new log4j.PatternLayout("%d{ISO8601} %c - %m%n")
      root.addAppender(new log4j.WriterAppender(layout, writer))
      setupDone = true
    }
  }
}

trait Logger {
  private var myLogger = log4j.Logger.getLogger(this.getClass().getName())
  Logger.setupLogger

  /* If you get strange errors with these methods and are using Scala
   * version < 2.7.7, comment out the 'protected' declarations to work
   * around the bug. */
  protected def trace(text: String, args: Any*) { myLogger.trace(text format (args : _*)) }
  protected def debug(text: String, args: Any*) { myLogger.debug(text format (args : _*)) }
  protected def info(text: String, args: Any*) { myLogger.info(text format (args : _*)) }
  protected def warn(text: String, args: Any*) { myLogger.warn(text format (args : _*)) }
  protected def logError(text: String, args: Any*) { myLogger.error(text format (args : _*)) }
  protected def fatal(text: String, args: Any*) { myLogger.fatal(text format (args : _*)) }

  // Queries.
  protected def isTraceEnabled: Boolean = myLogger.isTraceEnabled
  protected def isDebugEnabled: Boolean = myLogger.isDebugEnabled
  protected def isInfoEnabled: Boolean = myLogger.isInfoEnabled
}
