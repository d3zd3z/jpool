//////////////////////////////////////////////////////////////////////
// My logger trait.

package org.davidb.logging

import org.apache.log4j
import java.io.Writer

object Logger {
  // If there is nothing happening, just printing the messages out is
  // sufficient.
  private val stdout = System.console.writer

  private val rootTag = new Object

  case class Elt(tag: Object, wrapper: (=> Unit) => Unit)

  // Each level of wrapper has an object associated with it, a tag,
  // that is used to identify and ensure that we are wrapping the
  // right thing.
  private def blankWrapper(thunk: => Unit) { thunk }
  private var wrapStack = List(Elt(rootTag, blankWrapper _))

  // Setting a wrapper.  The wrap function will be called with a
  // thunk, which must be called at some point.
  def pushWrapper(tag: Object, wrapper: (=> Unit) => Unit) {
    if (wrapStack.findIndexOf(_.tag eq tag) != -1)
      error("Attempt to push duplicate wrapper tag.")
    val next = wrapStack.head
    def chain(thunk: => Unit) {
      wrapper(next.wrapper(thunk))
    }
    wrapStack = Elt(tag, chain _) :: wrapStack
  }
  def popWrapper(tag: Object) {
    if (wrapStack.head.tag ne tag)
      error("PopWrapper of incorrect tag")
    wrapStack = wrapStack.tail
  }

  private class LogWriter extends Writer {
    private val buffer = new StringBuilder
    def write(buf: Array[Char], offset: Int, length: Int) {
      buffer.append(buf, offset, length)
    }
    def flush() {
      wrapStack.head.wrapper {
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

  private def loggerNameFor(cls: Class[_]) = {
    val name = cls.getName
    if (name endsWith "$")
      name.substring(0, name.length - 1)
    else
      name
  }

  def apply(cls: Class[_]) = {
    setupLogger
    log4j.Logger.getLogger(loggerNameFor(cls))
  }
  def apply(name: String) = {
    setupLogger
    log4j.Logger.getLogger(name)
  }
}

trait Loggable {
  @transient var logger = Logger(this.getClass)
}
