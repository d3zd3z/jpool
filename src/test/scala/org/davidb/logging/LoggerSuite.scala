/**********************************************************************/
// Testing of logging.

package org.davidb.logging

import org.scalatest.Suite

class LoggerSuite extends Suite with Loggable {
  // Just make sure that logging is possible.
  def testBasic {
    logger.info("This is a message for: %s (%d)".format("user", 42))
    if (!logger.isTraceEnabled) {
      logger.warn("Tracing is not enabled!")
    }
  }

  // Make sure that we can still cause error.
  def testError {
    intercept[RuntimeException] {
      error("Message")
      println("Should not be reached")
    }
  }

  def testSimpleWrapper {
    val myTag = new Object
    var phase = 1
    def wrapper(thunk: => Unit) {
      expect(1)(phase)
      // println("Pre wrap")
      thunk
      // println("Post wrap")
      phase += 1
    }
    Logger.pushWrapper(myTag, wrapper _)
    logger.info("Wrapped message")
    expect(2)(phase)
    Logger.popWrapper(myTag)
    logger.info("Unwrapped message")
    expect(2)(phase)

    intercept[RuntimeException] {
      Logger.popWrapper(myTag)
    }
  }

  def testNestedWrapper {
    val tag1 = new Object
    val tag2 = new Object
    var phase = 0
    def wrap1(thunk: => Unit) {
      expect(0)(phase)
      phase += 1
      thunk
      phase += 2
      expect(15)(phase)
    }
    def wrap2(thunk: => Unit) {
      expect(1)(phase)
      phase += 4
      thunk
      phase += 8
      expect(13)(phase)
    }
    Logger.pushWrapper(tag2, wrap2 _)
    Logger.pushWrapper(tag1, wrap1 _)
    logger.info("Wrapped message")
    Logger.popWrapper(tag1)
    Logger.popWrapper(tag2)
  }

  def testBadPush {
    val tag1 = new Object
    def wrap1(thunk: => Unit) { thunk }
    Logger.pushWrapper(tag1, wrap1 _)
    intercept[RuntimeException] {
      Logger.pushWrapper(tag1, wrap1 _)
    }
    Logger.popWrapper(tag1)
  }
}
