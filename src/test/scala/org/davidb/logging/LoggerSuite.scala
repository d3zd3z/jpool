//////////////////////////////////////////////////////////////////////
// Testing of logging.

package org.davidb.logging

import org.scalatest.Suite

class LoggerSuite extends Suite with Logger {
  // Just make sure that logging is possible.
  def testBasic {
    info("This is a message for: %s (%d)", "user", 42)
    if (!isTraceEnabled) {
      warn("Tracing is not enabled!")
    }
  }

  // Make sure that we can still cause error.
  def testError {
    intercept[RuntimeException] {
      error("Message")
      println("Should not be reached")
    }
  }

  def testWrapper {
    var phase = 1
    def wrapper(thunk: => Unit) {
      expect(1)(phase)
      println("Pre wrap")
      thunk
      println("Post wrap")
      phase += 1
    }
    Logger.setWrapper(wrapper _)
    info("Wrapped message")
    expect(2)(phase)
    Logger.clearWrapper()
    info("Unwrapped message")
    expect(2)(phase)
  }
}
