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
}
