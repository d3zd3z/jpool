//////////////////////////////////////////////////////////////////////
// Test native JNI.

package org.davidb.jpool

import org.scalatest.Suite

class LinuxSuite extends Suite {
  def testNative {
    assert(Linux.message == "Hello world")
  }
}
