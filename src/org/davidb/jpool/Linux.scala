//////////////////////////////////////////////////////////////////////
// Test of native interface.

package org.davidb.jpool

object Linux {
  @native
  def message: String

  System.loadLibrary("linux")
}
