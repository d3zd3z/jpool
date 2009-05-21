//////////////////////////////////////////////////////////////////////
// Test of native interface.

package org.davidb.jpool

import java.io.IOException

object Linux {
  @native def setup

  @native
  def message: String

  @native
  def readDir(name: String): List[(String, Long)]

  // These wrappers keep the JNI code less dependent on the
  // implementation details of the Scala runtime.
  private def makePair(name: String, inum: Long) = (name, inum)

  private def readdirError(path: String, errno: Int): Nothing =
    throw new IOException("Error reading directory: '%s' (%d)" format (path, errno))

  System.loadLibrary("linux")
  setup
}
