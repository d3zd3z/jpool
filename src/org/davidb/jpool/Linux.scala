//////////////////////////////////////////////////////////////////////
// Test of native interface.

package org.davidb.jpool

import java.io.IOException
import scala.collection.immutable

object Linux {
  @native def setup

  @native
  def message: String

  @native
  def readDir(name: String): List[(String, Long)]

  @native
  def lstat(name: String): Map[String, String]

  // These wrappers keep the JNI code less dependent on the
  // implementation details of the Scala runtime.
  private def makePair(name: String, inum: Long) = (name, inum)

  // For the mapping, these functions provide the zero and mplus
  // operations.
  private def infoZero = immutable.Map.empty
  private def infoPlus(prior: immutable.Map[String, String], key: String, value: String):
    immutable.Map[String, String] =
  {
    prior + (key -> value)
  }

  private def readdirError(path: String, errno: Int): Nothing =
    throw new IOException("Error reading directory: '%s' (%d)" format (path, errno))
  private def lstatError(path: String, errno: Int): Nothing =
    throw new IOException("Error statting node: '%s' (%d)" format (path, errno))

  System.loadLibrary("linux")
  setup
}
