/*
 * Temporary directory management.
 */

package org.davidb.jpool

import java.io.File
import java.util.UUID

object TempDir {
  def withTempDir[A](thunk: File => A): A = {
    val td = new TempDir
    try {
      thunk(td.path)
    } finally {
      td.close()
    }
  }
}

class TempDir {
  protected val prefix = "/jpool-"
  val path = make(4)

  private val debugging = System.getProperty("org.davidb.jpool.debug") ne null

  private var closed = false
  def close() {
    if (!closed) {
      if (debugging) {
        Runtime.getRuntime.exec(Array(
          "/bin/sh", "-c",
          String.format(
            "(/bin/ls -l %s;\n" +
            " for i in %s/*; do\n" +
            "   echo \"Dump of $i\";\n" +
            "   hexdump -C $i;\n" +
            " done) > /tmp/debug",
            path.getPath, path.getPath))).waitFor()
      }
      recursiveRemove(path)
      closed = true
    }
  }

  private def recursiveRemove(path: File) {
    if (path.isDirectory)
      path.listFiles.foreach(recursiveRemove _)
    path.delete()
  }

  /* Construct a tempdir, returning it's name. */
  private def make(limit: Int): File = {
    if (limit == 0)
      sys.error("Unable to create temporary directory")
    val base = System.getProperty("java.io.tmpdir", "/tmp")
    val path = base + prefix + UUID.randomUUID.toString.substring(0, 8)
    val tdir = new File(path)
    if (tdir.mkdir())
      tdir
    else
      make(limit - 1)
  }
}

// Mix in with a testsuite to provide a 'tmpDir' field of type
// TempDir, that will be setup and torn down before and after each
// test.
trait TempDirTest extends org.scalatest.BeforeAndAfterEach {
  this: org.scalatest.Suite =>

  var tmpDir: TempDir = null

  override def beforeEach() {
    tmpDir = new TempDir
    super.beforeEach()
  }
  override def afterEach() {
    super.afterEach()
    tmpDir.close()
    tmpDir = null
  }
}
