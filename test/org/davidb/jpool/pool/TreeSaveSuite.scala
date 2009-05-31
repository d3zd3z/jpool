//////////////////////////////////////////////////////////////////////
// Testing tree saving.

package org.davidb.jpool.pool

import scala.collection.mutable
import org.scalatest.{Suite, TestFailedException}
import org.davidb.logging.Logger
import java.io.File
import java.util.Properties

class TreeSaveSuite extends Suite with PoolTest with Logger {

  // Restore not implemented, so the best we can do is save something
  // and generate a listing and make sure not fatal errors happen.
  // TODO: This is way too verbose, so not really want we're going to
  // want to be doing.  Better to use a generalized tree walker.
  def testSave {
    // These also test graceful handling of unreadable files.
    info("warnings are expected for this test")
    var h1 = new TreeSave(pool, "/bin").store(someProps("bin"))
    var h1b = new TreeSave(pool, "/bin").store(someProps("bin"))
    var h2 = new TreeSave(pool, "/dev").store(someProps("dev"))
    printf("%s%n%s%n", h1, h2)

    check(h1, "./ls")
    check(h2, "./null")

    // Test a restore.
    info("Testing restore: bin")
    val r1 = restore("restore-bin", h1)
    Proc.run("cmp", "/bin/ls", "%s/ls" format r1)

    info("Testing restore: dev")
    val r2 = restore("restore-dev", h2)

    // Verify that some restore operations fail.
    intercept[RuntimeException] {
      new TreeRestore(pool).restore(h1, r1)
    }

    if (Linux.isRoot) {
      compareBackup(h1, r1)
      compareBackup(h2, r2)
    } else {
      warn("Skipping permission restore test, since user is not root")
    }
  }

  // Try restoring to a directory based on the given path.  Returns
  // the File describing the path where the restore took place.
  private def restore(prefix: String, hash: Hash): String = {
    val dir = new File(tmpDir.path, prefix)
    val dirName = dir.getPath
    assert(dir.mkdir())
    new TreeRestore(pool).restore(hash, dirName)
    dirName
  }

  // Compare backups by backing the restored directory up again, then
  // walking both snapshots and comparing them.  This can miss bugs in
  // the snapshot part of the backup, but should catch any problems
  // restoring the data.
  // If there are device nodes, and the likes, this test probably has
  // to be run as root.  I have had trouble running these tests under
  // fakeroot.
  private def compareBackup(first: Hash, secondDir: String) {
    val second = new TreeSave(pool, secondDir).store(someProps("bin"))

    val firstWalk = new TreeWalk(pool).walk(first).elements
    val secondWalk = new TreeWalk(pool).walk(second).elements

    while (firstWalk.hasNext && secondWalk.hasNext) {
      val f = firstWalk.next
      val s = secondWalk.next

      // Explode out the comparisons to avoid testing the things we
      // can't control.
      try {
        // The states are different objects, since they are within the
        // class, so compare their string representations.
        if (f.state.toString != s.state.toString)
          fail("state mismatch testing backup %s != %s" format (f.state, s.state))
        if (f.atts.kind != s.atts.kind)
          fail("kind mismatch testing backup %s != %s" format (f.atts.kind, s.atts.kind))
        if (f.path != s.path)
          fail("path mismatch testing backup %s != %s" format (f.path, s.path))
        if (f.atts.name != s.atts.name)
          fail("name mismatch testing backup %s != %s" format (f.atts.name, s.atts.name))

        val fk = f.atts.contents - "ctime"
        val sk = f.atts.contents - "ctime"

        if (fk != sk) {
          fail("attributes differ")
        }
      } catch {
        case e: TestFailedException =>
          logError("first: %s", f)
          logError("second: %s", f)
          Thread.sleep(30000)
          throw e
      }
    }

    if (firstWalk.hasNext)
      error("Extra node in first backup")
    if (secondWalk.hasNext)
      error("Extra node in second backup")
  }

  // Iterate from a given hash, making sure that the tree is
  // properly formed (each enter and leave matches up, and all nodes
  // once entered start with the current node).
  // The 'expected' string must be somewhere in the tree as well.
  private def check(hash: Hash, expected: String) {
    var sawExpected = false
    val dirs = new mutable.Stack[String]
    val walker = new TreeWalk(pool)
    for (node <- walker.walk(hash)) {
      node.state match {
        case walker.Enter =>
          dirs.push(node.path)
        case walker.Leave =>
          val oldDir = dirs.pop()
          assert(node.path === oldDir)
        case walker.Node =>
          assert(node.path startsWith dirs.top)
          if (node.path == expected)
            sawExpected = true
      }
    }
    assert(sawExpected)
  }

  // Make up some properties.
  private def someProps(name: String): Properties = {
    val props = new Properties
    props.setProperty("name", name)
    props
  }
}
