/**********************************************************************/
// Testing tree saving.

package org.davidb.jpool
package pool

import scala.collection.mutable
import org.scalatest.{Suite, TestFailedException}
import org.davidb.logging.Loggable
import java.io.{File, FileWriter}
import java.util.Properties

class TreeSaveSuite extends Suite with ProgressPoolTest with Loggable {

  // TODO: This is way too verbose, so not really want we're going to
  // want to be doing.  Better to use a generalized tree walker.
  def testSave {
    // These also test graceful handling of unreadable files.
    logger.info("warnings are expected for this test")
    var h1 = new TreeSave(pool, "/bin", meter).store(someProps("bin"))
    var h1b = new TreeSave(pool, "/bin", meter).store(someProps("bin"))
    var h2 = new TreeSave(pool, "/dev", meter).store(someProps("dev"))
    printf("%s%n%s%n", h1, h2)

    check(h1, "./sh")
    check(h2, "./null")

    // Test a restore.
    logger.info("Testing restore: bin")
    val r1 = restore("restore-bin", h1)
    Proc.run("cmp", "/bin/sh", "%s/sh" format r1)

    logger.info("Testing restore: dev")
    val r2 = restore("restore-dev", h2)

    // Verify that some restore operations fail.
    intercept[RuntimeException] {
      new TreeRestore(pool, meter).restore(h1, r1)
    }

    if (Linux.isRoot) {
      compareBackup(h1, r1)
      compareBackup(h2, r2)
    } else {
      logger.warn("Skipping permission restore test, since user is not root")
    }
  }

  // Make sure that restore can restore hardlinks properly.
  def testHardLinks {
    val d1 = new File(tmpDir.path, "orig")
    assert(d1.mkdir())

    val n1 = "%s/file1" format d1.getPath
    val fw = new FileWriter(n1)
    fw.write(StringMaker.generate(1, 32*1024))
    fw.close()
    Linux.link(n1, "%s/file2" format d1.getPath)

    val h1 = new TreeSave(pool, d1.getPath, meter).store(someProps("orig"))

    // Restore it.
    val r1 = restore("rest", h1)
    val stat1 = Linux.lstat("%s/file1" format r1)
    val stat2 = Linux.lstat("%s/file2" format r1)
    assert(stat1("nlink") === "2")
    assert(stat1 === stat2)
  }

  // Try restoring to a directory based on the given path.  Returns
  // the File describing the path where the restore took place.
  private def restore(prefix: String, hash: Hash): String = {
    val dir = new File(tmpDir.path, prefix)
    val dirName = dir.getPath
    assert(dir.mkdir())
    new TreeRestore(pool, meter).restore(hash, dirName)
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
    val second = new TreeSave(pool, secondDir, meter).store(someProps("bin"))

    val firstWalk = new TreeWalk(pool).walk(first)
    val secondWalk = new TreeWalk(pool).walk(second)

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

        val fk = f.atts.contents - "ctime"
        val sk = f.atts.contents - "ctime"

        if (fk != sk) {
          fail("attributes differ")
        }
      } catch {
        case e: TestFailedException =>
          logger.error("first: %s" format f)
          logger.error("second: %s" format f)
          Thread.sleep(30000)
          throw e
      }
    }

    if (firstWalk.hasNext)
      sys.error("Extra node in first backup")
    if (secondWalk.hasNext)
      sys.error("Extra node in second backup")
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
