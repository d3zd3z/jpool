//////////////////////////////////////////////////////////////////////
// Testing tree saving.

package org.davidb.jpool.pool

import scala.collection.mutable
import org.scalatest.Suite
import org.davidb.logging.Logger
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
    var h2 = new TreeSave(pool, "/dev").store(someProps("dev"))
    printf("%s%n%s%n", h1, h2)

    check(h1, "./bin/ls")
    check(h2, "./dev/null")
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
      }
    }
  }

  // Make up some properties.
  private def someProps(name: String): Properties = {
    val props = new Properties
    props.setProperty("name", name)
    props
  }
}
