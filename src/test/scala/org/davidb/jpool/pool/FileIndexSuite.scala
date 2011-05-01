// Test individual file index.

package org.davidb.jpool
package pool

import org.scalatest.Suite

class FileIndexSuite extends Suite with TempDirTest {

  def testEmpty {
    printf("The temp dir is %s\n", tmpDir.path)
  }

  // Test just writing a bunch of nodes to the index.
  def testPlain {
    val indexFile = new java.io.File(tmpDir.path, "indexplain")
    val items = new Iterable[(Hash, (Int, String))] {
      def iterator = new Iterator[(Hash, (Int, String))] {
        private var count = 0
        def hasNext = count < 10000
        def next() = {
          count += 1
          val kind = makeKind(count)
          val h = Hash(kind, count.toString)
          (h, (count, kind))
        }
      }
    }

    FileIndexFile.writeIndex(indexFile, 12345, items)

    // Make sure that we get an exception if the size mismatches.
    intercept[FileIndexFile.PoolSizeMismatch] {
      new FileIndexFile(indexFile, 12344)
    }
    intercept[FileIndexFile.PoolSizeMismatch] {
      new FileIndexFile(indexFile, 12346)
    }

    val index = new FileIndexFile(indexFile, 12345)
    for ((h, (pos, kind)) <- items) {
      index.get(h) match {
        case None => fail()
        case Some((p, k)) =>
          assert(p === pos)
          assert(k === kind)
      }
      val h2 = HashSuite.mutate(h)
      assert(index.get(h2) === None)
    }
  }

  // Simple generation of some variation of kinds.
  def makeKind(count: Int): String = {
    val kinds = kindList
    // Joy joy, signed numbers are good enough for Java, they're good
    // enough for you.
    val index = (((count * 1103515245 + 12345) & 0x7FFFFFFF) % kinds.length)
    kinds(index)
  }

  private val kindList = List("BLOB", "DIR ", "DIR0", "DIR1", "IND0", "IND1", "IND2", "BACK")

}
