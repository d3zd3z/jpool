// Test individual file index.

package org.davidb.jpool
package pool

import org.scalatest.Suite
import scala.collection.mutable

class FileIndexSuite extends Suite with TempDirTest {

  def testEmpty {
    printf("The temp dir is %s\n", tmpDir.path)
  }

  // Test just writing a bunch of nodes to the index.
  def testPlain {
    val indexFile = new java.io.File(tmpDir.path, "indexplain")
    val items = new Iterable[(Hash, FileIndex.Elt)] {
      def iterator = new Iterator[(Hash, FileIndex.Elt)] {
        private var count = 0
        def hasNext = count < 10000
        def next() = {
          count += 1
          val kind = makeKind(count)
          val h = Hash(kind, count.toString)
          (h, FileIndex.Elt(count, kind))
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
    for ((h, FileIndex.Elt(pos, kind)) <- items) {
      index.get(h) match {
        case None => fail()
        case Some(FileIndex.Elt(p, k)) =>
          assert(p === pos)
          assert(k === kind)
      }
      val h2 = HashSuite.mutate(h)
      assert(index.get(h2) === None)
    }
  }

  // Test creation, testing, update of index.
  def testUpdate {
    val fakePool = new FakePoolFile(new java.io.File(tmpDir.path, "dataUpdate"))
    val ind1 = new FileIndex(fakePool)
    addItems(ind1, fakePool, 100)
    checkIndex(ind1, fakePool)
    addItems(ind1, fakePool, 100)
    checkIndex(ind1, fakePool)

    val ind2 = new FileIndex(fakePool)
    checkIndex(ind2, fakePool)
    addItems(ind2, fakePool, 100)
    checkIndex(ind2, fakePool)
    addItems(ind2, fakePool, 100)
    checkIndex(ind2, fakePool)
  }

  // Test that the given index matches the data in it's pool.
  def checkIndex(index: FileIndex, pf: FakePoolFile) {
    for (i <- 0 until pf.size) {
      val (hash, kind, size) = pf.readInfo(i)
      expectResult(Some(FileIndex.Elt(i, kind))) {
        index.get(hash)
      }
    }
  }

  def addItems(index: FileIndex, pf: FakePoolFile, count: Int) {
    for (i <- 0 until count) {
      val pos = pf.size
      val chunk = makeChunk(pos)
      assert(pf.append(chunk) === pos)
      index += ((chunk.hash, FileIndex.Elt(pos, chunk.kind)))
    }
    index.flush()
  }

  // A fake storage pool that stores the information needed to test
  // indexing (but only in RAM).
  class FakePoolFile(path: java.io.File) extends PoolFileBase(path) {
    def read(pos: Int) = sys.error("Should not be called")
    def readUnchecked(pos: Int) = sys.error("Should not be called")

    private var buf = new mutable.ArrayBuffer[(Hash, String, Int)]()

    def readInfo(pos: Int): (Hash, String, Int) = {
      curPos = pos + 1
      buf(pos)
    }

    def append(chunk: Chunk): Int = {
      val pos = buf.length
      buf += ((chunk.hash, chunk.kind, chunk.dataLength))
      pos
    }

    var curPos = 0

    override def size: Int = buf.length
    override def position: Int = curPos
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

  def makeChunk(pos: Int): Chunk = {
    Chunk.make(makeKind(pos), StringMaker.generate(pos, 64))
  }

}
