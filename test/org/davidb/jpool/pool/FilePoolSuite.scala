// Test file pools.

package org.davidb.jpool.pool

import java.io.File
import java.net.URI
import org.scalatest.{Suite, BeforeAndAfter}

import scala.collection.mutable.ArrayBuffer

class FilePoolSuite extends Suite with PoolTest {

  def testSimplePool {
    val c1 = makeChunk(1, 1024)
    val c2 = makeChunk(2, 1024)
    intercept[IllegalArgumentException] {
      pool += (c1.hash -> c2)
    }
    pool += (c1.hash -> c1)
    pool += (c2.hash -> c2)
    val c1b = pool(c1.hash)
    val c2b = pool(c2.hash)
    assert(c1.hash === c1b.hash)
    assert(c2.hash === c2b.hash)
  }

  def testHasRecovery {
    val chunk = makeChunk(1, 1024)
    pool += (chunk.hash -> chunk)
    pool.close()
    assert(new File(new File(tmpDir.path, "metadata"), "data-index-0001").delete)
    intercept[PerformedRecovery] {
      pool = new NoRecoveryPool(tmpDir.path)
    }
  }

  def testReopen {
    val hashes = new ArrayBuffer[Hash]
    for (i <- 0 until 100) {
      val chunk = makeChunk(i, 512)
      pool += (chunk.hash -> chunk)
      hashes += chunk.hash
    }

    reopen
    for (hash <- hashes) {
      val chunk = pool(hash)
      assert (hash === chunk.hash)
    }
  }

  def testReopenNoWrite {
    reopen
  }

}
