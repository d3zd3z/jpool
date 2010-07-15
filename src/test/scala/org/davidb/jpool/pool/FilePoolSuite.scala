// Test file pools.

package org.davidb.jpool
package pool

import scala.annotation.tailrec
import java.io.File
import java.net.URI
import org.scalatest.{Suite, BeforeAndAfter}

import scala.collection.mutable.ArrayBuffer

class FilePoolSuite extends Suite with ProgressPoolTest {

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

  def testStickyLimit {
    val a = pool.limit
    assert(a === 640*1024*1024)  // Might not be set in stone.
    pool.limit = 512*1024
    assert(pool.limit == 512*1024)
    reopen
    assert(pool.limit == 512*1024)
  }

  def testLimit {
    val hashes = new ArrayBuffer[Hash]
    pool.limit = 32*1024
    for (i <- 0 until 300) {
      val chunk = makeChunk(i, 1024)
      pool += (chunk.hash -> chunk)
      hashes += chunk.hash
    }

    reopen
    for (hash <- hashes) {
      val chunk = pool(hash)
      assert(hash === chunk.hash)
    }

    // Assure that multiple files were opened.
    var limit = -1
    @tailrec def loop(index: Int) {
      val f = new File(tmpDir.path, "pool-data-%04d.data" format index)
      if (f.isFile) {
        // printf("File: %d len = %x%n", index, f.length)
        assert(f.length < 32L*1024)
        limit = index
        loop(index + 1)
      }
    }
    loop(0)
    assert(limit > 0)
  }
}
