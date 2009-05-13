// Test file pools.

package org.davidb.jpool.pool

import java.net.URI
import org.scalatest.{Suite, BeforeAndAfter}

class FilePoolSuite extends Suite with BeforeAndAfter with TempDirTest {

  var pool: ChunkStore = null
  override def beforeEach() {
    super.beforeEach()
    pool = PoolFactory.getStoreInstance(new URI("jpool:file://%s" format tmpDir.path.getPath))
  }
  override def afterEach() {
    pool.close()
    super.afterEach()
  }

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

  private def makeChunk(index: Int, size: Int) = Chunk.make("blob", StringMaker.generate(index, size))
}
