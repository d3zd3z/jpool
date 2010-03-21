// A trait to make for easier tests of pool-based things.

package org.davidb.jpool.pool

import java.io.File
import java.net.URI
import org.scalatest.{Suite, BeforeAndAfterEach}

trait PoolTest extends Suite with BeforeAndAfterEach with TempDirTest {

  class PerformedRecovery extends AssertionError("Recovery performed")
  class NoRecoveryPool(prefix: File) extends FilePool(prefix) {
    override def recoveryNotify { throw new PerformedRecovery }
  }

  var pool: ChunkStore = null
  override def beforeEach() {
    super.beforeEach()
    pool = PoolFactory.getStoreInstance(new URI("jpool:file://%s" format tmpDir.path.getPath))
  }
  override def afterEach() {
    pool.close()
    super.afterEach()
  }
  protected def reopen {
    pool.close()
    pool = new NoRecoveryPool(tmpDir.path)
  }

  protected def makeChunk(index: Int, size: Int) = Chunk.make("blob", StringMaker.generate(index, size))
}
