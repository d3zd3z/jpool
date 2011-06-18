// A trait to make for easier tests of pool-based things.

package org.davidb.jpool
package pool

import java.io.File
import java.net.URI
import org.scalatest.{Suite, BeforeAndAfterEach}

// The pool tests aren't concurrent, so lock the tests that can't run
// concurrently.
object PoolTest {
  val lock = new scala.concurrent.Lock
}

trait PoolTest extends Suite with BeforeAndAfterEach with TempDirTest {

  class PerformedRecovery extends AssertionError("Recovery performed")
  class NoRecoveryPool(prefix: File) extends FilePool(prefix) {
    override def recoveryNotify { throw new PerformedRecovery }
  }

  var pool: ChunkStore = null
  override def beforeEach() {
    super.beforeEach()
    PoolTest.lock.acquire()
    pool = PoolFactory.getStoreInstance(new URI("jpool:file://%s" format tmpDir.path.getPath))
  }
  override def afterEach() {
    pool.close()
    PoolTest.lock.release()
    super.afterEach()
  }
  protected def reopen {
    pool.close()
    pool = new NoRecoveryPool(tmpDir.path)
  }

  protected def makeChunk(index: Int, size: Int) = Chunk.make("blob", StringMaker.generate(index, size))
}

// A trait to make it easier to test things that write progress but
// aren't driven from the top level.
trait ProgressPoolTest extends PoolTest {
  var meter: BackupProgressMeter = null
  override def beforeEach() {
    super.beforeEach()
    meter = new BackupProgressMeter
    ProgressMeter.register(meter)
    pool.setProgress(meter)
  }
  override def afterEach() {
    pool.setProgress(NullProgress)
    ProgressMeter.unregister(meter)
    meter = null
    super.afterEach()
  }
}
