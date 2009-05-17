// Test the tree builder.

package org.davidb.jpool

import jpool.pool.{ChunkStore, PoolFactory, PoolTest}

import java.net.URI
import org.scalatest.{Suite, BeforeAndAfter}

class TreeBuilderSuite extends Suite with PoolTest {

  def testSingle {
    val tb = TreeBuilder.makeBuilder("tmp", pool, 10*20)
    tb.add(makeChunk(1, 1))
    val rootHash = tb.finish()
    val walker = TreeBuilder.walk("tmp", pool, rootHash)
    val expected = (1 to 1) map (makeChunk(_, 1))
    assert ((walker map (_.hash)) sameElements (expected map (_.hash)))
  }

}
