// Test the tree builder.

package org.davidb.jpool.pool

import org.davidb.jpool._

import java.net.URI
import org.scalatest.{Suite, BeforeAndAfter}

class TreeBuilderSuite extends Suite with ProgressPoolTest {

  def testSingle = {
    val hash = rangeCheck(1 to 1)
    checkKind(hash, "blob")
  }
  def testTwo = rangeCheck(1 to 2)
  def testTen = rangeCheck(1 to 10)
  def testEleven = rangeCheck(1 to 11)
  def testMany = rangeCheck(1 to 1111)

  private def rangeCheck(nums: Range): Hash = {
    val tb = TreeBuilder.makeBuilder("tmp", pool, 10*Hash.HashLength)
    add(tb, nums)
    val hash = tb.finish()
    check(hash, nums)
    hash
  }

  private def add(tb: TreeBuilder, nums: Range) {
    for (i <- nums) {
      tb.add(makeChunk(i, 1))
    }
  }

  private def check(hash: Hash, nums: Range) {
    val walker = TreeBuilder.walk("tmp", pool, hash)
    val expected = nums map (makeChunk(_, 1))
    assert ((walker map (_.hash)) sameElements (expected map (_.hash)))
  }

  private def checkKind(hash: Hash, kind: String) {
    val chunk = pool(hash)
    assert(chunk.kind === kind)
  }
}
