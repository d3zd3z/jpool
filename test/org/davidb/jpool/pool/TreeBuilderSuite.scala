// Test the tree builder.

package org.davidb.jpool.pool

import java.net.URI
import org.scalatest.{Suite, BeforeAndAfter}

class TreeBuilderSuite extends Suite with PoolTest {

  def testSingle = rangeCheck(1 to 1)
  def testTwo = rangeCheck(1 to 2)
  def testTen = rangeCheck(1 to 10)
  def testEleven = rangeCheck(1 to 11)
  def testMany = rangeCheck(1 to 1111)

  private def rangeCheck(nums: Range) {
    val tb = TreeBuilder.makeBuilder("tmp", pool, 10*Hash.HashLength)
    add(tb, nums)
    check(tb.finish(), nums)
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
}
