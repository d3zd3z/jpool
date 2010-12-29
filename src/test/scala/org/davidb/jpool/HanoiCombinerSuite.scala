/**********************************************************************/
// UNIT test
/**********************************************************************/

package org.davidb.jpool

import org.scalatest.Suite

class HanoiCombinerSuite extends Suite {
  // Test for coherence between the two HanoiCombiner calls.
  def testCoherence {
    val vals = new collection.mutable.Stack[Int]

    for (i <- 0 to 4097) {
      val cc = HanoiCombiner.combineCount(i)
      assert(cc <= vals.size)

      for (j <- 0 until cc) {
        vals.pop()
      }

      vals.push(i)

      assert(vals.reverse sameElements HanoiCombiner.presentSet(i))
    }
  }
}
