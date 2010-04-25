//////////////////////////////////////////////////////////////////////
// Testing of DirStore.

package org.davidb.jpool.pool

import org.davidb.jpool._
import org.scalatest.Suite

class DirStoreSuite extends Suite with ProgressPoolTest {

  def testDirStore {
    val store = new DirStore(pool, 256*1024)
    def nums = Iterator.range(1, 256) ++ Iterator.range(256, 32768, 256) ++ Iterator.single(32767)
    for (i <- nums) {
      val name = StringMaker.generate(i, i)
      val hash = Hash("blob", name)
      store.append(name, hash)
    }
    val dhash = store.finish()

    var posns = nums
    for ((name, hash) <- DirStore.walk(pool, dhash)) {
      val pos = posns.next
      assert(name.length === pos)
      assert(name === StringMaker.generate(pos, pos))
      assert(hash === Hash("blob", name))
    }
  }

}
