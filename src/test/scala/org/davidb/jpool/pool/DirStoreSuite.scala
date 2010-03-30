//////////////////////////////////////////////////////////////////////
// Testing of DirStore.

package org.davidb.jpool.pool

import org.scalatest.Suite

class DirStoreSuite extends Suite with ProgressPoolTest {

  def testDirStore {
    val store = new DirStore(pool, 256*1024)
    def nums = Stream.concat(Stream.range(1, 256), Stream.range(256, 32768, 256), Stream(32767))
    for (i <- nums) {
      val name = StringMaker.generate(i, i)
      val hash = Hash("blob", name)
      store.append(name, hash)
    }
    val dhash = store.finish()

    var posns = nums
    for ((name, hash) <- DirStore.walk(pool, dhash)) {
      val pos = posns.head
      posns = posns.tail
      assert(name.length === pos)
      assert(name === StringMaker.generate(pos, pos))
      assert(hash === Hash("blob", name))
    }
  }

}
