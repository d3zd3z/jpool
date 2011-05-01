/*
 * Hash management.
 *
 * Our hashes are very non-specialized.
 */

package org.davidb.jpool

/*
 * Test suite
 */
import org.scalatest.Suite
import scala.collection.immutable
import scala.collection.mutable

class HashSuite extends Suite {
  def testBasics {
    assert(Hash("blob", "").toString === "0fd0bcfb44f83e7d5ac7a8922578276b9af48746")
    assert(Hash("blob", "Hello").toString === "51712bd6234e069e7e0b012a7c19e6e12a25d327")
  }

  def testStrings {
    intercept[IllegalArgumentException] {
      Hash.ofString("")
    }
    intercept[IllegalArgumentException] {
      Hash.ofString("00000000000000000000000000000000000000000")
    }
    assert(Hash.ofString("0123456789abcdef0123456789abcdef01234567") ===
      Hash.ofString("0123456789abcdef0123456789abcdef01234567"))
    assert(Hash.ofString("0123456789abcdef0123456789abcdef01234567") <
      Hash.ofString("0123456789abcdef0123456789abcdef01234568"))
    assert(Hash.ofString("0123456789abcdef0123456789abcdef01234567") >
      Hash.ofString("0123456789abcdef0123456788abcdef01234567"))
  }

  def testMaps {
    // Immutable/Sorted
    val places = 0 until 1000
    var m1: immutable.SortedMap[Hash, Int] = immutable.TreeMap.empty
    for (i <- places) {
      m1 += (hashOfNum(i) -> i)
    }
    checkMap(m1, places)

    // Immutable/Hashed.
    var m2: Map[Hash, Int] = Map()
    for (i <- places) {
      m2 += (hashOfNum(i) -> i)
    }
    checkMap(m2, places)

    // Mutable/Sorted
    val m3 = mutable.Map.empty[Hash, Int]
    for (i <- places) {
      m3 += (hashOfNum(i) -> i)
    }
    checkMap(m3, places)
  }

  private def checkMap(map: collection.Map[Hash, Int], r: Range) {
    for (i <- r) {
      val h1 = hashOfNum(i)
      map.get(h1) match {
        case None => fail("Hash not present in map")
        case Some(num) => assert(i === num)
      }

      // Modify the hash and make sure this one isn't present.
      val h2 = HashSuite.mutate(h1)
      assert(!map.contains(h2))
    }
  }

  private def hashOfNum(num: Int): Hash = {
    Hash("blob", String.format("payload %d", int2Integer(num)))
  }
}

object HashSuite {
  def mutate(h: Hash): Hash = {
    val bytes = h.getBytes
    bytes(bytes.length - 1) = (bytes(bytes.length - 1) ^ 1).toByte
    Hash.raw(bytes)
  }
}
