/*
 * Mapped Index Files.
 *
 * The workhorse of the HashIndex class.
 *
 * The mapped index maintains a fixed mapping between Hashes and
 * some type that can be encoded in a fixed binary representation.
 */

package org.davidb.jpool

import java.io.File
import java.nio.ByteBuffer

import collection.{mutable, immutable}

// For testing, use an example store that encodes integers into the
// result.
class PairIndex(val path: File) extends MappedIndexFile[Int] {
  protected val encoder = new FixedEncodable[Int] {
    val EBytes = 8
    def encode(obj: Int, buf: ByteBuffer) {
      buf.putInt(obj)
    }
    def decode(buf: ByteBuffer): Int = {
      buf.getInt()
    }
  }
}

import org.scalatest.Suite

// Testing.
class MappedIndexFileSuite extends Suite {
  val a = 1000
  val b = 2000

  def testMerges {
    TempDir.withTempDir { name =>
      var m = immutable.TreeMap[Hash,Int]()
      for (i <- 0 until a) {
        m += (Hash("blob", i.toString) -> i)
      }
      testIterator(0 until a, m)

      val store = new PairIndex(new File(name, "index"))
      store.properties.setProperty("last", "1000")
      store.write(m)

      testIterator(0 until a, store)

      for (i <- 0 until a) {
        val h1 = Hash("blob", i.toString)

        (m.get(h1)) match {
          case None => fail("Expecting hit")
          case Some(e) => assert(e === i)
        }

        val bytes = h1.getBytes
        bytes(bytes.length - 1) = (bytes(bytes.length - 1) ^ 1).toByte
        val h2 = Hash.raw(bytes)
        assert((m.get(h2)) === None)
      }

      m = immutable.TreeMap[Hash,Int]()
      for (i <- a until b) {
        m += (Hash("blob", i.toString) -> i)
      }
      testIterator(a until b, m)
      testIterator(0 until b, (new MergingMapIterator[Hash, Int]).addIterator(store).addIterator(m))

      // Write out the new data.
      store.write((new MergingMapIterator[Hash, Int]).addIterator(store).addIterator(m))
      testIterator(0 until b, store)
    }
  }

  def testIterator(range: Range, iter: Iterable[(Hash,Int)]) {
    val keys = immutable.TreeSet[(Hash,Int)]() ++ range.map(i =>
      (Hash("blob", i.toString), i))
    // println("keys: " + keys.toList)
    // println("iter: " + iter.toList)

    assert(keys.toList sameElements iter.toList)
  }
}
