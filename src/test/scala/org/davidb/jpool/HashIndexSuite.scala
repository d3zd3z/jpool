/**********************************************************************/
//
// The storage pool stores blobs in a series of files (the pool).  The
// HashIndex keeps track of where in this pool each hash is located.
//
// The HashIndex is stored as a series of memory-mapped files, sorted
// by Hash, containing the hash, and some "extra" information.  The
// number of files used is a tradeoff between the extra search time
// (all files have to be searched, as well as the kernel having to
// maintain extra mappings), and the time needed to rewrite the last
// file.
//
// At any given time, the HashIndex maintains 0 or more mappings of
// files as well as an internal update of hashes recently written that
// haven't been flushed out.  See the implementation details below on
// the Hanoi-style combination to give a decent tradeoff between
// individual files, and merges that need to be performed.
//
// The index is robust against abnormal termination of the program, as
// long as the underlying filesystem implements reasonable atomic
// semantics on rename.

package org.davidb.jpool

import collection.{mutable, immutable}
import java.nio.ByteBuffer

/**********************************************************************/
// Unit test.
/**********************************************************************/

import org.scalatest.{Suite, BeforeAndAfter}

class IntHashIndex(val basePath: String, val prefix: String) extends {
  protected val encoder = new FixedEncodable[Int] {
    def EBytes = 4
    def encode(obj: Int, buf: ByteBuffer) {
      buf.putInt(obj)
    }
    def decode(buf: ByteBuffer): Int = {
      buf.getInt()
    }
  }
} with HashIndex[Int] {
  // Make this small enough that the test completes in a reasonable
  // time.  Comment this out for a very long test, that probably
  // doesn't really exercise anything different, other than that
  // larger TreeMaps work.
  override def ramMax = 103
}

class HashIndexSuite extends Suite with TempDirTest {
  var hashIndex: IntHashIndex = null

  override def beforeEach() {
    super.beforeEach()
    hashIndex = new IntHashIndex(tmpDir.path.getPath, "index-")
  }

  override def afterEach() {
    super.afterEach()
    hashIndex = null
  }

  def testInvalidDir {
    intercept[IllegalArgumentException] {
      new IntHashIndex("/xxxxx/yyyyy/zzzzz", "index-")
    }
  }

  def testPrefixSlash {
    intercept[IllegalArgumentException] {
      new IntHashIndex("/tmp", "in/dex-")
    }
  }

  def reload() {
    hashIndex.close()
    hashIndex = new IntHashIndex(tmpDir.path.getPath, "index-")
  }

  // Test the simple case of writing nodes consecutively without any
  // explicit flushes, except at the end.
  def testRam {
    val limit = hashIndex.ramMax * 32 - 1
    addHashes(0 to limit)
    validateHashes(0 to limit)
    hashIndex.flush()
    validateHashes(0 to limit)
    reload()
    validateHashes(0 to limit)
  }

  // Test reopens across numerous boundaries.
  def testReopen {
    val limit = hashIndex.ramMax
    val bounds = new mutable.ArrayBuffer[Int]

    for (i <- 0 until limit * 16 by limit) {
      if (i > 0)
        bounds += i - 1
      bounds += i
      bounds += i + 1
    }

    var lastBound = 0
    for (bound <- bounds) {
      // println("Testing bound: " + bound)
      addHashes(lastBound until bound)
      validateHashes(0 until bound)
      reload()
      if (false && bound == limit * 15 + 1) {
        Runtime.getRuntime.exec(Array(
          "/bin/sh", "-c",
          String.format(
            "(/bin/ls -l %s;\n" +
            " for i in %s/*; do\n" +
            "   echo \"Dump of $i\";\n" +
            "   hexdump -C $i;\n" +
            " done) > /tmp/debug",
            tmpDir.path.getPath, tmpDir.path.getPath))).waitFor()
      }
      validateHashes(0 until bound)
      lastBound = bound
    }
  }

  private def addHashes(r: Range) {
    r.foreach { i => hashIndex.put(bufHash(i), i) }
  }

  private def validateHashes(r: Range) {
    r.foreach { i =>
      // Verify that this hash is present, and "nearby" ones are not.
      val h = bufHash(i)
      assert(hashIndex.get(h) === Some(i))

      val bytes = h.getBytes
      bytes(bytes.length - 1) = (bytes(bytes.length - 1) ^ 1).toByte
      val h2 = Hash.raw(bytes)
      assert(hashIndex.get(h2) === None)
    }
  }

  private def bufHash(index: Int): Hash = {
    Hash("blob", index.toString)
  }
}
