/*
 * Chunk management.
 */

package org.davidb.jpool

/**********************************************************************
 * Unit test
 **********************************************************************/

import org.scalatest.Suite

import java.nio.ByteBuffer
import TempDir.withTempDir
import java.io.{File, RandomAccessFile}

import scala.language.implicitConversions

class ChunkSuite extends Suite {
  implicit def stringOfBuffer(buf: ByteBuffer): String =
    new String(buf.array, buf.arrayOffset + buf.position, buf.remaining)

  def testCreate {
    for (i <- sizes) {
      val data = StringMaker.generate(i, i)
      val chunk = Chunk.make("blob", data)
      assert(data == (chunk.data: String))
      assert(Hash("blob", data) == chunk.hash)
    }
  }

  def testIO {
    TempDir.withTempDir { name =>
      val chan = new RandomAccessFile(new File(name, "testIO.dat"), "rw").getChannel
      var positions = Map[Int, Long]()
      var hashes = Map[Int, Hash]()
      for (i <- sizes) {
        positions += (i -> chan.position)
        val chunk = Chunk.make("blob", StringMaker.generate(i, i))
        chunk.write(chan)
        hashes += (i -> chunk.hash)
      }
      val finalPos = chan.position

      // Test sequential readback.
      chan.position(0)
      for (i <- sizes) {
        assert(chan.position == positions(i))
        val chunk = Chunk.read(chan)
        assert(chunk.hash == hashes(i))
      }
      assert(finalPos == chan.position)

      chan.close()
      // println("Tmp: " + new File(name, "testIO.dat"))
    }
  }

  private def show(chunk: Chunk) {
    println(chunk.toString)
  }

  private val sizes = {
    val items = new collection.mutable.ArrayBuffer[Int]
    var seen = Set[Int]()
    for {
      power <- 0 until 18
      offset <- -1 to 1
      item = (1 << power) + offset
      if item >= 0
      if !seen.contains(item)
    }
    {
      items += item
      seen += item
    }
    items
  }
}
