//////////////////////////////////////////////////////////////////////
// Debugging utility.
//
// Usage:
//   MkIndex base basename count

package org.davidb.jpool.tools

import java.nio.ByteBuffer
import org.davidb.jpool

class TestHashIndex(val basePath: String, val prefix: String) extends {
  protected val encoder = new FixedEncodable[(Int,Int)] {
    def EBytes = 8
    def encode(obj: (Int,Int), buf: ByteBuffer) {
      buf.putInt(obj._1)
      buf.putInt(obj._2)
    }
    def decode(buf: ByteBuffer): (Int,Int) = {
      val file = buf.getInt()
      val offset = buf.getInt()
      (file, offset)
    }
  }
} with HashIndex[(Int,Int)]

object MkIndex {
  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println("Usage: MkIndex path basename count")
      System.exit(1)
    }

    val hi = new TestHashIndex(args(0), args(1))
    val limit = Integer.parseInt(args(2))
    for (i <- 1 to limit) {
      val hash = Hash("blob", i.toString)
      hi += (hash -> (i, 42))
    }
    hi.close()
  }
}
