// Utilities for file I/O.

package org.davidb.jpool

import java.nio.ByteBuffer
import java.nio.channels.{FileChannel, ReadableByteChannel}

object FileUtil {
  def fullWrite(chan: FileChannel, _bufs: ByteBuffer*) {
    val bufs = _bufs.toArray
    var len = bufs.foldLeft(0)(_ + _.remaining).toLong
    while (len > 0) {
      val count = chan.write(bufs)
      if (count <= 0)
        sys.error("Unable to write buffer data")
      len -= count
    }
  }

  // Read 'count' bytes fully into a new ByteBuffer.
  def readBytes(chan: ReadableByteChannel, count: Int): ByteBuffer = {
    val buf = ByteBuffer.allocate(count)
    while (buf.remaining > 0) {
      val tmp = chan.read(buf)
      if (tmp <= 0)
        sys.error("Unable to read data from channel")
    }
    buf.rewind()
    buf
  }

  // Extract 'count' bytes out of a ByteBuffer.
  def getBytes(buf: ByteBuffer, count: Int): Array[Byte] = {
    val result = new Array[Byte](count)
    buf.get(result)
    result
  }
}
