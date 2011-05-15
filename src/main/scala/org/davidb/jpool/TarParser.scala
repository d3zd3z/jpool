// Tar file parser.

package org.davidb.jpool

import scala.annotation.tailrec
import java.nio.channels.{Channels, ReadableByteChannel}
import java.nio.ByteBuffer

object TarParser {
  // Decode the buffer as a tar header, returning an Option for the
  // header, with None indicating the EOF marker is reached.
  def decode(data: ByteBuffer): Option[TarHeader] = {
    require(data.remaining == 512)
    val rawData = data.duplicate
    data.position(257)
    val piece = getBytes(data, 5)
    if (new String(piece) == "ustar") {
      // Valid tar block.
      // TODO: Deal with values >8GB.
      data.position(124)
      val octSize = getBytes(data, 12)
      if (octSize(11) != 0)
        sys.error("Not null terminated")
      val size = java.lang.Long.parseLong(new String(octSize, 0, 11), 8)
      Some(new TarHeader(rawData, size))
    } else {
      // All zeros is EOF.
      data.position(0)
      var pos = 0
      while (pos < 512) {
        if (data.get != 0)
          sys.error("Invalid tar block seen")
        pos += 1
      }
      None
    }
  }

  private def getBytes(buf: ByteBuffer, size: Int): Array[Byte] = {
    val result = new Array[Byte](size)
    buf.get(result)
    result
  }
}

class TarParser(chan: ReadableByteChannel) {
  private val buffer = ByteBuffer.allocate(20 * 512)
  fill

  // Get a single block.
  def get: ByteBuffer = {
    if (buffer.remaining == 0)
      fill
    val result = buffer.slice()
    result.limit(512)
    // printf("Result: %s%n" format result)
    buffer.position(buffer.position + 512)
    result
  }

  // Decode the next tar header, returning an Option for the header,
  // with None indicating that the EOF marker is reached.
  def getHeader: Option[TarHeader] = {
    TarParser.decode(get)
  }

  private def fill {
    buffer.clear
    while (buffer.remaining > 0) {
      val count = chan.read(buffer)
      if (count <= 0)
        throw new java.io.EOFException("Early end of tar file reached")
    }
    buffer.flip
  }
}

class TarHeader(raw_ : ByteBuffer, val size: Long) {
  def dataBlocks = (size + 511) >>> 9
  val raw: ByteBuffer = raw_.duplicate
}

// For testing, read a tarfile from stdin, and decode the pieces.
object TarDecode {
  def main(args: Array[String]) {
    val stdin = Channels.newChannel(System.in)
    val tar = new TarParser(stdin)
    @tailrec def loop {
      tar.getHeader match {
        case None => println("EOF")
        case Some(x) =>
          printf("Tar %d blocks=%d%n", x.size, x.dataBlocks)
          var left = x.dataBlocks
          while (left > 0L) {
            tar.get
            left -= 1
          }
          loop
      }
    }
    loop
  }
}
