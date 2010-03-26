//////////////////////////////////////////////////////////////////////
// Restoring tar files from the storage pool.

package org.davidb.jpool.pool

import java.nio.channels.WritableByteChannel
import java.nio.ByteBuffer
import java.util.Properties

class TarRestore(pool: ChunkSource, dest: WritableByteChannel) {

  Progress.open()

  def decode(hash: Hash) {
    val back = Back.load(pool, hash)
    decodeTar(back.hash)
  }

  private def decodeTar(hash: Hash) {
    for (head <- TreeBuilder.walk("tar", pool, hash)) {
      head.kind match {
        case "tard" => decodeDirect(head.data)
        case "tari" => decodeIndirect(head.data)
        case unknown => error("Unknown tar header block type '%s'" format unknown)
      }
    }
  }

  // Write the tar EOF.
  def finish() {
    val buf = ByteBuffer.allocate(512)
    // Send at least two zero blocks.
    send(buf)
    buf.clear
    send(buf)
    // And as many as necessary to write something out.
    while (buffer.remaining > 0) {
      buf.clear
      send(buf)
    }
    ship()
    Progress.close()
  }

  private def decodeDirect(head: ByteBuffer) {
    send(head)
  }

  private def decodeIndirect(head: ByteBuffer) {
    val olimit = head.limit
    head.limit(512)
    send(head)
    head.limit(olimit)
    assert(head.remaining == Hash.HashLength)
    val rawHash = new Array[Byte](Hash.HashLength)
    head.get(rawHash)
    val hash = Hash.raw(rawHash)
    // printf("Indirect %s (%s)%n", head, hash)
    for (chunk <- TreeBuilder.walk("ind", pool, hash)) {
      // printf("  chunk: %s%n", chunk.data)
      send(chunk.data)
    }
  }

  // Buffering.
  val buffer = ByteBuffer.allocate(10240)
  private def send(input: ByteBuffer) {
    while (input.remaining > 0) {
      if (buffer.remaining == 0)
        ship()
      // Determine how much we can put.
      val olimit = input.limit
      input.limit(input.position + (buffer.remaining min input.remaining))
      buffer.put(input)
      input.limit(olimit)
    }
  }

  private def ship() {
    require(buffer.remaining == 0)
    buffer.flip
    dest.write(buffer)
    buffer.clear
  }
}
