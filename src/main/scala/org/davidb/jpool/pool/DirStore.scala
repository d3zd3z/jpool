//////////////////////////////////////////////////////////////////////
// Dirstore.
//
// Whereas the 'Attributes'/Node maps to the information stored in the
// inode in the filesystem, the Dirstore represents the data stored in
// a directory.  It is essentially a list of pairs of names and
// hashes.

package org.davidb.jpool
package pool

import java.nio.ByteBuffer

object DirStore {
  // Iterate (lazily) through the directory specified by the given
  // hash.
  def walk(pool: ChunkSource, hash: Hash): Iterator[(String, Hash)] = {
    for {
      node <- TreeBuilder.walk("dir", pool, hash)
      entry <- decode(node)
    } yield entry
  }

  // Perform a GC walk across a node.
  def gcWalk(node: Chunk, visit: Hash => Unit) {
    val data = node.data
    while (data.remaining > 0) {
      val nameLength = data.getShort
      val nameEncoded = new Array[Byte](nameLength)
      data.get(nameEncoded)
      val name = new String(nameEncoded, "iso8859-1")
      val hashBuf = new Array[Byte](Hash.HashLength)
      data.get(hashBuf)
      val hash = Hash.raw(hashBuf)
      visit(hash)
    }
  }

  // This is a bit fragile, since the stream walks through a mutable
  // structure.
  private def decode(chunk: Chunk): Iterator[(String, Hash)] = decodeData(chunk.data)

  private def decodeData(data: ByteBuffer): Iterator[(String, Hash)] = new Iterator[(String, Hash)] {
    def hasNext = (data.remaining > 0)
    def next(): (String, Hash) = {
      val nameLength = data.getShort
      val nameEncoded = new Array[Byte](nameLength)
      data.get(nameEncoded)
      val name = new String(nameEncoded, "iso8859-1")
      val hashBuf = new Array[Byte](Hash.HashLength)
      data.get(hashBuf)
      val hash = Hash.raw(hashBuf)
      (name, hash)
    }
  }
}

class DirStore (pool: ChunkStore, limit: Int) {
  private val builder = TreeBuilder.makeBuilder("dir", pool, limit)
  private var buffer = ByteBuffer.allocate(limit)

  def append(name: String, hash: Hash) {
    if (name exists (ch => ch.toInt > 255))
      error("Encoding error, encountered name with non 8-bit encoded value")
    val encoded = name.getBytes("iso8859-1")
    val elength = encoded.length
    require (elength > 0 && elength <= java.lang.Short.MAX_VALUE)
    val totalLength = 2 + elength + Hash.HashLength
    if (buffer.remaining < totalLength)
      ship
    if (buffer.remaining < totalLength)
      error("DirStore buffer size is insufficient for filename length")
    buffer.putShort(elength.toShort)
    buffer.put(encoded)
    buffer.put(hash.getBytes)
  }

  def finish(): Hash = {
    ship
    builder.finish()
  }

  private def ship {
    if (buffer.position > 0) {
      buffer.flip()
      // printf("Shipping DirStore buffer%n")
      // Pdump.dump(buffer)
      builder.add(Chunk.make("dir ", buffer))
      buffer = ByteBuffer.allocate(limit)
    }
  }
}
