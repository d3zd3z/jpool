// Tree builder.

package org.davidb.jpool.pool

import scala.collection.mutable.Stack
import java.nio.ByteBuffer

object TreeBuilder {
  // Construct a treebuilder.  The 'prefix' is a 3 character string
  // used to indicate the indirection level.
  def makeBuilder(prefix: String, pool: ChunkStore): TreeBuilder =
    makeBuilder(prefix, pool, 256*1024)

  // Construct a treebuilder, limiting each indirect chunk to 'limit'
  // bytes.
  def makeBuilder(prefix: String, pool: ChunkStore, limit: Int): TreeBuilder =
    new TreeBuilder(prefix, pool, (limit / Hash.HashLength) * Hash.HashLength)

  // Iterate (lazily) through a tree rooted at a given hash.  Must use
  // the same prefix used to build the tree.
  def walk(prefix: String, pool: ChunkStore, hash: Hash): Iterable[Chunk] = {
    val chunk = pool(hash)
    val kind = chunk.kind
    if (kind.startsWith(prefix) && kind(3).isDigit)
      error("TODO: tree")
    else
      Stream.cons(chunk, Stream.empty)
  }

  // (Mutably) get the next hash from the given buffer.
  protected def getHash(buffer: ByteBuffer): Hash = {
    val raw = new Array[Byte](Hash.HashLength)
    buffer.get(raw)
    Hash.raw(raw)
  }
}

class TreeBuilder private (prefix: String, pool: ChunkStore, limit: Int) {
  require(prefix.length == 3)

  // Add a chunk to the built tree.  The chunk itself will be added to
  // the store, and it's hash will be remembered for this store.
  def add(chunk: Chunk) {
    pool += (chunk.hash -> chunk)
    room
    printf("Adding: %s%n", buffers.top)
    buffers.top.put(chunk.hash.getBytes)
    printf("Adding: %s%n", buffers.top)
  }

  // Write out any remaining buffers, and return the final chunk's
  // Hash describing the entire tree.
  def finish(): Hash = {
    if (buffers.length != 1)
      error("TODO: More than one")
    val top = buffers.pop
    top.flip
    if (top.remaining != Hash.HashLength)
      error("TODO: More than one (2)")
    TreeBuilder.getHash(top)
  }

  // Make sure the bytebuffer system has sufficient room.
  private def room {
    if (buffers.top.remaining == 0)
      error("TODO")
  }

  val buffers = new Stack[ByteBuffer]
  buffers.push(ByteBuffer.allocate(limit))
}
