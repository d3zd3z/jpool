// Tree builder.

package org.davidb.jpool
package pool

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
  def walk(prefix: String, pool: ChunkSource, hash: Hash): Iterator[Chunk] = {
    val chunk = pool(hash)
    val kind = chunk.kind
    if (kind.startsWith(prefix) && kind(3).isDigit) {
      val buffer = chunk.data
      val iter = new Iterator[Iterator[Chunk]] {
        def hasNext: Boolean = buffer.remaining > 0
        def next: Iterator[Chunk] = walk(prefix, pool, getHash(buffer))
      }
      iter.flatten
    } else
      Iterator.single(chunk)
  }

  // Perform a GC walk of the given tree.  Will call 'mark' for each
  // directory node to visit, and nodeWalk to walk each of the nodes
  // in the directory.
  def gcWalk(prefix: String, pool: ChunkSource, hash: Hash, visitor: GC.Visitor, nodeWalk: Chunk => Unit) {
    val chunk = pool(hash)
    val kind = chunk.kind
    if (kind.startsWith(prefix) && kind(3).isDigit) {
      visitor.visit(chunk) {
        val buffer = chunk.data
        while (buffer.remaining > 0)
          gcWalk(prefix, pool, getHash(buffer), visitor, nodeWalk)
      }
    } else {
      nodeWalk(chunk)
    }
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
    // printf("Adding: %s%n", buffers.top)
    append(chunk.hash, 0)
    // printf("Adding: %s%n", buffers.top)
  }

  // Add a chunk, by hash, e.g. we've already stored the chunk and now
  // just have the hash for it.
  def add(hash: Hash) = append(hash, 0)

  // Write out any remaining buffers, and return the final chunk's
  // Hash describing the entire tree.
  def finish(): Hash = {
    if (buffers.length != 1) {
      // Collapse buffers.
      var level = 0
      while (buffers.length > 1) {
        val tmp = buffers.pop()
        append(summarize(tmp, level + 1), level + 1)
        level += 1
      }
    }
    val top = buffers.pop
    summarize(top, 0)
  }

  // Summarize the hashes in the buffer, naming them according to the
  // given level.  Avoids creating a summary level for the special
  // case of a single hash.  The buffer should be filled (with the
  // position at the end).
  private def summarize(buffer: ByteBuffer, level: Int): Hash = {
    buffer.flip
    if (buffer.remaining == 0) {
      if (level == 0) {
        // Empty chunk is allowed, but only at the first level.
        // TODO: Do we want to be more specific than "null" for the
        // type?
        val chunk = Chunk.make("null", "")
        pool += (chunk.hash -> chunk)
        return chunk.hash
      } else
        error("Internal error: Empty hash buffer at non-zero level")
    }

    if (buffer.remaining == Hash.HashLength) {
      TreeBuilder.getHash(buffer)
    } else {
      val chunk = Chunk.make(prefix + level.toString, buffer)
      pool += (chunk.hash -> chunk)
      chunk.hash
    }
  }

  // Append this hash, at level 'n', to the top of the stack.
  private def append(hash: Hash, level: Int) {
    if (buffers.isEmpty) {
      push(hash)
    } else if (buffers.top.remaining == 0) {
      val sumHash = summarize(buffers.pop(), level + 1)
      append(sumHash, level + 1)
      push(hash)
    } else {
      buffers.top.put(hash.getBytes)
    }
  }

  // Create a new buffer, and push the specified hash onto it.
  private def push(hash: Hash) {
    val buf = ByteBuffer.allocate(limit)
    buffers.push(buf)
    buf.put(hash.getBytes)
  }

  val buffers = new Stack[ByteBuffer]
  buffers.push(ByteBuffer.allocate(limit))
}
