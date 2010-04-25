//////////////////////////////////////////////////////////////////////
// Store and retrieve file data into the pool.

package org.davidb.jpool.pool

import org.davidb.jpool._
import java.nio.ByteBuffer

object FileData {
  // Store the contents of the named path into the given storage pool.
  // Returns the Hash needed to restore (below) that file.  If there
  // is a problem, the exception will be propagated.
  def store(pool: ChunkStore, path: String): Hash = {
    var builder = TreeBuilder.makeBuilder("ind", pool, chunkSize)
    def process(buf: ByteBuffer) {
      val chunk = Chunk.make("blob", buf)
      builder.add(chunk)
    }
    Linux.readFile(path, chunkSize, process _)
    builder.finish()
  }

  // Restore the file previously stored with 'store' into a file at
  // the named path.
  def restore(pool: ChunkSource, path: String, hash: Hash) {
    Linux.writeFile(path, TreeBuilder.walk("ind", pool, hash).map(_.data))
  }

  // Chunk size to use.  This is a 'var' but it really isn't intended
  // to be set.  This could be made a ThreadLocal, but changing it is
  // really only used for testing.
  var chunkSize = 256*1024
}
