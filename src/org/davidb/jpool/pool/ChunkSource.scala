//////////////////////////////////////////////////////////////////////
// A ChunkSource is somethign that contains chunks, and can retrieve
// them.

package org.davidb.jpool.pool

trait ChunkSource {
  // Retrieve all of the backups available in this source.
  def getBackups: Set[Hash]

  // Attempt to read a single Chunk from this pool.
  def readChunk(hash: Hash): Option[Chunk]

  // Close up any resources associated with this source.
  def close()
}
