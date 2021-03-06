/**********************************************************************/
// A ChunkSource is somethign that contains chunks, and can retrieve
// them.

package org.davidb.jpool
package pool

trait ChunkSource extends collection.Map[Hash, Chunk] {
  // Retrieve all of the backups available in this source.
  def getBackups: Set[Hash]

  // Close up any resources associated with this source.
  def close()

  // Flush out any pending data.
  def flush()

  // Set a progress meter.
  def setProgress(meter: DataProgress)
}

trait ChunkStore extends ChunkSource with collection.mutable.Map[Hash, Chunk] {
  // Settable limit on individual size of files.
  var limit: Int
}
