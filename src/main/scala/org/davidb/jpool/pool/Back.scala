//////////////////////////////////////////////////////////////////////
// Management of overall backup properties.

package org.davidb.jpool.pool

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer
import java.util.Properties

object Back {
  // Retrieve the properties associated with this backup.
  def load(pool: ChunkSource, hash: Hash): Back = {
    val chunk = pool(hash)
    val data = chunk.data
    val encoded = new ByteArrayInputStream(data.array, data.arrayOffset + data.position, data.remaining)
    val props = new Properties
    props.loadFromXML(encoded)
    new Back(props)
  }
}

class Back(val props: Properties) {
  // Write the properties, encoded to a chunk in the specified pool,
  // returning the hash describing this chunk.
  def store(pool: ChunkStore): Hash = {
    val buf = new ByteArrayOutputStream
    props.storeToXML(buf, "Backup")
    val encoded = ByteBuffer.wrap(buf.toByteArray)
    val chunk = Chunk.make("back", encoded)
    pool += (chunk.hash -> chunk)
    chunk.hash
  }

  // Retrieve the hash property, as a hash.
  def hash: Hash = Hash.ofString(props.getProperty("hash"))
}
