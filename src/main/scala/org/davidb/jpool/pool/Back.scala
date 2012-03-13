/**********************************************************************/
// Management of overall backup properties.

package org.davidb.jpool
package pool

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer
import java.util.Properties
import collection.JavaConversions._

object Back {
  // Retrieve the properties associated with this backup.
  def load(pool: ChunkSource, hash: Hash): Back = {
    val chunk = pool(hash)
    val data = chunk.data
    if (data.get(data.position) == '<') {
      val encoded = new ByteArrayInputStream(data.array, data.arrayOffset + data.position, data.remaining)
      val props = new Properties
      props.loadFromXML(encoded)
      new Back(props)
    } else {
      val atts = Attributes.decode(chunk)
      if (atts.kind != "back")
	sys.error("Backup record is not of type \"back\"")
      val props = new Properties
      for ((key, value) <- atts.contents)
	props.setProperty(key, value)
      new Back(props)
    }
  }
}

class Back(val props: Properties) {
  // Write the properties, encoded to a chunk in the specified pool,
  // returning the hash describing this chunk.
  def storeXml(pool: ChunkStore): Hash = {
    val buf = new ByteArrayOutputStream
    props.storeToXML(buf, "Backup")
    val encoded = ByteBuffer.wrap(buf.toByteArray)
    val chunk = Chunk.make("back", encoded)
    pool += (chunk.hash -> chunk)
    chunk.hash
  }

  def store(pool: ChunkStore): Hash = {
    var map = Map.empty[String, String]
    for (key <- props.stringPropertyNames()) {
      val value = props.getProperty(key)
      map += (key -> value)
    }
    new Attributes("back", map).store(pool, "back")
  }

  // Retrieve the hash property, as a hash.
  def hash: Hash = Hash.ofString(props.getProperty("hash"))
}
