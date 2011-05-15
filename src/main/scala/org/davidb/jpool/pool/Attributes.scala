/**********************************************************************/
// File attributes.
//
// These are a lot like properties, except fit better into Scala, and
// have a "kind", and "name" property associated with them.
//
// Note that name fields and such are encoded into UTF-8 even though
// they come from byte encoded Linux filenames.  This means that
// Unicode characters will not be represented correctly in the UTF-8,
// but the conversion will be reversable, and allows the malformed
// filenames allowed under Linux.

package org.davidb.jpool
package pool

import org.apache.commons.codec.binary.Base64
import scala.collection.mutable.ListBuffer
import scala.xml
import java.nio.ByteBuffer

object Attributes {
  def decode(bytes: Array[Byte]): Attributes = decode(ByteBuffer.wrap(bytes))

  // Convert the given chunk from a pool into attributes.
  def decode(chunk: Chunk): Attributes = decode(chunk.data)

  def decode(data: ByteBuffer): Attributes = {
    val text = new String(data.array, data.arrayOffset + data.position,
      data.remaining, "UTF-8")
    val nodes = xml.XML.loadString(text)
    val kind = (nodes \ "@kind").text
    val entries = for (child <- nodes \ "entry") yield decodeEntry(child)
    new Attributes(kind, Map[String, String]() ++ entries)
  }

  // Convert the result of a Linux stat or lstat result into
  // Attributes.
  def ofLinuxStat(att: Linux.StatInfo) =
    new Attributes(att("*kind*"), att - "*kind*")

  private def decodeEntry(node: xml.Node): (String, String) = {
    val key = node \ "@key"
    val body = node.text
    if ((node \ "@encoded").length > 0) {
      val decoded = new String(Base64.decodeBase64(body.getBytes()), "iso8859-1")
      (key.text, decoded)
    } else {
      (key.text, body)
    }
  }
}

class Attributes(var kind: String,
  val contents: Map[String, String]) extends scala.collection.Map[String, String]
{
  // Map interface.
  override def size: Int = contents.size
  def get(key: String): Option[String] = contents.get(key)
  def iterator = contents.iterator
  def - (key: String): Attributes = sys.error("Cannot delete from Attributes")
  def + [B >: String](kv: (String, B)): Attributes = sys.error("Cannot add to Attributes")

  // Store these attributes into a storage pool.
  def store(pool: ChunkStore): Hash = {
    val chunk = Chunk.make("node", ByteBuffer.wrap(toByteArray()))
    pool += (chunk.hash -> chunk)
    chunk.hash
  }

  // Convert this node into XML.
  def toXML: xml.Elem = {
    val items = for (key <- contents.keysIterator.toList.sortWith(_<_))
      yield entryEncode(key, contents(key))
    <node kind={kind}>{items}</node>
  }

  // Encode this entry, using base-64 if necessary.  Also validates
  // that every character fits within a byte.  Note that a space will
  // cause us to use base64, since a space at the end or beginning of
  // a string might easily be eaten.
  private def entryEncode(key: String, value: String): xml.Elem = {
    if (value exists (ch => ch.toInt > 255))
      sys.error("Encoding error, non 8-bit character")
    if (value forall (ch => ch >= '!' && ch <= '~')) {
      <entry key={key}>{value}</entry>
    } else {
      val encoded = new String(Base64.encodeBase64(value.getBytes("iso8859-1")))
      <entry key={key} encoded="base64">{encoded}</entry>
    }
  }

  // Convert this node into a Binary representation of the XML.
  def toByteArray(): Array[Byte] = {
    val buf = new StringBuilder
    buf.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
    buf.append(xml.Utility.toXML(toXML))
    buf.toString.getBytes("UTF-8")
  }

  override def toString(): String = xml.Utility.toXML(toXML).toString

  override def equals(that: Any): Boolean = that match {
    case other: Attributes =>
      kind == other.kind &&
        contents == other.contents
    case _ => false
  }

  override def hashCode(): Int =
    (kind.hashCode + 41) * 41 + contents.hashCode
}
