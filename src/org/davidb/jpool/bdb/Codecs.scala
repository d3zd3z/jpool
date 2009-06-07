//////////////////////////////////////////////////////////////////////
// Basic datatype encoding/decoding for database storage.
//
// This is intended to be used as  import bdb.Codecs._ in order
// to get the implicit definition.

package org.davidb.jpool.bdb

import java.nio.ByteBuffer

object Codecs {

  class EncodableString(base: String) extends bdb.Encodable {
    def encode = ByteBuffer.wrap(base.getBytes("UTF-8"))
  }
  implicit def toEncodableString(base: String) : EncodableString =
    new EncodableString(base)

  object stringDecoder extends bdb.Decoder[String] {
    def decode(buf: ByteBuffer): String = {
      new String(buf.array, buf.arrayOffset + buf.position, buf.remaining)
    }
  }

  class EncodableLong(base: Long) extends bdb.Encodable {
    def encode = {
      val buf = ByteBuffer.allocate(8)
      buf.putLong(base)
      buf.flip()
      buf
    }
  }
  implicit def toEncodableLong(base: Long) : EncodableLong =
    new EncodableLong(base)

  object longDecoder extends bdb.Decoder[Long] {
    def decode(buf: ByteBuffer): Long = buf.getLong()
  }
}
