//////////////////////////////////////////////////////////////////////
// String encoding/decoding for database storage.
//
// Strings are used enough as both keys and data that we provide the
// helper here.
//
// This is intended to be used as  import bdb.StringCoder._ in order
// to get the implicit definition.

package org.davidb.jpool.bdb

import java.nio.ByteBuffer

object StringCoder {
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
}
