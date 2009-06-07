//////////////////////////////////////////////////////////////////////
// Decoders usually live in a decoding object.

package org.davidb.jpool.bdb

import java.nio.ByteBuffer

trait Decoder[A] {
  def decode(data: ByteBuffer): A
}
