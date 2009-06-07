//////////////////////////////////////////////////////////////////////
// Defines a class that is encodable.

package org.davidb.jpool.bdb

import java.nio.ByteBuffer

trait Encodable {
  def encode: ByteBuffer
}
