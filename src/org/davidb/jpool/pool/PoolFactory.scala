//////////////////////////////////////////////////////////////////////
// Pool generation factory (from URI).

package org.davidb.jpool.pool

import java.net.{URI, UnknownServiceException}

object PoolFactory {
  // Pools have different capabilities (ChunkSource, ChunkStore, etc).
  // This factory returns the most general (PoolReader) and pattern
  // matching or instance tests can be used to determine if other
  // traits apply.
  def getInstance(uri: URI): ChunkSource = {
    if (uri.getScheme != "jpool")
      throw new IllegalArgumentException("Pool URI must start with 'jpool:'")
    val u2 = new URI(uri.getRawSchemeSpecificPart)
    throw new UnknownServiceException("Unknown pool service: " + u2.getScheme)
  }
}
