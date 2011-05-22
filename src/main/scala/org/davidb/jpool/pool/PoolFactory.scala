/**********************************************************************/
// Pool generation factory (from URI).

package org.davidb.jpool.pool

import java.io.File
import java.net.{URI, UnknownServiceException}

object PoolFactory {
  // Pools have different capabilities (ChunkSource, ChunkStore, etc).
  // This factory returns the most general (PoolReader) and pattern
  // matching or instance tests can be used to determine if other
  // traits apply.
  def getInstance(uri: URI): ChunkSource = {
    // As a special case, accept a raw filepath (absolute or
    // relative), and treat it as a local file reference.
    if (uri.getScheme eq null) {
      return new FilePool(new File(uri.getPath).getCanonicalFile)
    }

    if (uri.getScheme != "jpool")
      throw new IllegalArgumentException("Pool URI must start with 'jpool:'")
    val u2 = new URI(uri.getRawSchemeSpecificPart)
    u2.getScheme match {
      case "file" =>
        new FilePool(new File(u2.getPath))
      case scheme =>
        throw new UnknownServiceException("Unknown pool service: " + scheme)
    }
  }

  def getStoreInstance(uri: URI): ChunkStore = {
    getInstance(uri) match {
      case st: ChunkStore => st
      case _ => throw new UnsupportedOperationException("Request store is not writable")
    }
  }
}
