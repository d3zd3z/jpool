//////////////////////////////////////////////////////////////////////
// Garbage collection operations on pools.

package org.davidb.jpool.pool

import org.davidb.jpool._

object GC {
  sealed abstract class MarkVisited
  case object Seen extends MarkVisited
  case object Unseen extends MarkVisited

  // Marker function passed around the garbage collector.  Given a
  // hash, and a thunk to possibly read the chunk itself (used for
  // copy-type GC).  The function should return 'Seen' or 'Unseen' to
  // indicate if this particular hash was seen before (the GC will use
  // this to avoid walking parts of the tree that are already marked.
  type MarkFn = (Hash, () => Chunk) => MarkVisited
}
