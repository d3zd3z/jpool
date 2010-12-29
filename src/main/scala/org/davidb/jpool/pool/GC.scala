/**********************************************************************/
// Garbage collection operations on pools.

package org.davidb.jpool
package pool

object GC {
  // An object with this trait is used by the garbage collector to
  // mark the traversal.  The GC will call isSeen() before descending
  // through the children of a node to determine if this node is
  // already present.  A node is marked only after all of it's
  // children are visited.
  //
  // The visit method is used by the garbage collector itself to
  // perform an operation bracketed by this check and the trailing
  // mark operation.
  trait Visitor {
    // Has this node been visited?  This will be called pre-order in
    // the GC scan.
    def isSeen(hash: Hash): Boolean

    // Mark this node.  This will be called post-order, such that it's
    // children will always have been called first.  'visit' will only
    // be called if isSeen had returned Unseen.
    def mark(chunk: Chunk)

    // A common pattern is to check if seen, do something and then
    // mark when finished.  Capture that with this method.
    def visit(chunk: Chunk)(thunk: => Unit) {
      if (!isSeen(chunk.hash)) {
        thunk
        mark(chunk)
      }
    }
  }
}
