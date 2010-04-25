//////////////////////////////////////////////////////////////////////
// Listing backup contents.

package org.davidb.jpool.pool

import org.davidb.logging.Loggable

import java.util.Properties
import java.io.ByteArrayInputStream

class TreeWalk(pool: ChunkSource) extends AnyRef with Loggable {

  sealed class State
  case object Enter extends State
  case object Leave extends State
  case object Node extends State
  case class Visitor(path: String, level: Int, atts: Attributes, state: State)

  // Recursively walk the nodes.  For directories, Visits the
  // directory with state set to 'Enter', then the nodes, and then the
  // state set to 'Leave'.
  def walk(hash: Hash): Stream[Visitor] = walk(hash, ".")

  def walk(hash: Hash, path: String): Stream[Visitor] = {
    val back = Back.load(pool, hash)
    val node = pool(back.hash)
    walk(node, path, 0)
  }

  def walk(node: Chunk, path: String, level: Int): Stream[Visitor] = {
    if (node.kind == "null")
      return Stream.empty
    if (node.kind != "node") {
      logger.warn("Backup node is not of type 'node': %s (%s)".format(node.hash, node.kind))
      return Stream.empty
    }
    val atts = Attributes.decode(node)
    if (atts.kind == "DIR") {
      val children = Hash.ofString(atts("children"))

      def subWalk(node: (String, Hash)): Stream[Visitor] = {
        val subNode = pool(node._2)
        walk(subNode, path + "/" + node._1, level + 1)
      }

      val subtree = for {
        node <- DirStore.walk(pool, children)
        elt <- subWalk(node)
      } yield elt
      Stream.cons(new Visitor(path, level, atts, Enter),
        subtree append Stream(new Visitor(path, level, atts, Leave)))
    } else {
      Stream(new Visitor(path, level, atts, Node))
    }
  }

  // Recursively show the backup listing starting at the given hash.
  def show(hash: Hash) {
    for (node <- walk(hash)) {
      printf("%5s %4s %s%n", node.state, node.atts.kind, node.path)
    }
  }

  // Traverse all chunks within a particular tree, calling 'mark' on
  // each chunk.  'mark' should return status as to whether or not it
  // has seen the chunk before or not.  'mark' can also will be called
  // with a 'get' argument which can be used to return the chunk.
  // This should be used instead of directly reading the chunk, since
  // many chunks will have already been read in as part of the
  // traversal.

  def gcWalk(hash: Hash, visitor: GC.Visitor) {
    val node = pool(hash)
    gcWalk(node, visitor)
  }
  def gcWalk(node: Chunk, visitor: GC.Visitor) {
    visitor.visit(node) {
      node.kind match {
        case "back" =>
          // This reloads the same chunk, but doesn't happen very
          // often.
          val back = Back.load(pool, node.hash)
          gcWalk(back.hash, visitor)

        case "node" =>
          val atts = Attributes.decode(node)
          if (atts.kind == "DIR") {
            val children = Hash.ofString(atts("children"))
            def childWalk(chunk: Chunk) {
              require(chunk.kind == "dir " || chunk.kind == "null")
              visitor.visit(chunk) {
                DirStore.gcWalk(chunk, (hash: Hash) => gcWalk(hash, visitor))
              }
            }
            TreeBuilder.gcWalk("dir", pool, children, visitor, childWalk _)
          } else if (atts.kind == "REG") {
            val data = Hash.ofString(atts("data"))
            def pieceWalk(chunk: Chunk) {
              // TODO: Make this not read all of the data if it is not
              // needed.
              require(chunk.kind == "blob" || chunk.kind == "null")
              visitor.visit(chunk) { }
            }
            TreeBuilder.gcWalk("ind", pool, data, visitor, pieceWalk _)
          } else {
            // Otherwise, this node has no extra data, so we're done
            // with it.
            // error("TODO: Kind '%s'" format atts.kind)
          }

        case x => error("Unknown backup node '%s'" format x)
      }
    }
  }
}
