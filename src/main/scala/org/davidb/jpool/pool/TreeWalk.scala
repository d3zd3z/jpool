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

      Stream.cons(new Visitor(path, level, atts, Enter),
        Stream.concat(DirStore.walk(pool, children) map (subWalk _)) append
          Stream(new Visitor(path, level, atts, Leave)))
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
}
