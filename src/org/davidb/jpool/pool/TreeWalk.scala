//////////////////////////////////////////////////////////////////////
// Listing backup contents.

package org.davidb.jpool.pool

import org.davidb.logging.Logger

import java.util.Properties
import java.io.ByteArrayInputStream

class TreeWalk(pool: ChunkSource) extends AnyRef with Logger {

  sealed class State
  case object Enter extends State
  case object Leave extends State
  case object Node extends State
  case class Visitor(path: String, level: Int, atts: Attributes, state: State)

  // Recursively walk the nodes.  For directories, Visits the
  // directory with state set to 'Enter', then the nodes, and then the
  // state set to 'Leave'.
  def walk(hash: Hash): Stream[Visitor] = {
    val (_, nodeHash) = lookupHash(hash)
    val node = pool(nodeHash)
    walk(node, ".", 0)
  }

  def walk(node: Chunk, path: String, level: Int): Stream[Visitor] = {
    if (node.kind == "null")
      return Stream.empty
    if (node.kind != "node") {
      warn("Backup node is not of type 'node': %s (%s)", node.hash, node.kind)
      return Stream.empty
    }
    val atts = Attributes.decode(node)
    val fullPath = if (level == 0) "." else "%s/%s" format (path, atts.name)
    if (atts.kind == "DIR") {
      val children = Hash.ofString(atts("children"))

      Stream.cons(new Visitor(fullPath, level, atts, Enter),
        Stream.concat(TreeBuilder.walk("dir", pool, children) map (walk(_, fullPath, level + 1))) append
          Stream(new Visitor(fullPath, level, atts, Leave)))
    } else {
      Stream(new Visitor(fullPath, level, atts, Node))
    }
  }

  // Recursively show the backup listing starting at the given hash.
  def show(hash: Hash) {
    for (node <- walk(hash)) {
      printf("%5s %4s %s%n", node.state, node.atts.kind, node.path)
    }
  }

  // Lookup the hash associated with a given tar set.
  def lookupHash(hash: Hash): (Properties, Hash) = {
    val chunk = pool(hash)
    val data = chunk.data
    val encoded = new ByteArrayInputStream(data.array, data.arrayOffset + data.position, data.remaining)
    val props = new Properties
    props.loadFromXML(encoded)
    (props, Hash.ofString(props.getProperty("hash")))
  }
}
