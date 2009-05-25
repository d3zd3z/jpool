//////////////////////////////////////////////////////////////////////
// Listing backup contents.

package org.davidb.jpool.pool

import org.davidb.logging.Logger

class TreeList(pool: ChunkSource) extends AnyRef with Logger {
  // Recursively show the backup listing starting at the given hash.
  def show(hash: Hash) {
    val node = pool(hash)
    show(node, ".", 0)
  }

  def show(node: Chunk, path: String, level: Int) {
    if (node.kind == "null")
      return
    if (node.kind != "node") {
      warn("Backup node is not of type node: %s (%s)", node.hash, node.kind)
      return
    }
    val atts = Attributes.decode(node)
    val fullPath =
      if (level == 0) "."
      else "%s/%s" format (path, atts.name)
    if (atts.kind == "DIR") {
      printf("<DIR %s%n", fullPath)
      val children = Hash.ofString(atts("children"))
      TreeBuilder.walk("dir", pool, children) foreach (show(_, fullPath, level + 1))
    }
    printf("%4s %s%n", atts.kind, fullPath)
  }
}
