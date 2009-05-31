//////////////////////////////////////////////////////////////////////
// Restoring tree data.

package org.davidb.jpool.pool

import org.davidb.logging.Logger

class TreeRestore(pool: ChunkSource) extends AnyRef with Logger {
  // Restore the specified tree based at the named path.
  def restore(hash: Hash, path: String) {
    checkRoot(path)
    val walker = new TreeWalk(pool)
    for (node <- walker.walk(hash, path)) {
      node.state match {
        case walker.Enter =>
          if (node.level > 0) {
            Linux.mkdir(node.path)
          }
        case walker.Leave =>
          Linux.restoreStat(node.path, node.atts)
        case walker.Node =>
          if (node.atts.kind == "REG") {
            val dataHash = Hash.ofString(node.atts("data"))
            FileData.restore(pool, node.path, dataHash)
            Progress.addData(node.atts("size").toLong)
          }
          Linux.restoreStat(node.path, node.atts)
      }
      Progress.addNode()
    }
  }

  // Verify that the root of this restore is a (nearly) empty
  // directory.  It must be a directory, and it must contain either no
  // files, or a single directory called "lost+found".
  private def checkRoot(path: String) {
    val atts = Attributes.ofLinuxStat(Linux.lstat(path))
    if (atts.kind != "DIR") {
      error("Root of restore is not a directory")
    }

    Linux.readDir(path) match {
      case List() =>
      case List(("lost+found", _)) =>
        val lfatts = Attributes.ofLinuxStat(Linux.lstat(path + "/lost+found"))
        if (lfatts.kind != "DIR") {
          error("Root of restore has lost+found that isn't a directory")
        }
      case _ =>
        error("Root of restore is a non-empty directory")
    }
  }
}
