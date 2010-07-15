//////////////////////////////////////////////////////////////////////
// Restoring tree data.

package org.davidb.jpool
package pool

import scala.collection.mutable
import org.davidb.logging.Loggable

class TreeRestore(pool: ChunkSource, meter: BackupProgressMeter)
    extends AnyRef with Loggable {
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
          if (node.atts.kind == "REG")
            regRestore(node)
          Linux.restoreStat(node.path, node.atts)
      }
      meter.addNode()
    }
  }

  // Perform restore of a regular file.  Keeps track of restored
  // hardlinks and recreates the links when they are discovered.
  private def regRestore(node: TreeWalk#Visitor) {
    val dataHash = Hash.ofString(node.atts("data"))
    val nlink = node.atts("nlink").toInt
    if (nlink == 1) {
      FileData.restore(pool, node.path, dataHash)
    } else {
      val inum = node.atts("ino").toLong
      if (links contains inum) {
        Linux.link(links(inum).path, node.path)
      } else {
        FileData.restore(pool, node.path, dataHash)
        links += (inum -> node)
      }
    }
    meter.addData(node.atts("size").toLong)
  }

  // TODO: Track link counts so we can delete nodes once we've written
  // the last link.
  private val links = mutable.Map.empty[Long, TreeWalk#Visitor]

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
