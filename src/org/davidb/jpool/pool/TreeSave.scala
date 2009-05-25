//////////////////////////////////////////////////////////////////////
// Saving and restoring tree data.

package org.davidb.jpool.pool

import scala.collection.mutable.ListBuffer
import org.davidb.logging.Logger

class TreeSave(pool: ChunkStore, rootPath: String) extends AnyRef with Logger {
  // Store the item, of whatever type it is, into the given storage
  // pool, returning the hash needed to retrieve it later.  If unable
  // to save the item, the exception will be propagated.  Directories
  // will warn, but will save as much as possible.
  def store(): Hash = {
    store(rootPath, "%root%", rootStat)
  }

  // Internal store, where we've already statted the nodes (avoids
  // duplicate stats, since directory traversal requires statting).
  def store(path: String, name: String, stat: Linux.StatInfo): Hash = {
    handlers.get(stat("*kind*")) match {
      case None =>
        logError("Unknown filesystem entry kind: %s (%s)", stat("*kind*"), path)
        error("Cannot dump entry")
      case Some(handler) =>
      handler(path, name, stat)
    }
  }

  private var handlers = Map[String, (String, String, Linux.StatInfo) => Hash]()

  private val rootStat = Linux.lstat(rootPath)
  if (rootStat("*kind*") != "DIR") {
    logError("Root of backup must be a directory: %s", rootPath)
    error("Cannot save")
  }

  // TODO: Implement DirCache to avoid dumping files that are already
  // known.

  private def handleDir(path: String, name: String, stat: Linux.StatInfo): Hash = {
    var nstats = new ListBuffer[(String, Linux.StatInfo)]

    // Iterate over the names sorted by inode number, statting each
    // entry.  Don't descend directories that cross device boundaries.
    if (stat("dev") == rootStat("dev")) {
      for ((name, _) <- Linux.readDir(path).sort(byInode _)) {
        try {
          val stat = Linux.lstat(path + "/" + name)
          nstats += (name, stat)
        } catch {
          case e: NativeError =>
            warn("Unable to stat file, skipping: %s", path)
        }
      }
    }

    // Sort the results by name.  The sort helps repeated backups of
    // unchanged directories to keep the same hash.
    val items = nstats.toList.sort(byName _)

    val builder = TreeBuilder.makeBuilder("dir", pool)

    for ((name, childStat) <- items) {
      val fullName = path + "/" + name
      try {
        val childHash = store(fullName, name, childStat)
        builder.add(childHash)
      } catch {
        case e: NativeError =>
          warn("Unable to backup node, skipping: %s", fullName)
      }
    }

    val children = builder.finish()

    var fullStat = stat
    // fullStat += ("path" -> path)
    fullStat += ("children" -> children.toString())

    Attributes.ofLinuxStat(fullStat, name).store(pool)
  }
  handlers += ("DIR" -> handleDir _)

  private def handleLnk(path: String, name: String, stat: Linux.StatInfo): Hash = {
    var target = Linux.readlink(path)
    Attributes.ofLinuxStat(stat + ("target" -> target), name).store(pool)
  }
  handlers += ("LNK" -> handleLnk _)

  private def handleReg(path: String, name: String, stat: Linux.StatInfo): Hash = {
    var dataHash = FileData.store(pool, path)
    Attributes.ofLinuxStat(stat + ("data" -> dataHash.toString), name).store(pool)
  }
  handlers += ("REG" -> handleReg _)

  private def handleSimple(path: String, name: String, stat: Linux.StatInfo): Hash =
    Attributes.ofLinuxStat(stat, name).store(pool)
  handlers += ("CHR" -> handleSimple)
  handlers += ("BLK" -> handleSimple)
  handlers += ("FIFO" -> handleSimple)
  handlers += ("SOCK" -> handleSimple)

  private def byInode(a: (_, Long), b: (_, Long)) = a._2 < b._2
  private def byName(a: (String, _), b: (String, _)) = a._1 < b._1
}
