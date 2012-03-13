/**********************************************************************/
// Saving and restoring tree data.

package org.davidb.jpool
package pool

import scala.collection.mutable.ListBuffer
import org.davidb.logging.Loggable
import java.util.Properties

import org.apache.commons.codec.net.URLCodec

object TreeSave {
  lazy val devMap = new DevMap
}

class TreeSave(pool: ChunkStore, rootPath: String, meter: BackupProgressMeter) extends AnyRef with Loggable {
  // Store the item, of whatever type it is, into the given storage
  // pool, returning the hash needed to retrieve it later.  If unable
  // to save the item, the exception will be propagated.  Directories
  // will warn, but will save as much as possible.
  // Writes the properties into the 'back' record, adding a 'hash'
  // property to the root of this backup, and a 'kind=snapshot'
  // property to indicate that this is a tree snapshot.
  def store(props: Properties): Hash = {
    val treeHash = internalStore(rootPath, rootStat)
    props.setProperty("hash", treeHash.toString)
    props.setProperty("kind", "snapshot")
    val back = new Back(props).store(pool)
    seenDb.close()
    back
  }

  // Internal store, where we've already statted the nodes (avoids
  // duplicate stats, since directory traversal requires statting).
  private def internalStore(path: String, stat: Linux.StatInfo): Hash = {
    val hash = handlers.get(stat("*kind*")) match {
      case None =>
        logger.error("Unknown filesystem entry kind: %s (%s)" format (stat("*kind*"), path))
        sys.error("Cannot dump entry")
      case Some(handler) => handler(path, stat)
    }
    meter.addNode()
    hash
  }

  private var handlers = Map[String, (String, Linux.StatInfo) => Hash]()

  private val rootStat = Linux.lstat(rootPath)
  if (rootStat("*kind*") != "DIR") {
    logger.error("Root of backup must be a directory: %s" format rootPath)
    sys.error("Cannot save")
  }

  val devUuid =
    try {
      TreeSave.devMap(rootStat("dev").toLong)
    } catch {
      case e: java.util.NoSuchElementException =>
        logger.warn("Unable to determine UUID of filesystem, using mountpoint to generate UUID.")
        "dev-" + (new URLCodec).encode(rootPath)
    }
  val seenPrefix = pool match {
    case fp: FilePool => fp.seenPrefix
    case _ => sys.error("TODO: Unable to save to a non-local file pool")
  }
  val seenDb = new SeenDb(seenPrefix, devUuid)
  seenDb.purge()

  private def seenNodeToMapping(node: SeenNode): (Long, SeenNode) = (node.inode, node)

  private def handleDir(path: String, stat: Linux.StatInfo): Hash = {
    var nstats = new ListBuffer[(String, Linux.StatInfo)]

    val myIno = stat("ino").toLong
    val previous = Map.apply(seenDb.getFiles(myIno).map(seenNodeToMapping _) : _*)
    val updated = new ListBuffer[SeenNode]

    // Iterate over the names sorted by inode number, statting each
    // entry.  Don't descend directories that cross device boundaries.
    if (stat("dev") == rootStat("dev")) {
      for ((name, _) <- Linux.readDir(path).sortWith(byInode _)) {
        try {
          val stat = Linux.lstat(path + "/" + name)
          nstats += ((name, stat))
        } catch {
          case e: NativeError =>
            logger.warn("Unable to stat file, skipping: %s" format path)
        }
      }
    }

    // Sort the results by name.  The sort helps repeated backups of
    // unchanged directories to keep the same hash.
    val items = nstats.toList.sortWith(byName _)

    val builder = new DirStore(pool, 256*1024)

    for ((name, childStat) <- items) {
      val fullName = path + "/" + name
      val childIno = childStat("ino").toLong
      // TODO: The seen database only stores the integer portion of
      // the timestamp.
      val (childCtime, _) = Linux.decodeTime(childStat("ctime"))

      // See if this has already been seen.
      previous.get(childIno) match {
        case Some(node) if node.ctime == childCtime && pool.contains(node.hash) =>
          builder.append(name, node.hash)
          updated += node
          meter.addSkip(childStat("size").toLong)
          meter.addNode()
        case _ =>
          try {
            val childHash = internalStore(fullName, childStat)
            builder.append(name, childHash)
            if (childStat("*kind*") == "REG")
              updated += new SeenNode(childIno, childCtime, childHash)
          } catch {
            case e: NativeError =>
              logger.warn("Unable to backup node, skipping: %s" format fullName)
          }
      }
    }

    seenDb.update(myIno, updated.toList)

    val children = builder.finish()

    var fullStat = stat
    // fullStat += ("path" -> path)
    fullStat += ("children" -> children.toString())

    Attributes.ofLinuxStat(fullStat).store(pool, "node")
  }
  handlers += ("DIR" -> handleDir _)

  private def handleLnk(path: String, stat: Linux.StatInfo): Hash = {
    var target = Linux.readlink(path)
    Attributes.ofLinuxStat(stat + ("target" -> target)).store(pool, "node")
  }
  handlers += ("LNK" -> handleLnk _)

  private def handleReg(path: String, stat: Linux.StatInfo): Hash = {
    var dataHash = FileData.store(pool, path)
    Attributes.ofLinuxStat(stat + ("data" -> dataHash.toString)).store(pool, "node")
  }
  handlers += ("REG" -> handleReg _)

  private def handleSimple(path: String, stat: Linux.StatInfo): Hash =
    Attributes.ofLinuxStat(stat).store(pool, "node")
  handlers += ("CHR" -> handleSimple)
  handlers += ("BLK" -> handleSimple)
  handlers += ("FIFO" -> handleSimple)
  handlers += ("SOCK" -> handleSimple)

  private def byInode(a: (_, Long), b: (_, Long)) = a._2 < b._2
  private def byName(a: (String, _), b: (String, _)) = a._1 < b._1
}
