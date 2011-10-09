/*
 * Pool file index.
 *
 * Each pool file has an associated index file that maps the offets of
 * each hash found in that file.
 */

package org.davidb.jpool
package pool

import scala.collection.{mutable, immutable}

import org.davidb.logging.Loggable

import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.nio.channels.FileChannel
import java.nio.ByteBuffer
import java.nio.ByteOrder

class FileIndex(pfile: PoolFileBase) extends mutable.Map[Hash, FileIndex.Elt] with Loggable {
  protected val indexPath = getIndexName
  protected val index = {
    try {
      new FileIndexFile(indexPath, pfile.size)
    } catch {
      case _: FileIndexFile.PoolSizeMismatch |
           _: java.io.FileNotFoundException =>
        reIndexFile()
        new FileIndexFile(indexPath, pfile.size)
    }
  }

  // Updates go into the RAM index.
  protected var ramIndex = Map[Hash, FileIndex.Elt]()

  // Index operations.
  def get(key: Hash): Option[FileIndex.Elt] = {
    index.get(key).orElse(ramIndex.get(key))
  }
  def -= (key: Hash): this.type = sys.error("Cannot remove from index")
  def += (kv: (Hash, FileIndex.Elt)): this.type = {
    ramIndex += kv
    this
  }
  def iterator: Iterator[(Hash, FileIndex.Elt)] = {
    index.iterator ++ ramIndex.iterator
  }

  // Write any updates to the index.  Safely does nothing if there is
  // nothing to do.
  def flush() {
    if (ramIndex.size == 0)
      return

    FileIndexFile.writeIndex(indexPath, pfile.size, this)

    // TODO: Perhaps we should be able to update ourself with the new
    // data, but that clashes with this being mutable.
  }

  // Regenerate the index of the entire file, if it is out of date or
  // stale.
  private def reIndexFile() {
    logger.info("Reindexing '%s'".format(pfile.path.getPath()))

    var items = immutable.TreeMap[Hash, FileIndex.Elt]()
    var pos = 0
    val len = pfile.size
    while (pos < len) {
      val (hash, kind, size) = pfile.readInfo(pos)
      items += (hash -> FileIndex.Elt(pos, kind))
      pos = pfile.position
    }
    FileIndexFile.writeIndex(indexPath, pfile.size, items)
    logger.info("Done")
  }

  private def getIndexName: File = {
    val base = pfile.path.getPath();
    val name = if (base.endsWith(".data")) {
      base.substring(0, base.length - 5) + ".idx"
    } else {
      base + ".idx"
    }
    new File(name)
  }
}

// This helper class is for reading/writing the FileIndex.
class FileIndexFile(path: File, poolSize: Int) extends immutable.Map[Hash, FileIndex.Elt] {
  private val (top, hashPos, offsetPos, kindPos, buffer) = readIndex()
  override def size = top(255)

  // Lookup this hash.
  def get(key: Hash): Option[FileIndex.Elt] = {
    val topByte = key.byte(0)
    var low = if (topByte > 0) top(topByte-1) else 0
    var high = top(topByte) - 1

    while (high >= low) {
      val mid = low + ((high - low) >>> 1)
      val comp = hashes(mid).compare(key)
      if (comp > 0)
	high = mid - 1
      else if (comp < 0)
	low = mid + 1
      else {
	return Some(FileIndex.Elt(offsets(mid), kinds(mid)))
      }
    }
    return None
  }

  private def hashes(pos: Int): Hash = {
    val buf = buffer.duplicate()
    buf.position(hashPos + Hash.HashLength * pos)
    Hash.raw(buf)
  }

  private def offsets(pos: Int): Int = {
    val buf = buffer.duplicate()
    buf.position(offsetPos + 4 * pos)
    buf.order(ByteOrder.LITTLE_ENDIAN)
    buf.getInt()
  }

  private def kinds(pos: Int): String = {
    val buf = buffer.duplicate()
    buf.position(kindPos + 4 * pos)
    new String(FileUtil.getBytes(buf, 4), "UTF-8").intern()
  }

  private def readIndex(): (Array[Int], Int, Int, Int, ByteBuffer) = {
    // Map the file into memory.
    val fis = new FileInputStream(path)
    val chan = fis.getChannel()
    val buf = chan.map(FileChannel.MapMode.READ_ONLY, 0, chan.size)
    buf.load()
    buf.order(ByteOrder.LITTLE_ENDIAN)
    fis.close()

    val magic = FileUtil.getBytes(buf, 8)
    if (!java.util.Arrays.equals(magic, FileIndex.magic))
      sys.error("Invalid index magic header")

    val version = buf.getInt()
    if (version != 3)
      sys.error("Unsupported index version")

    val psize = buf.getInt()
    if (psize != poolSize) {
      throw new FileIndexFile.PoolSizeMismatch("Index for %d bytes, pool is %d bytes".format(psize, poolSize))
    }

    val top = new Array[Int](256)
    for (i <- 0 until 256) {
      top(i) = buf.getInt()
    }
    val size = top(255)

    val hashPos = buf.position
    val offsetPos = hashPos + Hash.HashLength * size
    val kindPos = offsetPos + 4 * size

    (top, hashPos, offsetPos, kindPos, buf)
  }

  // The index cannot be updated, so these are just failures.
  def + [B1 >: FileIndex.Elt](kv: (Hash, B1)) = sys.error("Not mutable")
  def - (key: Hash) = sys.error("Not mutable")

  def iterator: Iterator[(Hash, FileIndex.Elt)] = {
    (0 until size).iterator.map { i: Int =>
      (hashes(i), FileIndex.Elt(offsets(i), kinds(i)))
    }
  }
}

object FileIndexFile {

  class PoolSizeMismatch(message: String) extends Exception(message)

  case class Node(hash: Hash, offset: Int, kind: String)

  def writeIndex(path: File, poolSize: Int, items: Iterable[(Hash, FileIndex.Elt)]) {

    val size = items.size
    val allBuf = new mutable.ArrayBuffer[Node](size)
    for ((hash, FileIndex.Elt(pos, kind)) <- items) {
      allBuf += Node(hash, pos, kind.intern())
    }
    val all = util.Sorting.stableSort(allBuf.result(), (x: Node) => x.hash)

    // util.Sorting.stableSort(all, (a: Node, b: Node) => a.hash < b.hash)

    val tmpName = new File(path.getPath + ".tmp")
    val fos = new FileOutputStream(tmpName)
    val chan = fos.getChannel()

    val header = ByteBuffer.allocate(16)
    header.order(ByteOrder.LITTLE_ENDIAN)
    header.put(FileIndex.magic)
    header.putInt(3)
    header.putInt(poolSize)
    header.flip()
    FileUtil.fullWrite(chan, header)

    // Top buffer.
    val top = ByteBuffer.allocate(1024)
    top.order(ByteOrder.LITTLE_ENDIAN)
    var offset = 0
    for (first <- 0 until 256) {
      // Write the first OID that is larger than the given index.
      while (offset < size && first >= (all(offset).hash.byte(0).toInt & 0xFF))
        offset += 1
      top.putInt(offset)
    }
    top.flip()
    FileUtil.fullWrite(chan, top)

    // Write the hashes themselves.
    val hashBuf = ByteBuffer.allocate(size * Hash.HashLength)
    for (node <- all) {
      node.hash.putTo(hashBuf)
    }
    hashBuf.flip()
    FileUtil.fullWrite(chan, hashBuf)

    // Write the offset table.
    val offsetBuf = ByteBuffer.allocate(size * 4)
    offsetBuf.order(ByteOrder.LITTLE_ENDIAN)
    for (node <- all) {
      offsetBuf.putInt(node.offset)
    }
    offsetBuf.flip()
    FileUtil.fullWrite(chan, offsetBuf)

    // Followed by the expanded kind table.
    // TODO: Future version could compress this kind table a lot, by
    // just storing references to interned strings.
    val kindBuf = ByteBuffer.allocate(size * 4)
    for (node <- all) {
      kindBuf.put(node.kind.getBytes("UTF-8"))
    }
    kindBuf.flip()
    FileUtil.fullWrite(chan, kindBuf)

    chan.force(false)
    fos.close()

    tmpName.renameTo(path)
  }
}

object FileIndex {

  case class Elt(offset: Int, kind: String)

  def main(args: Array[String]) {
    // val base = "/mnt/grime/pro/pool/npool"
    // val pfile = new PoolFile(new File(base + "/pool-data-0000.data"))
    // new FileIndex(pfile)

    testWrite()
  }

  def testWrite() {
    var items = immutable.TreeMap[Hash, FileIndex.Elt]()
    val limit = 1000
    for (i <- 1 to limit) {
      val hash = Hash("blob", i.toString)
      items += (hash -> FileIndex.Elt(i, "blob"))
    }

    FileIndexFile.writeIndex(new File("index.idx"), 0x12345678, items)

    printf("Loading new index\n")
    val index = new FileIndexFile(new File("index.idx"), 0x12345678)
    for (i <- 1 to limit) {
      val hash = Hash("blob", i.toString)
      index.get(hash) match {
	case None => sys.error("Key not found")
	case Some(FileIndex.Elt(pos, kind)) =>
	  assert(pos == i)
	  assert(kind == "blob")
      }

      // Mangle the hash to make sure a different one isn't present.
      val bytes = hash.getBytes
      bytes(bytes.length - 1) = (bytes(bytes.length - 1) ^ 1).toByte
      val h2 = Hash.raw(bytes)
      assert(index.get(h2) == None)
    }
  }

  protected[pool] val magic = "ldumpidx".getBytes("UTF-8")
}
