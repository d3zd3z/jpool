// Storage of data in a "pool" consisting of a set of pool files.

package org.davidb.jpool.pool

import org.davidb.jpool._
import scala.collection.mutable
import java.io.File
import java.nio.ByteBuffer

class PoolHashIndex(val basePath: String, val prefix: String) extends {
  protected val encoder = new FixedEncodable[(Int,Int)] {
    def EBytes = 8
    def encode(obj: (Int,Int), buf: ByteBuffer) {
      buf.putInt(obj._1)
      buf.putInt(obj._2)
    }
    def decode(buf: ByteBuffer): (Int,Int) = {
      val file = buf.getInt()
      val offset = buf.getInt()
      (file, offset)
    }
  }
} with HashIndex[(Int,Int)]

class FilePool(prefix: File) extends ChunkStore {
  if (!prefix.isDirectory())
    error("Pool name '" + prefix + "' must be a directory")

  // Default initial limit for pool datafiles.  Any write that would
  // exceed this value will cause a new file to be written.  Changing
  // this on existing pools doesn't affect the size of files already
  // written.

  private val metaPrefix = new File(prefix, "metadata")
  sanityTest
  metaCheck
  private val db = new PoolDb(metaPrefix)
  private val files = scan
  private val hashIndex = new PoolHashIndex(metaPrefix.getPath, "data-index-")
  recover

  val seenPrefix = new File(prefix, "seen").getAbsolutePath

  private final val defaultLimit = 640*1024*1024
  private var _limit = db.getProperty("limit", defaultLimit.toString).toInt

  def limit: Int = _limit
  def limit_=(value: Int) {
    require(value > 0)
    db.setProperty("limit", value.toString)
    _limit = value
  }

  def getBackups: Set[Hash] = db.getBackups

  def get(hash: Hash): Option[Chunk] = {
    hashIndex.get(hash) match {
      case None => None
      case Some((file, offset)) => Some(files(file).read(offset))
    }
  }

  // No need to read the data for just a containment check.
  // TODO: Remember the last Hash lookup for get/contains and reuse
  // it.
  override def contains(hash: Hash): Boolean = hashIndex.contains(hash)

  def iterator: Iterator[(Hash, Chunk)] = error("TODO")

  def -= (key: Hash) =
    throw new UnsupportedOperationException("Pools only support adding, not removal")

  var progressMeter: DataProgress = NullProgress
  def setProgress(meter: DataProgress) {
    progressMeter = meter
  }

  def += (kv: (Hash, Chunk)): this.type = {
    val (key, value) = kv
    require(key == value.hash)
    if (!hashIndex.contains(key)) {
      progressMeter.addData(value.dataLength)
      needRoom(value)
      val fileNum = files.size - 1
      val file = files(fileNum)
      val pos = file.append(value)
      hashIndex += (key -> (fileNum, pos))

      hashIndex.setProperty("pool.fileSeen." + fileNum, file.size.toString)

      if (value.kind == "back")
        db.addBackup(key)
    } else {
      progressMeter.addDup(value.dataLength)
    }
    this
  }

  // Scan the pool directory for the pool files.
  private def scan: mutable.ArrayBuffer[PoolFile] = {
    val Name = """^pool-data-(\d{4})\.data$""".r
    val names = prefix.list()
    util.Sorting.quickSort(names)
    val nums = for (Name(num) <- names) yield num.toInt
    sequenceCheck(nums)
    val files = nums map ((n: Int) => new PoolFile(makeName(n)))
    val buf = new mutable.ArrayBuffer[PoolFile]()
    buf ++= files
    buf
  }

  def close() {
    flush()
    // TODO: Better invalidate our own state.
    db.close()
  }

  def flush() {
    hashIndex.flush()
  }

  // Ensure that we can write to the last file.  Make sure it exists,
  // and has room.
  private def needRoom(chunk: Chunk) {
    // TODO: Check for growth.
    if (files.size == 0) {
      files += new PoolFile(makeName(files.size))
    } else {
      val file = files(files.size - 1)
      if (file.size + chunk.writeSize > limit) {
        file.close
        files += new PoolFile(makeName(files.size))
      }
    }
  }

  private def makeName(index: Int): File = {
    new File(prefix, "pool-data-%04d.data" format index)
  }

  // Verify that there aren't any pool files missing, or other
  // oddities.
  private def sequenceCheck(nums: Iterable[Int]) {
    var pos = 0
    for (n <- nums) {
      if (n != pos)
        error("Out of sequence pool file: " + makeName(n))
      pos += 1
    }
  }

  // Pool directory sanity test.  Make sure this directory appears as
  // a reasonably sane pool.  It should either contain at least one
  // pool data file, contain only metadata, or be entirely empty.
  private def sanityTest {
    val names = prefix.list()
    if (names.size == 0)
      return
    if (names contains "pool-data-0000.data")
      return
    if (names.size == 1 && names(0) == "metadata" &&
        new File(prefix, "metadata").isDirectory)
      return
    error("Pool directory %s is not a valid pool, but is not empty"
      format prefix.getPath)
  }

  // Ensure there is a metadata directory.
  private def metaCheck {
    if (!metaPrefix.isDirectory) {
      if (!metaPrefix.mkdir())
        error("Unable to create metadata dir: %s" format metaPrefix)
    }
  }

  protected def recoveryNotify {}

  // Walk through the pool files, recovering the index if necessary.
  private def recover {
    var dirty = false
    for (i <- 0 until files.size) {
      val pf = files(i)
      val size = pf.size
      var current = hashIndex.getProperty("pool.fileSeen." + i, "0").toInt
      if (current < size) {
        if (!dirty) {
          /*log.info*/println("Performing index recovery on pool %s" format prefix)
          dirty = true
        }
        /*log.info*/println("Indexing file %s from %d to %d" format (i, current, size))
        while (current < size) {
          val ch = pf.read(current)
          if (!hashIndex.contains(ch.hash))
            hashIndex += (ch.hash -> (i, current))
            // printf("Add hash: %s (%d,%d)%n", ch, i, current)
          if (ch.kind == "back") {
            /*log.info*/printf("Adding backup: %s%n", ch.hash)
            db.addBackup(ch.hash)
          }
          current = pf.position
        }

        hashIndex.setProperty("pool.fileSeen." + i, size.toString)
      }
      pf.close()
    }

    if (dirty) {
      hashIndex.flush()
      try {
        recoveryNotify
      } catch {
        case e: Throwable =>
          db.close()
          throw e
      }
    }
  }
}
