// Storage of data in a "pool" consisting of a set of pool files.

package org.davidb.jpool
package pool

import scala.collection.mutable
import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer

class FilePool(prefix: File) extends ChunkStore {
  if (!prefix.isDirectory())
    error("Pool name '" + prefix + "' must be a directory")

  // Default initial limit for pool datafiles.  Any write that would
  // exceed this value will cause a new file to be written.  Changing
  // this on existing pools doesn't affect the size of files already
  // written.

  private val metaPrefix = new File(prefix, "metadata")
  sanityTest
  private val lock = lockPool()
  metaCheck
  private var keyInfo = findKeyInfo()
  private val db = new PoolDb(metaPrefix)
  private val files = scan

  val seenPrefix = new File(prefix, "seen").getAbsolutePath

  private final val defaultLimit = 640*1024*1024
  private var _limit = db.getProperty("limit", defaultLimit.toString).toInt

  // A property called 'newfile=true' will force the first write to
  // create a new file.
  private var forceNew = db.getProperty("newfile", "false").toBoolean

  def limit: Int = _limit
  def limit_=(value: Int) {
    require(value > 0)
    db.setProperty("limit", value.toString)
    _limit = value
  }

  def getBackups: Set[Hash] = db.getBackups

  def get(hash: Hash): Option[Chunk] = {
    hashLookup(hash).map(res => res._1.pool.read(res._2))
  }

  private def hashLookup(hash: Hash): Option[(FileAndIndex, Int)] = {
    var num = 0
    while (num < files.length) {
      val fi = files(num)
      val result = fi.index.get(hash)
      if (result.isDefined)
        return Some((fi, result.get._1))
      num += 1
    }
    None
  }

  // No need to read the data for just a containment check.
  // TODO: Remember the last Hash lookup for get/contains and reuse
  // it.
  override def contains(hash: Hash): Boolean = hashLookup(hash).isDefined

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
    if (!contains(key)) {
      progressMeter.addData(value.dataLength)
      needRoom(value)
      val fileNum = files.size - 1
      val file = files(fileNum)
      val pos = file.pool.append(value)
      file.index += (key -> (pos, value.kind))

      if (value.kind == "back")
        db.addBackup(key)
    } else {
      progressMeter.addDup(value.dataLength)
    }
    this
  }

  class FileAndIndex(val pool: PoolFileBase, val index: FileIndex)

  // Scan the pool directory for the pool files.
  private def scan: mutable.ArrayBuffer[FileAndIndex] = {
    val Name = """^pool-data-(\d{4})\.data$""".r
    val names = prefix.list()
    util.Sorting.quickSort(names)
    val nums = for (Name(num) <- names) yield num.toInt
    sequenceCheck(nums)
    val files = nums map ((n: Int) => makePoolFile(n))
    val buf = new mutable.ArrayBuffer[FileAndIndex]()
    buf ++= files
    buf
  }

  private def lockPool() = {
    val lockFile = new File(prefix, "lock")
    new RandomAccessFile(lockFile, "rw").getChannel.lock()
  }

  private var closed = false
  override protected def finalize() = synchronized {
    if (!closed) {
      lock.release()
      flush()
      db.close()
      closed = true
    }
  }

  def close() = finalize()

  def flush() {
    for (fi <- files) {
      fi.index.flush()
    }
  }

  // Ensure that we can write to the last file.  Make sure it exists,
  // and has room.
  private def needRoom(chunk: Chunk) {
    // TODO: Check for growth.
    if (files.size == 0) {
      files += makePoolFile(files.size)
    } else {
      val file = files(files.size - 1)
      if (file.pool.size + chunk.writeSize > limit || forceNew) {
        file.pool.close
        files += makePoolFile(files.size)
      }
    }
    forceNew = false
  }

  private def makeName(index: Int): File = {
    new File(prefix, "pool-data-%04d.data" format index)
  }

  private def makePoolFile(index: Int): FileAndIndex = {
    val pool = keyInfo match {
      case None =>
        new PoolFile(makeName(index))
      case Some(info) =>
        new EncryptedPoolFile(makeName(index), info)
    }
    new FileAndIndex(pool, new FileIndex(pool))
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
    val safe = Set.empty ++ Array("metadata", "backup.crt", "backup.key", "lock")
    if (!names.forall(safe contains _) ||
      ((names contains "metadata") &&
        !new File(prefix, "metadata").isDirectory))
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

  private def findKeyInfo(): Option[crypto.RSAInfo] = {
    val cert = new File(prefix, "backup.crt")
    val key = new File(prefix, "backup.key")

    if (cert.isFile()) {
      if (key.isFile()) {
        val info = crypto.RSAInfo.loadPair(cert, key, new crypto.JavaConsolePinReader)
        // Note that we don't verify them, since that would require
        // the password.
        Some(info)
      } else
        Some(crypto.RSAInfo.loadCert(cert))
    } else {
      if (key.isFile())
        error("backup.key is present, but backup.crt is not.")
      None
    }
  }

  // Generate a key for this pool.  The pool must not have any pool
  // files written to it yet.
  def makeKeyPair() {
    if (files.size != 0)
      error("Cannot create key once pool contains data.")
    if (keyInfo != None)
      error("Pool already has a key.")

    val info = crypto.RSAInfo.generate()
    info.saveCert(new File(prefix, "backup.crt"))
    info.savePrivate(new File(prefix, "backup.key"), new crypto.JavaConsolePinReader)
    keyInfo = Some(info)
  }
}
