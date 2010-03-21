//////////////////////////////////////////////////////////////////////
// Test of native interface.

package org.davidb.jpool

import java.nio.ByteBuffer
import java.io.IOException
import scala.collection.immutable

import org.davidb.jpool.pool.Attributes

import org.davidb.logging.Logger

object Linux extends AnyRef with Logger {
  @native def setup

  @native def message: String

  @native def readDir(name: String): List[(String, Long)]

  type StatInfo = Map[String, String]
  @native def lstat(name: String): StatInfo
  @native def stat(name: String): StatInfo

  @native def readlink(name: String): String

  @native def symlink(oldPath: String, newPath: String)
  @native def link(oldPath: String, newPath: String)
  @native def mkdir(name: String)

  // Bulk read.  Reads the contents of the given file, in blocks of
  // the given chunks size, and calls 'process' with a fresh
  // ByteBuffer for each chunk.
  def readFile(path: String, chunkSize: Int, process: ByteBuffer => Unit) {
    val fd = openForRead(path)
    try {
      def loop {
        val buf = ByteBuffer.allocate(chunkSize)
        val count = readChunk(fd, buf.array, buf.arrayOffset + buf.position,
          buf.remaining)
        if (count > 0) {
          buf.limit(count)
          process(buf)
        }
        if (count == chunkSize)
          loop
      }
      loop
    } finally {
      close(fd)
    }
  }

  // Bulk write.  The iterator should return ByteBuffers, the contents
  // of which will be written to the file.
  def writeFile(path: String, chunks: Iterator[ByteBuffer]) {
    val fd = openForWrite(path)
    try {
      for (chunk <- chunks) {
        writeChunk(fd, chunk.array, chunk.arrayOffset + chunk.position,
          chunk.remaining)
      }
    } finally {
      close(fd)
    }
  }

  // TODO: It would be more efficient to set attributes using the FD
  // from writeFile instead of the named mode sets.  Realistically, it
  // probably doesn't matter that much.

  // Restore stat information on a given node.  For 'REG' regular
  // files, this should be done after the data has been written to the
  // file (using writeFile), and for 'DIR' directories, this should be
  // called after all of the contents of the directory are written.
  // For all other node types, this call will create the node in
  // question.
  def restoreStat(path: String, stat: Attributes) {
    if (stat.kind == "REG" || stat.kind == "DIR") {
      if (isRoot) {
        chown(path, stat("uid").toLong, stat("gid").toLong)
      }
      chmod(path, stat("mode").toLong)
      val mtime = stat("mtime").toLong
      utime(path, mtime, mtime)
    } else if (stat.kind == "LNK") {
      // There is no lchmod on Linux, but the umask is used in the
      // link permissions (they don't actually get used for anything).
      withUmask(stat("mode").toInt & 4095) {
        symlink(stat("target"), path)
      }
      if (isRoot)
        lchown(path, stat("uid").toLong, stat("gid").toLong)
    } else if (specialNode(stat.kind)) {
      val isDev = stat.contains("rdev")
      if (isDev && !isRoot) {
        warn("Cannot restore device as non-root: %s", path)
        return
      }
      val dev = if (isDev) stat("rdev").toLong else 0L
      withUmask(0) {
        makeSpecial(path, stat.kind, stat("mode").toLong, dev)
      }
      if (isRoot) {
        chown(path, stat("uid").toLong, stat("gid").toLong)
      }
      val mtime = stat("mtime").toLong
      utime(path, mtime, mtime)
    } else {
      warn("TODO: Restore of %s: %s", stat.kind, path)
    }
  }

  private val specialNode = Set[String]("CHR", "BLK", "FIFO", "SOCK")

  private var createOp = Map[String, (String, StatInfo) => Unit]()
  createOp += ("BLK" -> ((path, stat) => {
      if (isRoot) {
      } else
        warn("Cannot restore block device as non-root: %s", path)
    }))

  // Are we running as root?
  lazy val isRoot: Boolean = geteuid() == 0

  // Perform 'thunk' with the specified umask.
  private def withUmask(mask: Int)(op: => Unit) {
    val oldMask = umask(mask)
    try {
      op
    } finally {
      umask(oldMask)
    }
  }

  // Perform a stat operation "safely", requiring certain keys to be
  // present, otherwise warning that the necessary keys are missing.

  @native protected def openForWrite(path: String): Int
  @native protected def openForRead(path: String): Int
  @native protected def close(fd: Int)
  @native protected def writeChunk(fd: Int, data: Array[Byte], offset: Int, length: Int)
  @native protected def readChunk(fd: Int, data: Array[Byte], offset: Int, length: Int): Int

  @native protected def geteuid(): Int
  @native protected def umask(mask: Int): Int

  @native protected def chmod(path: String, mode: Long)
  @native protected def chown(path: String, uid: Long, gid: Long)
  @native protected def lchown(path: String, uid: Long, gid: Long)
  @native protected def utime(path: String, atime: Long, mtime: Long)

  @native protected def makeSpecial(path: String, kind: String, mode: Long, dev: Long)

  // These wrappers keep the JNI code less dependent on the
  // implementation details of the Scala runtime.
  private def makePair(name: String, inum: Long) = (name, inum)

  // For the mapping, these functions provide the zero and mplus
  // operations.
  private def infoZero = immutable.Map.empty
  private def infoPlus(prior: immutable.Map[String, String], key: String, value: String):
    immutable.Map[String, String] =
  {
    prior + (key -> value)
  }

  @native protected def strerror(errnum: Int): String

  def throwNativeError(name: String, path: String, errno: Int): Nothing = {
    val message =
      if (path == null)
        "native error in '%s' (%d:%s)" format (name, errno, strerror(errno))
      else
        "native error in '%s' accessing '%s' (%d:%s)" format
          (name, path, errno, strerror(errno))
    throw new NativeError(name, path, errno, message)
  }

  // If the test framework passed in a specific library, use it.
  val libname = System.getProperty("linux.lib")
  if (libname ne null)
    System.load(libname)
  else
    System.loadLibrary("linux")
  setup
}
