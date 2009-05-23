//////////////////////////////////////////////////////////////////////
// Test of native interface.

package org.davidb.jpool

import java.nio.ByteBuffer
import java.io.IOException
import scala.collection.immutable

object Linux {
  @native def setup

  @native def message: String

  @native def readDir(name: String): List[(String, Long)]

  @native def lstat(name: String): Map[String, String]

  @native def readlink(name: String): String

  @native def symlink(oldPath: String, newPath: String)

  // Bulk read.  Reads the contents of the given file, in blocks of
  // the given chunks size, and calls 'process' with a fresh
  // ByteBuffer for each chunk.
  @native def readFile(path: String, chunkSize: Int, process: ByteBuffer => Unit)

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

  @native protected def openForWrite(path: String): Int
  @native protected def close(fd: Int)
  @native protected def writeChunk(fd: Int, data: Array[Byte], offset: Int, length: Int)

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

  class NativeError(val name: String, val path: String, val errno: Int, message: String)
    extends IOException(message)
  def throwNativeError(name: String, path: String, errno: Int): Nothing = {
    val message =
      if (path == null)
        "native error in '%s' (%d:%s)" format (name, errno, strerror(errno))
      else
        "native error in '%s' accessing '%s' (%d:%s)" format
          (name, path, errno, strerror(errno))
    throw new NativeError(name, path, errno, message)
  }

  System.loadLibrary("linux")
  setup
}
