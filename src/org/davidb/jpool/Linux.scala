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

  private def readdirError(path: String, errno: Int): Nothing =
    throw new IOException("Error reading directory: '%s' (%d)" format (path, errno))
  private def lstatError(path: String, errno: Int): Nothing =
    throw new IOException("Error statting node: '%s' (%d)" format (path, errno))
  private def readlinkError(path: String, errno: Int): Nothing =
    throw new IOException("Error readlink: '%s' (%d)" format (path, errno))
  private def symlinkError(oldPath: String, newPath: String, errno: Int): Nothing =
    throw new IOException("Error making symlink from '%s' to '%s' (%d)"
      format (oldPath, newPath, errno))
  private def openError(path: String, errno: Int): Nothing =
    throw new IOException("Error opening file: '%s' (%d)" format (path, errno))
  private def readError(path: String, errno: Int): Nothing =
    throw new IOException("Error reading file '%s' (%d)"
      format (path, errno))
  private def writeError(errno: Int): Nothing =
    throw new IOException("Error writing file (%d)" format errno)
  private def closeError(errno: Int): Nothing =
    throw new IOException("Error closing file (%d)" format errno)

  System.loadLibrary("linux")
  setup
}
