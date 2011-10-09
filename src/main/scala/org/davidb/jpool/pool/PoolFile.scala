/**********************************************************************/
// A single file holding a bunch of chunks in the storage pool.

package org.davidb.jpool
package pool

import java.io.{File, RandomAccessFile}
import java.nio.channels.FileChannel

abstract class PoolFileBase(val path: File) {
  var state: State = new ClosedState

  abstract class State {
    def getReadable(): FileChannel
    def getWritable(): FileChannel
    def close(): Unit
  }
  class ClosedState extends State {
    def close() {}
    def getReadable(): FileChannel = {
      state = new ReadState
      state.getReadable()
    }
    def getWritable(): FileChannel = {
      state = new WriteState
      state.getWritable()
    }
  }
  class ReadState extends State {
    val file = new RandomAccessFile(path, "r")
    val chan = file.getChannel()
    def getReadable(): FileChannel = chan
    def getWritable(): FileChannel = {
      chan.close()
      state = new WriteState
      state.getWritable()
    }
    def close() {
      chan.close()
      state = new ClosedState
    }
  }
  class WriteState extends State {
    val file = new RandomAccessFile(path, "rw")
    val chan = file.getChannel()
    def getReadable(): FileChannel = chan
    def getWritable(): FileChannel = chan
    def close() {
      chan.close()
      state = new ClosedState
    }
  }

  def size: Int = {
    try {
      state.getReadable().size.toInt
    } catch {
      // Non-existent files are considered to have no content.  Needed
      // because creation will attempt to index the file before
      // writing to it.
      case _: java.io.FileNotFoundException => 0
    }
  }
  def position: Int = state.getReadable().position.toInt

  def read(pos: Int): Chunk
  def readUnchecked(pos: Int): (Chunk, Hash)
  def readInfo(pos: Int): (Hash, String, Int)
  def append(chunk: Chunk): Int

  def close() = state.close()
}

class PoolFile(path: File) extends PoolFileBase(path) {

  def read(pos: Int): Chunk = {
    val chan = state.getReadable()
    chan.position(pos)
    Chunk.read(chan)
  }
  def readUnchecked(pos: Int): (Chunk, Hash) = {
    val chan = state.getReadable()
    chan.position(pos)
    Chunk.readUnchecked(chan)
  }

  def readInfo(pos: Int): (Hash, String, Int) = {
    val chan = state.getReadable()
    chan.position(pos)
    Chunk.readInfo(chan)
  }

  def append(chunk: Chunk): Int = {
    val chan = state.getWritable()
    val pos = chan.size.toInt
    chan.position(pos)
    chunk.write(chan)
    pos
  }
}
