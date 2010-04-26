//////////////////////////////////////////////////////////////////////
// Storage of tar files into the storage pool.

package org.davidb.jpool
package pool

import java.io.ByteArrayOutputStream
import java.nio.channels.{ReadableByteChannel}
import java.nio.ByteBuffer
import java.util.Properties

class TarSave(pool: ChunkStore, chan: ReadableByteChannel) {

  private var tars = TreeBuilder.makeBuilder("tar", pool)
  private var tar = new TarParser(chan)
  encodeEntry
  val subHash = tars.finish()

  // Encode a 'back' record, storing the given properites.  Note that
  // the properties will also have the 'hash' property set to the hash
  // of the root of this backup.
  def store(props: Properties): Hash = {
    props.setProperty("hash", subHash.toString)
    val result = new Back(props).store(pool)
    result
  }

  private def encodeEntry {
    tar.getHeader match {
      case None =>
        // Read the second block of nulls that indicates EOF.
        // Not sure how much this is required.
        /*
        tar.getHeader match {
          case None => // Real EOF
          case Some(head) =>
            Pdump.dump(head.raw)
            error("Improper EOF seen on tarfile")
        }
        */
      case Some(head) =>
        if (head.dataBlocks < 2)
          addTard(tar, head)
        else
          addTari(tar, head)
        encodeEntry
    }
  }

  // If the data is "small enough" to fit with the header in a single
  // chunk, put it in a "tard" chunk (for tar direct).
  private def addTard(tar: TarParser, head: TarHeader) {
    val buf = ByteBuffer.allocate(512 * (1 + head.dataBlocks.toInt))
    buf.put(head.raw)
    var left = head.dataBlocks
    while (left > 0L) {
      buf.put(tar.get)
      left -= 1
    }
    buf.flip
    tars.add(Chunk.make("tard", buf))
  }

  // Otherwise, put all of the data into blobs, using indirect blocks.
  // TODO: The top-level indirect block could likely be combined into
  // the tar header, consider this as an optimization.
  private def addTari(tar: TarParser, head: TarHeader) {
    val headBuf = ByteBuffer.allocate(512 + Hash.HashLength)
    headBuf.put(head.raw)
    val blobs = TreeBuilder.makeBuilder("ind", pool)
    var left = head.dataBlocks
    while (left > 0L) {
      val size = (left min 512).toInt
      val buf = ByteBuffer.allocate(size * 512)
      for (i <- 0 until size) {
        // TODO: API improvmenets to avoid all of the copies.
        buf.put(tar.get)
        left -= 1
      }
      buf.flip
      blobs.add(Chunk.make("blob", buf))
    }
    val hash = blobs.finish()
    headBuf.put(hash.getBytes)
    headBuf.flip
    tars.add(Chunk.make("tari", headBuf))
  }
}
