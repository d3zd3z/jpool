//////////////////////////////////////////////////////////////////////
// Migrating backups from one pool to another.
//
// Usage:
//   Clone jpool:file:///path1 jpool:file:///path2 backuphash backuphash...

package org.davidb.jpool.tools

import org.davidb.jpool._
import org.davidb.jpool.pool._
import org.davidb.logging.Loggable

import java.net.URI

object Clone extends AnyRef with Loggable {

  class CloneMeter extends ProgressMeter {
    import ProgressMeter.humanize

    var _read = 0L
    var _readChunks = 0L
    var _write = 0L

    def addReadChunk() = synchronized {
      _readChunks += 1
      update()
    }

    def formatState(forced: Boolean): Array[String] = {
      Array("Read : %s bytes, %s chunks".format(humanize(_read), _readChunks),
        "Write: %s bytes".format(humanize(_write)))
    }

    object reader extends DataProgress {
      def addData(count: Long) {
        _read += count
        update()
      }
      def addDup(count: Long) {
        error("Dup on read is strange")
      }
    }

    object writer extends DataProgress {
      def addData(count: Long) {
        _write += count
        update()
      }
      def addDup(count: Long) {
        error("There shouldn't be any duping.")
      }
    }
  }

  def main(args: Array[String]) {
    if (args.length < 3) {
      logger.error("Usage: Check jpool:file:///path1 jpool:file:///path2 hash hash...")
      exit(1)
    }

    val meter = new CloneMeter
    ProgressMeter.register(meter)

    val inPool = PoolFactory.getInstance(new URI(args(0)))
    inPool.setProgress(meter.reader)
    val outPool = PoolFactory.getStoreInstance(new URI(args(1)))
    outPool.setProgress(meter.writer)

    val walker = new TreeWalk(inPool)

    for (i <- 2 until args.length) {
      def mark(hash: Hash, get: () => Chunk): GC.MarkVisited = {
        val chunk = get()
        meter.addReadChunk()
        meter.reader.addData(chunk.dataLength)
        if (outPool.contains(hash))
          GC.Seen
        else {
          outPool += (chunk.hash -> chunk)
          GC.Unseen
        }
      }

      walker.gcWalk(Hash.ofString(args(i)), mark _)
    }

    ProgressMeter.unregister(meter)
    outPool.close()
    inPool.close()
  }

}
