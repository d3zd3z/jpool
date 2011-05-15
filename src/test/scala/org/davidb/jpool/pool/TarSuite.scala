/**********************************************************************/
// Test tar saving and restoring.

package org.davidb.jpool
package pool

import org.scalatest.Suite

import java.io.File
import java.io.{BufferedReader, InputStreamReader}
import java.nio.ByteBuffer
import java.nio.channels.{Channels, Pipe, ReadableByteChannel, WritableByteChannel}
import java.security.MessageDigest
import java.util.Properties

import scala.concurrent.SyncVar

class TarSuite extends Suite with ProgressPoolTest {

  def testTar {
    genTar
  }

  private def genTar {
    val proc = new ProcessBuilder("tar", "--posix", "-cf", "-", ".").start
    Proc.drainError(proc, "tar")
    val (reader, digestBox) = summarizeChannel(Channels.newChannel(proc.getInputStream))
    val saver = new TarSave(pool, reader)
    // printf("Tar hash: %s%n", saver.hash)
    val digest = digestBox.take()
    assert(proc.waitFor() === 0)
    // printf("Digest = %s%n", digest)

    val props = new Properties
    props.setProperty("info", "Test backup info")
    assert(viewTar(saver.store(props)) === digest)
  }

  private def viewTar(hash: Hash): Hash = {
    val (sink, digestBox) = simpleSummarize
    val restore = new TarRestore(pool, sink, meter)
    restore.decode(hash)
    restore.finish()
    sink.close()
    digestBox.take()
  }

  private def summarizeChannel(chan: ReadableByteChannel): (ReadableByteChannel, SyncVar[Hash]) = {
    val md = MessageDigest.getInstance("SHA-1")
    val result = new SyncVar[Hash]
    val pipe = Pipe.open()
    val child = new Thread {
      var buffer = ByteBuffer.allocate(10240) // tar default block size.
      val sink = pipe.sink()
      override def run() {
        buffer.clear
        val count = fill
        if (count == buffer.limit) {
          buffer.flip
          // printf("Source block%n")
          // Pdump.dump(buffer)
          md.update(buffer)
          buffer.flip
          while (buffer.remaining > 0) {
            val tmp = sink.write(buffer)
            if (tmp <= 0)
              sys.error("Unable to write to pipe")
          }
          run()
        } else if (count == -1) {
          // Done.
          result.put(Hash.raw(md.digest()))
        } else {
          printf("Child tar returned %d bytes%n", count)
          sys.error("Invalid read from child tar.")
        }
      }

      private def fill: Int = {
        while (buffer.remaining > 0) {
          val count = chan.read(buffer)
          if (count == -1) {
            if (buffer.position != 0)
              sys.error("Invalid read from child tar.")
            return -1
          }
          if (count == 0)
            sys.error("Zero read from tar pipe")
        }
        buffer.position
      }
    }
    child.setDaemon(true)
    child.start()
    (pipe.source(), result)
  }

  private def simpleSummarize: (WritableByteChannel, SyncVar[Hash]) = {
    val md = MessageDigest.getInstance("SHA-1")
    val result = new SyncVar[Hash]
    val pipe = Pipe.open()
    val child = new Thread {
      var buffer = ByteBuffer.allocate(10240) // Blocksize.
      val source = pipe.source()
      override def run() {
        buffer.clear
        val count = source.read(buffer)
        if (count >= 0) {
          assert(count === buffer.capacity)
          buffer.flip
          // printf("Dest block%n")
          // Pdump.dump(buffer)
          md.update(buffer)
          run()
        } else {
          result.put(Hash.raw(md.digest()))
        }
      }
    }
    child.setDaemon(true)
    child.start()
    (pipe.sink(), result)
  }
}
