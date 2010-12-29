/**********************************************************************/
// Test storage and retrieval of file data.

package org.davidb.jpool
package pool

import java.io.{File, FileOutputStream}
import java.nio.ByteBuffer
import org.scalatest.Suite

class FileDataSuite extends Suite with ProgressPoolTest {

  def testFile {
    val name = new File(tmpDir.path, "file0000")
    buildFile(name, 32768, 128, 1)
    val h1 = FileData.store(pool, name.getPath)
    val oldsize = FileData.chunkSize
    FileData.chunkSize = 4096
    val h2 = try {
      FileData.store(pool, name.getPath)
    } finally {
      FileData.chunkSize = oldsize
    }
    pool.flush()

    val name1 = new File(tmpDir.path, "file0000b")
    FileData.restore(pool, name1.getPath, h1)
    val name2 = new File(tmpDir.path, "file0000c")
    FileData.restore(pool, name2.getPath, h2)
    Proc.run("cmp", name.getPath, name1.getPath)
    Proc.run("cmp", name.getPath, name2.getPath)
  }

  def buildFile(name: File, blockSize: Int, blocks: Int, phase: Int) {
    val chan = new FileOutputStream(name).getChannel()
    for (pos <- 0 until blocks) {
      val buf = ByteBuffer.wrap(StringMaker.generate(phase + pos, blockSize).getBytes)
      while (buf.hasRemaining()) {
        val count = chan.write(buf)
        assert(count > 0)
      }
    }
    chan.close()
  }
}
