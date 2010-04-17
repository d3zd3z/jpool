//////////////////////////////////////////////////////////////////////
// Pool file handling test.

package org.davidb.jpool.pool

import org.davidb.jpool._

import scala.collection.mutable.ArrayBuffer
import java.io.{File, FileNotFoundException}

import org.scalatest.{Suite, BeforeAndAfter}

class PoolFileSuite extends Suite with TempDirTest {
  def testReadEmpty {
    makeFile
  }

  // Read of an empty file should return error when trying to get size
  // or position.
  def testReadError {
    val pf = makeFile
    intercept[FileNotFoundException] {
      pf.size
    }
    intercept[FileNotFoundException] {
      pf.position
    }
    intercept[FileNotFoundException] {
      pf.read(0)
    }
  }

  // Make sure we can append some chunks to an empty file.
  def testAppend {
    val pf = makeFile
    val c1 = makeChunk(1, 1024)
    val pos = pf.append(c1)
    assert (pos === 0)
    val c1b = pf.read(pos)
    assert (c1b.hash === c1.hash)
  }

  // Append a bunch of chunks to an empty file, and make sure we can
  // read them back.
  def testAppendMany {
    val pf = makeFile
    val info = new ArrayBuffer[(Int, Hash)]
    val rnd = new util.Random(42)
    var lastPos = -1
    while (info.size < 100) {
      val chunk = makeChunk(info.size, 128 + rnd.nextInt(8192))
      val pos = pf.append(chunk)
      assert (pos > lastPos)
      info += ((pos, chunk.hash))

      if ((info.size % 37) == 0)
        checkMany(pf, info)
    }
    checkMany(pf, info)
  }

  private def checkMany(pf: PoolFile, items: Seq[(Int, Hash)]) {
    for ((pos, hash) <- items) {
      val chunk = pf.read(pos)
      assert(chunk.hash == hash)
    }
  }

  private def makeFile = new PoolFile(new File(tmpDir.path, "file000.dat"))
  private def makeChunk(index: Int, size: Int) = Chunk.make("blob", StringMaker.generate(index, size))
}
