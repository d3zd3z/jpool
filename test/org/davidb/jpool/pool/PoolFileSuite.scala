//////////////////////////////////////////////////////////////////////
// Pool file handling test.

package org.davidb.jpool.pool

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

  def makeFile = new PoolFile(new File(tmpDir.path, "file000.dat"))
}
