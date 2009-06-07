//////////////////////////////////////////////////////////////////////
// Test the Berkeley DB API.

package org.davidb.jpool

import org.scalatest.Suite
import java.nio.ByteBuffer


class BerkeleyDbSuite extends Suite with TempDirTest {

  import bdb.StringCoder._

  def testDb {
    val env = bdb.Environment.openEnvironment(tmpDir.path)
    env.begin()
    val db = env.openDatabase("sample")

    def nums = Stream.concat(Stream.range(1, 256), Stream.range(256, 32768, 256), Stream(32767))
    for (i <- nums) {
      val name = StringMaker.generate(i, i)
      val hash = Hash("blob", name)
      db.put(hash, name)
    }
    env.commit()

    for (i <- nums) {
      val name = StringMaker.generate(i, i)
      val hash = Hash("blob", name)
      db.get(hash, stringDecoder) match {
        case None => fail("Unable to find entry")
        case Some(res) => assert(name === new String(res))
      }
    }

    // Proc.run("sh", "-c",
    //   "hexdump -C %s/00000000.jdb > /tmp/debug" format tmpDir.path.getPath)
    // val listing = Proc.runAndCapture("ls", "-l", tmpDir.path.getPath)
    // for (line <- listing) {
    //   printf("[test] %s%n", line)
    // }
  }

  // Test that hash decoding works.
  def testHashes {
    val env = bdb.Environment.openEnvironment(tmpDir.path)
    env.begin()
    val db = env.openDatabase("hashes")

    for (i <- 1 to 2000) {
      val text = i.toString
      db.put(text, Hash("blob", text))
    }
    env.commit()

    for (i <- 1 to 2000) {
      val text = i.toString
      db.get(text, Hash) match {
        case None => fail("Unable to find entry")
        case Some(res) => assert(res == Hash("blob", text))
      }
    }
  }
}
