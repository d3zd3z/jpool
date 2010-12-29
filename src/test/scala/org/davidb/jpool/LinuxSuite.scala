/**********************************************************************/
// Test native JNI.

package org.davidb.jpool

import java.io.{File, FileWriter}
import java.nio.ByteBuffer
import java.security.MessageDigest
import org.scalatest.Suite
import scala.collection.mutable.ListBuffer

// Testing this is harder than you might think.  For example, on
// NixOS, very few of the standard directories are present.

class LinuxSuite extends Suite {
  def testNative {
    assert(Linux.message == "Hello world")
  }

  def testDirs {
    val usrBinSet = Set() ++ Linux.readDir("/tmp")
    val findSet = Set() ++ findReadDir("/tmp")
    assert(usrBinSet === findSet)
  }

  def testBadDir {
    intercept[NativeError] {
      Linux.readDir("/invalidDir/stuff")
    }
  }

  def testBadStat {
    intercept[NativeError] {
      Linux.lstat("/invalidDir/stuff")
    }
  }

  private def checkName(name: String, kind: String) {
    val stat = Linux.lstat(name)
    // printf("lstat(%s): %s%n", name, stat)
    checkStat(name, stat, kind)
  }

  def testStats {
    // TODO: Device nodes and such aren't that portable.
    checkName("/etc/passwd", "REG")
    checkName("/dev/null", "CHR")
    checkName("/dev/sda", "BLK")
    checkName(".", "DIR")

    TempDir.withTempDir { tdir =>
      val dnull = "%s/%s" format (tdir.getPath, "dnull")
      Linux.symlink("/dev/null", dnull)
      checkName(dnull, "LNK")
      val stat = Linux.stat(dnull)
      assert(stat("*kind*") === "CHR")
    }
  }

  def testSymlinks {
    intercept[NativeError] {
      Linux.readlink("/invalidDir")
    }
    TempDir.withTempDir { tdir =>
      val names = Array(
        ("s", "short"),
        ("weird", "this is'\"a ^long!@name/with!lo\001ts \377of garbage in it"),
        ("t127", StringMaker.generate(1, 127)),
        ("t128", StringMaker.generate(2, 128)),
        ("t129", StringMaker.generate(3, 129)),
        ("t254", StringMaker.generate(4, 254)),
        ("t255", StringMaker.generate(5, 255)),
        ("t256", StringMaker.generate(6, 256)),
        ("t257", StringMaker.generate(7, 257)))
      for ((src, dest) <- names) {
        Linux.symlink(dest, "%s/%s" format (tdir.getPath, src))
      }
      for ((src, dest) <- names) {
        val dest2 = Linux.readlink("%s/%s" format (tdir.getPath, src))
        assert(dest === dest2)
      }
    }
  }

  def hashFileIn(path: String, chunkSize: Int) {
    def handle(digest: MessageDigest)(chunk: ByteBuffer) {
      // printf("Chunk: %d bytes%n", chunk.remaining)
      digest.update(chunk)
    }
    val lsmd = MessageDigest.getInstance("SHA-1")
    Linux.readFile(path, chunkSize, handle(lsmd)_)
    val dig1 = Hash.raw(lsmd.digest)

    val dig2 = fileHash(path)
    assert(dig1 === dig2)
  }

  // Get the hash of a file using sha1sum externally.
  def fileHash(path: String): Hash = {
    val fields = Proc.runAndCapture("sha1sum", path) match {
      case Array(line) => line.split("\\s+")
      case _ => error("Unknown output from 'sha1sum'")
    }
    Hash.ofString(fields(0))
  }

  def testReads {
    hashFileIn("/etc/passwd", 32768)
    // Test on a large file to demonstrate there is no space leak.
    // hashFileIn("hugefile", 32768)
  }

  def testReadException {
    class SimpleException extends Exception
    def handle(chunk: ByteBuffer) { throw new SimpleException }
    // Try to open more than the limit.  It is typically 1024,
    // although settable, so this test isn't guaranteed.  We're mostly
    // making sure that the descriptor gets closed.
    for (i <- 1 to 2050) {
      intercept[SimpleException] {
        Linux.readFile("/etc/passwd", 32768, handle _)
      }
    }
  }

  def testWrites {
    val md = MessageDigest.getInstance("SHA-1")
    TempDir.withTempDir { tdir =>
      val fileName = "%s/file" format tdir.getPath
      val pieces = Iterator.range(1, 40) map { index =>
        val text = StringMaker.generate(index, 256*1024)
        val buf = ByteBuffer.wrap(text.getBytes)
        md.update(buf.duplicate)
        buf
      }
      Linux.writeFile(fileName, pieces)
      val dig1 = Hash.raw(md.digest)
      val dig2 = fileHash(fileName)
      assert(dig1 === dig2)
    }
  }

  def testLink {
    TempDir.withTempDir { tdir =>
      val fw = new FileWriter(new File(tdir, "file1"))
      fw.write(StringMaker.generate(1, 256*1024))
      fw.close()
      val n1 = "%s/file1" format tdir.getPath
      val n2 = "%s/file2" format tdir.getPath
      Linux.link(n1, n2)
      val st1 = Linux.lstat(n1)
      val st2 = Linux.lstat(n2)
      assert(st1 === st2)
    }
  }

  // Duplicate the native readdir, using find.
  private def findReadDir(path: String): List[(String, Long)] = {
    val result = new ListBuffer[(String, Long)]
    for (line <- Proc.runAndCapture("find", path, "-maxdepth", "1", "-printf", "%P\\t%i\\n")) {
      line.split("\t", 2) match {
        case Array(name, num) =>
          // Find generates a blank name for '.', which
          // Linux.readDir skips.
          if (name.length > 0)
            result += ((name, num.toLong))
        case _ =>
          printf("Unknown output from find: '%s'%n", line)
      }
    }
    result.toList
  }

  // Eliminate any fractional part of a number.
  // TODO: Actually test the fractional part of the timestamp.
  private def nofrac(text: String): String = {
    val index = text.indexOf('.')
    if (index < 0) text
    else text.substring(0, index)
  }

  private def checkStat(path: String, info: Map[String, String], kind: String) {
    assert(info("*kind*") === kind)
    val fields = Proc.runAndCapture("stat", "--format", "%s %Y %Z %h %u %g %f %i %d %t %T", path) match {
      case Array(line) =>
        line.split(" ").map(nofrac(_))
      case _ => error("Invalid response from 'stat' call")
    }
    def hexField(n: Int) = java.lang.Long.parseLong(fields(n), 16)
    // printf("fields: %s", fields.toList)
    assert(fields(0) === info("size"))
    assert(fields(1) === nofrac(info("mtime")))
    assert(fields(2) === nofrac(info("ctime")))
    assert(fields(3) === info("nlink"))
    assert(fields(4) === info("uid"))
    assert(fields(5) === info("gid"))
    assert((hexField(6) & 07777).toString === info("mode"))
    assert(fields(7) === info("ino"))
    assert(fields(8) === info("dev"))
    if (kind == "CHR" || kind == "BLK") {
      assert((hexField(9) * 256 + hexField(10)).toString === info("rdev"))
    }
  }
}
