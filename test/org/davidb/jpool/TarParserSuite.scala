package org.davidb.jpool

import org.scalatest.Suite
import java.nio.channels.Channels
import java.io.{BufferedReader, InputStreamReader}

class TarParserSuite extends Suite {
  def testTarup {
    val proc = new ProcessBuilder("tar", "--posix", "-cf", "-", ".").start
    drainError(proc)
    val tar = new TarParser(Channels.newChannel(proc.getInputStream))
    walkTar(tar)
    assert(proc.waitFor === 0)
  }

  private def walkTar(tar: TarParser) {
    tar.getHeader match {
      case None => // println("EOF")
      case Some(x) =>
        // printf("Tar %d blocks=%d%n", x.size, x.dataBlocks)
        var left = x.dataBlocks
        while (left > 0L) {
          tar.get
          left -= 1
        }
        walkTar(tar)
    }
  }

  private def drainError(proc: Process) {
    val child = new Thread {
      val input = new BufferedReader(new InputStreamReader(proc.getErrorStream))
      override def run() {
        val line = input.readLine()
        if (line ne null) {
          printf("tar: '%s'%n", line)
          run()
        }
      }
    }
    child.setDaemon(true)
    child.start()
  }
}
