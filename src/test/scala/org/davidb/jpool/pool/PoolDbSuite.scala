/**********************************************************************/
// Pool metadata database.

package org.davidb.jpool
package pool

import java.io.File
import java.io.{InputStream, InputStreamReader, BufferedReader}
import java.util.UUID

/**********************************************************************/
// Unit test.

import org.scalatest.{Suite, BeforeAndAfter}

class PoolDbSuite extends Suite {
  def nottestHello {

    Class.forName("org.h2.Driver")
    // compat.Platform.getClassForName("org.h2.Driver")
    val url = "jdbc:h2:/tmp/meta"
    val user = "sa"
    val pass = ""
    val con = java.sql.DriverManager.getConnection(url, user, pass)
    val stmt = con.createStatement
    stmt.executeUpdate("create table config(key varchar not null primary key, value varchar)")
    con.commit()
  }

  def testSimple {
    TempDir.withTempDir { tdir =>
      val db = new PoolDb(tdir)
      for (i <- 1 to 100) {
        db.addBackup(Hash("blob", i.toString))
      }
      val backups = db.getBackups()
      assert(backups.size === 100)
      for (i <- 1 to 100) {
        assert(backups.contains(Hash("blob", i.toString)))
      }
      db.close()

      // Try reopen.
      val db2 = new PoolDb(tdir)
      val b2 = db2.getBackups()
      assert(b2.size === 100)
      db2.close()

      // org.h2.tools.Script.main(Array("-url", "jdbc:h2:%s/meta".format(tdir.getPath),
      //   "-user", "sa", "-password", "", "-script", "/tmp/debug.sql") : _*)
    }
  }

  private def run(args: String*) {
    val pb = new ProcessBuilder(args: _*)
    pb.redirectErrorStream(true)
    val proc = pb.start()
    showOutput(proc.getInputStream())
    assert(proc.waitFor() === 0)
  }

  private def showOutput(is: InputStream) {
    val reader = new BufferedReader(new InputStreamReader(is))
    def lines(reader: BufferedReader) {
      val line = reader.readLine
      if (line ne null) {
        printf("run: %s%n", line)
        lines(reader)
      }
    }
    lines(reader)
  }
}
