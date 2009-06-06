//////////////////////////////////////////////////////////////////////
// Test the Berkeley DB API.

package org.davidb.jpool

import com.sleepycat.je.{ DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
  OperationStatus }

import org.scalatest.Suite

class BerkeleyDbSuite extends Suite with TempDirTest {
  def testDb {
    val envConfig = new EnvironmentConfig()
    envConfig.setAllowCreate(true)
    val env = new Environment(tmpDir.path, envConfig)

    val dbConfig = new DatabaseConfig()
    dbConfig.setAllowCreate(true)
    val db = env.openDatabase(null, "sample", dbConfig)

    def nums = Stream.concat(Stream.range(1, 256), Stream.range(256, 32768, 256), Stream(32767))
    for (i <- nums) {
      val name = StringMaker.generate(i, i)
      val hash = Hash("blob", name)
      val key = new DatabaseEntry(hash.getBytes)
      val data = new DatabaseEntry(name.getBytes("UTF-8"))
      val stat = db.put(null, key, data)
      assert(stat === OperationStatus.SUCCESS)
    }

    for (i <- nums) {
      val name = StringMaker.generate(i, i)
      val hash = Hash("blob", name)
      val key = new DatabaseEntry(hash.getBytes)
      val data = new DatabaseEntry()
      val stat = db.get(null, key, data, null)
      assert(stat === OperationStatus.SUCCESS)
      assert(name === new String(data.getData))
    }

    // Proc.run("sh", "-c",
    //   "hexdump -C %s/00000000.jdb > /tmp/debug" format tmpDir.path.getPath)
    // val listing = Proc.runAndCapture("ls", "-l", tmpDir.path.getPath)
    // for (line <- listing) {
    //   printf("[test] %s%n", line)
    // }
  }
}
