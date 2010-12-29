/**********************************************************************/
// Testing of the Seen Database.

package org.davidb.jpool

import org.scalatest.Suite

class SeenDbSuite extends Suite with TempDirTest {
  def testBasic {
    val sdb = new SeenDb(tmpDir.path.getAbsolutePath, "this-is-my-uuid")
    sdb.close()
  }
}
