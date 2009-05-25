//////////////////////////////////////////////////////////////////////
// Testing tree saving.

package org.davidb.jpool.pool

import org.scalatest.Suite

class TreeSaveSuite extends Suite with PoolTest {

  // Restore not implemented, so the best we can do is save something
  // and generate a listing and make sure not fatal errors happen.
  // TODO: This is way too verbose, so not really want we're going to
  // want to be doing.  Better to use a generalized tree walker.
  def testSave {
    // These also test graceful handling of unreadable files.
    var h1 = new TreeSave(pool, "/bin").store()
    var h2 = new TreeSave(pool, "/dev").store()
    printf("%s%n%s%n", h1, h2)

    new TreeList(pool).show(h1)
    new TreeList(pool).show(h2)
  }
}
