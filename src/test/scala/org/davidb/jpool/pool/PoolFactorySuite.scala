// Testing the Pool Factory.

package org.davidb.jpool.pool

import org.davidb.jpool._

import java.net.{URI, UnknownServiceException}
import org.scalatest.Suite

class PoolFactorySuite extends Suite {
  def testInvalidURL {
    intercept[IllegalArgumentException] {
      PoolFactory.getInstance(new URI("bogus:foobar"))
    }
    intercept[UnknownServiceException] {
      PoolFactory.getInstance(new URI("jpool:unknown:foo"))
    }
  }

  def testFilePool {
    TempDir.withTempDir { tdir =>
      val pool = PoolFactory.getInstance(new URI("jpool:file://%s" format tdir.getPath))
      pool.close()
    }
  }
}
