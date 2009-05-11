// Testing the Pool Factory.

package org.davidb.jpool.pool

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
}
