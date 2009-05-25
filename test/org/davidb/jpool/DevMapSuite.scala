//////////////////////////////////////////////////////////////////////
// Device mapping.

package org.davidb.jpool

import org.scalatest.Suite

class DevMapSuite extends Suite {
  // This is a little tricky to test as root, since with LVM, most
  // of the device info isn't actually available.  Hopefully the root
  // directory is at least accessible.
  def testBasic {
    val dm = new DevMap
    val stat = Linux.lstat("/")
    val rootUUID = dm(stat("dev").toLong)
    assert (rootUUID.length > 8)
  }
}
