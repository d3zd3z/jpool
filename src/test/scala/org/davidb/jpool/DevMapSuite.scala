//////////////////////////////////////////////////////////////////////
// Device mapping.

package org.davidb.jpool

import org.scalatest.Suite

class DevMapSuite extends Suite {
  // This is a little tricky to test as root, since with LVM, most
  // of the device info isn't actually available.  Hopefully the root
  // directory is at least accessible.
  // Since we haven't solved the problem of getting btrfs working,
  // this fails on the root filesystem.  There really isn't a
  // consistent place we can look for to make this work.  For now,
  // just try /boot, since that doesn't work as btrfs.
  def testBasic {
    val dm = new DevMap
    val stat = Linux.lstat("/boot")
    val rootUUID = dm(stat("dev").toLong)
    assert (rootUUID.length > 8)
  }
}
