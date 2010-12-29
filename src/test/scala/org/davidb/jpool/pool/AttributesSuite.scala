/**********************************************************************/
// Testing of attribute stores.

package org.davidb.jpool
package pool

import org.scalatest.Suite

class AttributesSuite extends Suite with ProgressPoolTest {
  def testXML {
    var map = Map[String, String]()
    map += ("aaa" -> "123")
    map += ("bbb" -> "456")
    map += ("ugly" -> "This\370is\201ugly")
    val att = new Attributes("SILLY", map)
    val bin = att.toByteArray()
    // Pdump.dump(bin)

    val attb = Attributes.decode(bin)
    assert(att === attb)
  }

  // Test that we can encode and decode all possible characters.
  def testChars {
    var map = Map[String, String]()
    for (key <- 0 until 256) {
      map += (key.toString -> new String(Array(key.toChar)))
    }
    val att = new Attributes("Kind", map)
    val bin = att.toByteArray()
    // Pdump.dump(bin)
    val attb = Attributes.decode(bin)

    /*
    for (keyb <- 0 until 256) {
      val key = keyb.toString
      if (!(attb contains key)) {
        printf("Missing key %d%n", keyb)
      } else if (att(key) != attb(key)) {
        printf("Mismatch on key: %s '%s' '%s'%n", key, att(key), attb(key))
      }
    }
    */
    assert(att === attb)
  }

  def testPool {
    val a1 = Attributes.ofLinuxStat(Linux.lstat("/etc/passwd"))
    val h1 = a1.store(pool)
    val a2 = Attributes.ofLinuxStat(Linux.lstat("/dev/null"))
    val h2 = a2.store(pool)

    assert(a1 === Attributes.decode(pool(h1)))
    assert(a2 === Attributes.decode(pool(h2)))
  }
}
