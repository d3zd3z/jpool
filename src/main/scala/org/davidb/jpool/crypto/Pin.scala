// Querying for pins.

package org.davidb.jpool
package crypto

import annotation.tailrec
import collection.mutable.ArrayBuffer

trait PinReader {

  // Read a password from the user, with the given password.
  // Returns the empty string if the user cancels.
  def getPin(prompt: String): Array[Char]

  // Display a message for the user.
  def message(text: String)

  // Read a password twice.  Returns the empty string to indicate user
  // cancellation.
  @tailrec
  final def getInitial(): Array[Char] = {
    val first = getPin("Enter key password: ")
    if (first.length == 0)
      return first
    val second = getPin("Reenter key password: ")
    if (second.length == 0) {
      wipePin(first)
      return second
    }
    if (java.util.Arrays.equals(first, second)) {
      wipePin(second)
      return first
    }
    message("Passwords did not match")
    wipePin(first)
    wipePin(second)
    getInitial()
  }

  // Useful utility to wipe a password
  def wipePin(pin: Array[Char]) {
    var offset = 0
    val len = pin.length
    while (offset < len) {
      pin(offset) = 0
      offset += 1
    }
  }
}

// Read pins using the pinentry program.
class PinEntryReader extends PinReader {

  def message(text: String) {
    sys.error("message")
  }

  def getPin(prompt: String): Array[Char] = {
    sys.error("getPin")
  }
}
object PinEntryReader {
  def make(): Option[PinEntryReader] = {
    try {
      val tty = Linux.ttyname(0)
    } catch {
      case e: NativeError =>
        return None
    }
    Some(new PinEntryReader())
  }
}

// Use the Java console (doesn't work well inside of sbt).
class JavaConsolePinReader extends PinReader {

  def message(text: String) {
    System.console.printf("%s\n", text)
  }

  def getPin(prompt: String): Array[Char] = {
    val pin = System.console.readPassword("%s", prompt)
    if (pin.length == 1)
      getSBTPin(pin(0))
    else
      pin
  }

  // Try to read the rest of a pin through sbt.  This is a somewhat
  // tack hack to work around SBT's readline interfering with the Java
  // console password reader.  It prints a new line for each char,
  // which is weird.
  def getSBTPin(start: Char): Array[Char] = {
    val buf = new ArrayBuffer[Char]
    buf.append(start)
    def loop {
      val text = System.console.readPassword("> ")
      if (text.length > 0) {
        buf ++= text
        loop
      }
    }
    loop

    buf.toArray
  }

}

// For testing, a PinReader that just returns a pre-defined password.
class FixedPinReader(password: String) extends PinReader {
  def message(text: String) = printf("%s\n", text)
  def getPin(prompt: String) = password.toCharArray
}
