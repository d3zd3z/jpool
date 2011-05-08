/*
 * Hash management.
 *
 * Our hashes are very non-specialized.
 */

package org.davidb.jpool

import java.security.MessageDigest
import java.nio.ByteBuffer

object Hash {
  def makeDigest() = MessageDigest.getInstance("SHA-1")
  val HashLength = makeDigest().getDigestLength

  def apply(kind: String, payload: Array[Byte], offset: Int, len: Int): Hash = {
    val md = makeDigest()
    md.update(kind.getBytes())
    md.update(payload, offset, len)
    new Hash(md.digest())
  }

  def apply(kind: String, payload: Array[Byte]): Hash = Hash(kind, payload, 0, payload.length)
  def apply(kind: String, payload: ByteBuffer): Hash = {
    val md = makeDigest()
    md.update(kind.getBytes())
    md.update(payload.duplicate())
    new Hash(md.digest())
  }

  // Debugging.
  def apply(kind: String, payload: String): Hash = Hash(kind, payload.getBytes())

  // Build a hash out of a raw payload.
  def raw(rawHash: Array[Byte]): Hash = raw(rawHash, 0, rawHash.length)
  def raw(rawHash: Array[Byte], offset: Int, length: Int): Hash = {
    val buf = new Array[Byte](length)
    Array.copy(rawHash, offset, buf, 0, length)
    new Hash(buf)
  }

  // Extract a raw hash from a bytebuffer.
  def raw(rawbuf: ByteBuffer): Hash = {
    val buf = FileUtil.getBytes(rawbuf, HashLength)
    new Hash(buf)
  }

  def ofString(text: String): Hash = {
    require(text.length == HashLength * 2)
    val hash = new Array[Byte](HashLength)
    for (i <- 0 until HashLength) {
      val pos = i * 2
      hash(i) = Integer.parseInt(text.substring(pos, pos + 2), 16).toByte
    }
    new Hash(hash)
  }
}

class Hash private (val rawBytes: Array[Byte]) extends Ordered[Hash]
{
  assert(rawBytes.length == Hash.HashLength)

  def getBuffer() = ByteBuffer.wrap(getBytes)
  def getBytes(): Array[Byte] = {
    val HL = Hash.HashLength
    val tmp = new Array[Byte](HL)
    Array.copy(rawBytes, 0, tmp, 0, HL)
    tmp
  }

  def compare(that: Hash): Int = {
    val h1 = rawBytes
    val h2 = that.rawBytes

    def cmp(pos: Int): Int = {
      if (pos == Hash.HashLength)
        0
      else {
        val diff = (h1(pos) & 0xff) - (h2(pos) & 0xff)
        if (diff == 0)
          cmp(pos + 1)
        else
          diff
      }
    }
    cmp(0)
  }

  // Return an individual byte from the hash, properly masked as a byte.
  def byte(index: Int): Int = {
    rawBytes(index).toInt & 0xFF
  }

  // Put this hash into the destination buffer.
  def putTo(dest: ByteBuffer) {
    dest.put(rawBytes)
  }

  override def equals(that: Any) = that match {
    case h2: Hash => (this compare h2) == 0
    case _ => false
  }

  override def toString() = {
    val sb = new StringBuilder(Hash.HashLength * 2)
    for (b <- rawBytes)
      sb.append(String.format("%02x", int2Integer(0xff & b)))
    sb.toString
  }

  override def hashCode(): Int = {
    (rawBytes(0) << 24) |
      ((rawBytes(1) & 0xff) << 16) |
      ((rawBytes(2) & 0xff) << 8) |
      ((rawBytes(3) & 0xff))
  }
}
