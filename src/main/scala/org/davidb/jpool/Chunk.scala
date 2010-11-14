/*
 * Chunk management.
 */

package org.davidb.jpool

import java.nio.ByteBuffer
import java.nio.ByteOrder._
import java.nio.channels.{FileChannel, ReadableByteChannel}

import java.util.zip.{Inflater, Deflater}

/*
 * Chunks are the fundamental unit of storage.  Each chunk has a
 * 4-character 'kind', and 0 or more bytes of payload.
 */

object Chunk {

  // Simple constructors.
  def make(kind: String, data: String): Chunk =
    new DataChunk(kind, ByteBuffer.wrap(data.getBytes))
  def make(kind: String, data: ByteBuffer): Chunk =
    new DataChunk(kind, data.duplicate)

  def readUnchecked(chan: FileChannel): (Chunk, Hash) = {
    val header = readBytes(chan, 48)
    header.order(LITTLE_ENDIAN)

    val version = getBytes(header, 16)
    if (!java.util.Arrays.equals(version, baseVersion))
      error("Invalid chunk header")

    val clen = header.getInt()
    val uclen = header.getInt()
    val kind = new String(getBytes(header, 4))
    val hash = getBytes(header, 20)

    val payload = readBytes(chan, (clen + 15) & ~15)
    payload.limit(clen)
    // println("Clen: " + clen)
    // println("UClen: " + uclen)
    val chunk: Chunk = if (uclen == -1)
        new DataChunk(kind, payload)
      else
        new CompressedChunk(kind, payload, uclen)

    (chunk, Hash.raw(hash))
  }

  def read(chan: FileChannel): Chunk = {
    val (chunk, hash) = readUnchecked(chan)
    assert(hash == chunk.hash)
    chunk
  }

  // Try reading encrypted data.  The getKey function should retrieve
  // the key of the given index.  If it reads from the channel, it
  // should restore the position before returning.
  def readEncrypted(chan: FileChannel, getKey: Int => crypto.BackupSecret): Chunk = {
    val cryptOffset = chan.position.toString.getBytes("UTF-8")

    val header = readBytes(chan, 16)
    header.order(LITTLE_ENDIAN)
    val version = getBytes(header, 8)
    if (!java.util.Arrays.equals(version, cryptVersion))
      // TODO: Try reading an encrypted chunk, optionally.
      error("Invalid encrypted chunk header")

    val keyNum = header.getInt()
    val encLen = header.getInt()
    val secret = getKey(keyNum)

    val cipherText = readBytes(chan, encLen)
    val buf = ByteBuffer.wrap(secret.decrypt(cipherText.array, cryptOffset))
    buf.order(LITTLE_ENDIAN)
    val unlen = buf.getInt()
    val kind = new String(getBytes(buf, 4))
    val hash = getBytes(buf, 20)
    val payload = ByteBuffer.wrap(getBytes(buf, buf.remaining))

    val chunk: Chunk = if (unlen == -1)
        new DataChunk(kind, payload)
      else
        new CompressedChunk(kind, payload, unlen)
    assert(Hash.raw(hash) == chunk.hash)
    chunk
  }

  /*
   * Read 'count' bytes fully into a new ByteBuffer.
   */
  private def readBytes(chan: ReadableByteChannel, count: Int): ByteBuffer = {
    val buf = ByteBuffer.allocate(count)
    while (buf.remaining > 0) {
      val tmp = chan.read(buf)
      if (tmp <= 0)
        error("Unable to read data from channel")
    }
    buf.rewind()
    buf
  }

  /* Extract 'count' bytes out of a ByteBuffer. */
  private def getBytes(buf: ByteBuffer, count: Int): Array[Byte] = {
    val result = new Array[Byte](count)
    buf.get(result)
    result
  }

  final val baseVersionText = "adump-pool-v1.1\n"
  final val baseVersion = baseVersionText.getBytes("UTF-8")

  final val cryptVersion = "AES-128\n".getBytes("UTF-8")
}

abstract class Chunk(val kind: String) {
  require(kind.length == 4)
  require(kind.getBytes.length == 4)

  val _data: ByteBuffer
  val _zdata: ByteBuffer
  val dataLength: Int
  lazy val hash: Hash = Hash(kind, data)

  final def data: ByteBuffer = _data.duplicate()
  final def zdata: ByteBuffer = if (_zdata eq null) null else _zdata.duplicate()

  override def toString: String = {
    String.format("[%s: '%s', '%s', %d bytes]",
      "Chunk", kind, hash.toString, int2Integer(dataLength))
  }

  def writeSize: Int = {
    val zd = _zdata
    val len = if (zd eq null) dataLength else zd.remaining
    48 + (len + 15) & -15
  }

  def write(chan: FileChannel) {
    val header = ByteBuffer.allocate(48)
    header.order(LITTLE_ENDIAN)

    header.put(Chunk.baseVersion)

    var payload = zdata
    if (payload eq null) { // Didn't compress.
      payload = data
      header.putInt(payload.remaining)
      header.putInt(-1)
    } else {
      header.putInt(payload.remaining)
      header.putInt(dataLength)
    }
    header.put(kind.getBytes)
    header.put(hash.getBytes)

    header.flip()

    val padding = (-payload.remaining & 15)
    val writes =
      if (padding == 0)
        Array(header, payload)
      else {
        val tmp = ByteBuffer.allocate(padding)
        Array(header, payload, tmp)
      }
    fullWrite(chan, writes)
  }

  // Write an encrypted chunk to the backup stream.  The data is
  // written with a special header indicating the algorithm, an
  // integer index for the private key (beyond the scope of this
  // code), and then the ciphertext.
  def writeEncrypted(chan: FileChannel, secret: crypto.BackupSecret, keyNum: Int) {
    var payload = zdata

    // Format is a bit difference, since the length will be recovered.
    var unlen = dataLength
    if (payload eq null) {
      payload = data
      unlen = -1
    }
    val buf = ByteBuffer.allocate(4 + 4 + 20 + payload.remaining)
    buf.order(LITTLE_ENDIAN)
    buf.putInt(unlen)
    buf.put(kind.getBytes)
    buf.put(hash.getBytes)
    buf.put(payload)
    buf.flip()
    val enc = secret.encrypt(buf.array, chan.position.toString.getBytes("UTF-8"))

    val header = ByteBuffer.allocate(16)
    header.order(LITTLE_ENDIAN)
    header.put(Chunk.cryptVersion)
    header.putInt(keyNum)
    header.putInt(enc.length)
    header.flip()

    // The cipher should always be a multiple of a block size, so no
    // padding.
    assert((enc.length & 15) == 0)

    fullWrite(chan, Array(header, ByteBuffer.wrap(enc)))
  }

  private def fullWrite(chan: FileChannel, bufs: Array[ByteBuffer]) {
    var len = 0L
    for (b <- bufs)
      len += b.remaining

    while (len > 0) {
      val count = chan.write(bufs)
      if (count <= 0)
        error("Unable to write buffer data")
      len -= count
    }
  }
}

class DataChunk(kind: String, val _data: ByteBuffer) extends Chunk(kind) {
  lazy val _zdata = {
    if (dataLength <= 16)
      null
    else {
      // Attempt compression, but only use if it it results in a net
      // savings.
      val tmp = new Array[Byte](dataLength - 1)
      val defl = new Deflater(3)
      defl.setInput(_data.array, _data.arrayOffset + _data.position,
        _data.remaining)
      defl.finish()
      val count = defl.deflate(tmp)
      val result = if (defl.finished)
          ByteBuffer.wrap(tmp, 0, count)
        else
          null
      defl.end()
      result
    }
  }
  val dataLength = data.remaining
}

class CompressedChunk(kind: String, val _zdata: ByteBuffer,
  val dataLength: Int) extends Chunk(kind)
{
  lazy val _data = {
    val tmp = new Array[Byte](dataLength)
    val inf = new Inflater
    inf.setInput(_zdata.array, _zdata.arrayOffset + _zdata.position,
      _zdata.remaining)
    val len = inf.inflate(tmp)
    assert(len == dataLength)
    assert(inf.finished)
    inf.end()
    ByteBuffer.wrap(tmp)
  }
}
