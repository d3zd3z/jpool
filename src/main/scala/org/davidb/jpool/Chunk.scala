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

  class Header(val clen: Int, val uclen: Int, val kind: String, val hash: Hash)

  // Read the header of the next chunk.  Leaves the file position at
  // the beginning of the payload.
  private def readHeader(chan: FileChannel): Header = {
    val header = FileUtil.readBytes(chan, 48)
    header.order(LITTLE_ENDIAN)

    val version = FileUtil.getBytes(header, 16)
    if (!java.util.Arrays.equals(version, baseVersion))
      sys.error("Invalid chunk header")

    val clen = header.getInt()
    val uclen = header.getInt()
    val kind = new String(FileUtil.getBytes(header, 4))
    val hash = FileUtil.getBytes(header, 20)

    new Header(clen, uclen, kind, Hash.raw(hash))
  }

  def readUnchecked(chan: FileChannel): (Chunk, Hash) = {
    val header = readHeader(chan)

    val clen = header.clen
    val uclen = header.uclen
    val payload = FileUtil.readBytes(chan, (clen + 15) & ~15)
    payload.limit(clen)
    // println("Clen: " + clen)
    // println("UClen: " + uclen)
    val chunk: Chunk = if (uclen == -1)
        new DataChunk(header.kind, payload)
      else
        new CompressedChunk(header.kind, payload, uclen)

    (chunk, header.hash)
  }

  def read(chan: FileChannel): Chunk = {
    val (chunk, hash) = readUnchecked(chan)
    assert(hash == chunk.hash)
    chunk
  }

  // Read the information about a chunk.  Positions the file at the
  // beginning of the next chunk.  Returns the hash and the kind of
  // the chunk.
  def readInfo(chan: FileChannel): (Hash, String, Int) = {
    val header = readHeader(chan)
    val uclen = header.uclen
    val size = if (uclen == -1) header.clen else uclen

    chan.position(chan.position + (header.clen + 15) & ~15)
    (header.hash, header.kind, size)
  }

  // Try reading encrypted data.  The getKey function should retrieve
  // the key of the given index.  If it reads from the channel, it
  // should restore the position before returning.
  // If the header spots an encryption key, it will skip the key, and
  // raise the KeySkipped exception.
  object KeySkipped extends Exception
  def readEncrypted(chan: FileChannel, getKey: Int => crypto.BackupSecret): Chunk = {
    val cryptOffset = chan.position.toString.getBytes("UTF-8")

    val header = FileUtil.readBytes(chan, 16)
    header.order(LITTLE_ENDIAN)
    val version = FileUtil.getBytes(header, 8)
    if (!java.util.Arrays.equals(version, cryptVersion)) {
      if (java.util.Arrays.equals(version, "key-1.1\n".getBytes("UTF-8"))) {
        val keylen = header.getInt()
        chan.position(chan.position + keylen + 16)
        throw KeySkipped
      }
      // TODO: Try reading an encrypted chunk, optionally.
      sys.error("Invalid encrypted chunk header")
    }

    val keyNum = header.getInt()
    val encLen = header.getInt()
    val secret = getKey(keyNum)

    val cipherText = FileUtil.readBytes(chan, encLen)
    val buf = ByteBuffer.wrap(secret.decrypt(cipherText.array, cryptOffset))
    buf.order(LITTLE_ENDIAN)
    val unlen = buf.getInt()
    val kind = new String(FileUtil.getBytes(buf, 4))
    val hash = FileUtil.getBytes(buf, 20)
    val payload = ByteBuffer.wrap(FileUtil.getBytes(buf, buf.remaining))

    val chunk: Chunk = if (unlen == -1)
        new DataChunk(kind, payload)
      else
        new CompressedChunk(kind, payload, unlen)
    assert(Hash.raw(hash) == chunk.hash)
    chunk
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
    if (padding == 0)
      FileUtil.fullWrite(chan, header, payload)
    else {
      val tmp = ByteBuffer.allocate(padding)
      FileUtil.fullWrite(chan, header, payload, tmp)
    }
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

    FileUtil.fullWrite(chan, header, ByteBuffer.wrap(enc))
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
