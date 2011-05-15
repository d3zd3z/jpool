// A single file holding a bunch of encrypted chunks in the storage
// pool.

package org.davidb.jpool
package pool

import java.nio.ByteBuffer
import java.nio.ByteOrder._
import java.io.File
import java.nio.channels.FileChannel

class EncryptedPoolFile(path: File, keyInfo: crypto.RSAInfo)
  extends PoolFileBase(path)
{
  protected var keyMap = collection.immutable.IntMap.empty[crypto.BackupSecret]

  // The secrets are transient, and written the first time needed.
  // It's important to only call this when it would be appropriate to
  // write to the file, since the first access will cause a write to
  // the pool file.
  protected lazy val (secretPos, secret) = {
    val chan = state.getWritable()
    val pos = chan.size.toInt
    chan.position(pos)
    val sec = crypto.BackupSecret.generate()
    writeSecret(chan, sec)
    keyMap += (pos -> sec)
    (pos, sec)
  }

  protected def writeSecret(chan: FileChannel, secret: crypto.BackupSecret) {
    val wrapped = secret.wrap(keyInfo)
    assert((wrapped.length & 15) == 0)
    val fingerprint = keyInfo.getFingerprint
    assert(fingerprint.length == 20)

    val header = ByteBuffer.allocate(32)
    header.order(LITTLE_ENDIAN)
    header.put(keyVersion)
    header.putInt(wrapped.length)
    header.put(fingerprint)
    header.flip()
    FileUtil.fullWrite(chan, header, ByteBuffer.wrap(wrapped))
  }

  protected def readSecret(chan: FileChannel, pos: Int): crypto.BackupSecret = keyMap.get(pos) match {
    case Some(s) => s
    case None =>
      val oldpos = chan.position
      chan.position(pos)
      val header = FileUtil.readBytes(chan, 32)
      header.order(LITTLE_ENDIAN)
      val version = FileUtil.getBytes(header, 8)
      if (!java.util.Arrays.equals(version, keyVersion))
        sys.error("Invalid key header")
      val wlen = header.getInt()
      val fp = FileUtil.getBytes(header, 20)
      if (!java.util.Arrays.equals(fp, keyInfo.getFingerprint))
        sys.error("Backup encrypted with unknown key")
      val wrapped = FileUtil.readBytes(chan, wlen)
      val sec = crypto.BackupSecret.unwrap(keyInfo, wrapped.array)
      keyMap += (pos -> sec)
      chan.position(oldpos)
      sec
  }

  override def append(chunk: Chunk): Int = {
    // Be sure to force the key to be written before we compute a pos.
    val sec = secret

    val chan = state.getWritable()
    val pos = chan.size.toInt
    chan.position(pos)
    chunk.writeEncrypted(chan, sec, secretPos)
    pos
  }

  override def readUnchecked(pos: Int): (Chunk, Hash) = sys.error("TODO")

  override def read(pos: Int): Chunk = {
    val chan = state.getReadable()
    chan.position(pos)
    Chunk.readEncrypted(chan, readSecret(chan, _))
  }

  // Currently, encrypted reads end up reading the entire chunk.
  def readInfo(pos: Int): (Hash, String) = {
    val chunk = read(pos)
    (chunk.hash, chunk.kind)
  }

  protected final val keyVersion = "key-1.1\n".getBytes("UTF-8")
}
