// Test cryptography code.

package org.davidb.jpool
package crypto

import java.io.{File, RandomAccessFile}
import org.scalatest.{Suite, BeforeAndAfter}

class CryptoSuite extends Suite {

  // Verify that RSA key generation, save/restore works.
  def testKeyGen {
    TempDir.withTempDir { tmp =>
      val pinread = new FixedPinReader("secret")
      val info = RSAInfo.generate()
      info.saveCert(new File(tmp, "backup.crt"))
      info.savePrivate(new File(tmp, "backup.key"), pinread)
      info.verifyMatch()

      val info2 = RSAInfo.loadCert(new File(tmp, "backup.crt"))
      val priv = RSAInfo.loadPair(new File(tmp, "backup.crt"), new File(tmp, "backup.key"), pinread)
      assert(priv.priv === info.priv)
      priv.verifyMatch()
    }
  }

  // Verify that save and restore of the backup secret works.
  def testBackupSecret {
    val info = RSAInfo.generate()
    val key = BackupSecret.generate()
    val wrapped = key.wrap(info)
    val key2 = BackupSecret.unwrap(info, wrapped)
    assert(key === key2)
  }

  // Verify that encrypted chunk I/O works.
  def testChunkIO {
    TempDir.withTempDir { tmp =>
      val secret = BackupSecret.generate()
      val chan = new RandomAccessFile(new File(tmp, "test.data"), "rw").getChannel()
      val chunks = List(0, 15, 17, 35, 256*1024).map(len => Chunk.make("blob", StringMaker.generate(1, len)))
      val offsets = chunks.map { ch =>
        val pos = chan.position
        ch.writeEncrypted(chan, secret, 42)
        pos
      }

      def keyGet(base: Int): crypto.BackupSecret = {
        assert(base === 42)
        secret
      }

      for ((ofs, ch) <- (offsets zip chunks)) {
        chan.position(ofs)
        val chb = Chunk.readEncrypted(chan, keyGet _)
        assert(chb.hash === ch.hash)
      }
      chan.close()
    }
  }

  // Verify that encrypted pool files work.
  def testPoolFiles {
    TempDir.withTempDir { tmp =>
      val pinread = new FixedPinReader("secret")
      val info = RSAInfo.generate()
      val pf = new pool.EncryptedPoolFile(new File(tmp, "testpool.data"), info)
      val chunks = List(0, 15, 17, 35, 256*1024).map(len => Chunk.make("blob", StringMaker.generate(2, len)))
      val chunks2 = List(15, 17, 35, 256*1024).map(len => Chunk.make("blob", StringMaker.generate(3, len)))
      val ofs = chunks.map(pf.append(_))
      pf.close()
      val pf2 = new pool.EncryptedPoolFile(new File(tmp, "testpool.data"), info)
      val ofs2 = chunks2.map(pf.append(_))
      pf2.close()
      def check(ofs: List[Int], chs: List[Chunk]) {
        val pf = new pool.EncryptedPoolFile(new File(tmp, "testpool.data"), info)
        for ((offset, chIn) <- (ofs zip chs)) {
          val chGot = pf.read(offset)
          assert(chGot.hash === chIn.hash)
        }
        pf.close()
      }
      check(ofs ++ ofs2, chunks ++ chunks2)
    }
  }

}
