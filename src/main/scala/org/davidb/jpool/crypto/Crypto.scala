// Backup encryption.

package org.davidb.jpool
package crypto

import java.security.Key
import java.security.SecureRandom
import java.security.Security
import javax.crypto.Cipher
import javax.crypto.KeyGenerator
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec

import java.util.Date

import org.bouncycastle.jce.provider.BouncyCastleProvider

class BackupSecret private (val key: Key) {
}
object BackupSecret {
  def generate() = {
    val kgen = KeyGenerator.getInstance("AES", "BC")
    kgen.init(128, Crypto.rand)
    new BackupSecret(kgen.generateKey())
  }
}

case class Encrypted(cipherText: Array[Byte], iv: Array[Byte]) {
  def decrypt(key: Key): Array[Byte] = {
    val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding", Encrypted.provider)
    cipher.init(Cipher.DECRYPT_MODE, key, new IvParameterSpec(iv), Crypto.rand)
    cipher.doFinal(cipherText)
  }
}
object Encrypted {
  def encrypt(key: Key, plainText: Array[Byte]): Encrypted = {
    val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding", provider)
    cipher.init(Cipher.ENCRYPT_MODE, key, Crypto.rand)
    val iv = cipher.getIV
    val cipherText = cipher.doFinal(plainText)
    new Encrypted(cipherText, iv)
  }

  // The BouncyCastle seems to be a bit faster than the SunJCE
  // provider.  Not a lot.
  val provider = "BC"
  // val provider = "SunJCE"
}

abstract class Timer {
  def run() // Operation to perform time.

  // Run the operation 'count' times, returning the number of ms it
  // took.
  protected def runTimes(count: Int): Long = {
    val start = new Date().getTime()
    def loop(n: Int) {
      if (n < count) {
        run()
        loop(n+1)
      }
    }
    loop(0)
    val end = new Date().getTime()
    end - start
  }

  // Run the operation for a second or so, returning the number of
  // times per second that run() can be called.
  def measure(): Double = {
    def loop(n: Int): Double = {
      val time = runTimes(n)
      if (time < 1500)
        loop(2 * n)
      else {
        n.toDouble / (time.toDouble / 1000.0)
      }
    }
    loop(1)
  }

  def show(text: String) {
    val time = measure()
    printf("%10.3f op/sec: %s\n", time, text)
  }
}
object Timer {
  def time(op: => Unit, text: String) {
    new Timer {
      def run = op
    }.show(text)
  }
}

object Crypto {

  Security.addProvider(new BouncyCastleProvider)

  def rand = new SecureRandom()

  val secretBlock = Array.tabulate(1024 * 1024)(_.toByte)
  val smallBlock = Array.tabulate(16)(_.toByte)

  def main(args: Array[String]) {
    bench1()
  }

  def bench1() {
    for (i <- 1 to 5) {
      val secret = BackupSecret.generate()
      Timer.time(Encrypted.encrypt(secret.key, secretBlock), "encrypt 1024k")
      Timer.time(Encrypted.encrypt(secret.key, smallBlock), "encrypt 16")
      val bigCipher = Encrypted.encrypt(secret.key, secretBlock)
      val smallCipher = Encrypted.encrypt(secret.key, smallBlock)
      Timer.time(bigCipher.decrypt(secret.key), "decrypt 1024k")
      Timer.time(smallCipher.decrypt(secret.key), "decrypt 16")
      printf("---\n")
    }
  }
}
