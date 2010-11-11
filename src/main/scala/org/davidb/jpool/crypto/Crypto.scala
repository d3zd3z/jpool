// Backup encryption.

package org.davidb.jpool
package crypto

import org.apache.commons.codec.binary.{Base64, Hex}

import java.security.Key
import java.security.KeyPair
import java.security.KeyFactory
import java.security.KeyPairGenerator
import java.security.PublicKey
import java.security.PrivateKey
import java.security.SecureRandom
import java.security.Security
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import java.security.spec.X509EncodedKeySpec
import java.security.spec.PKCS8EncodedKeySpec
import javax.crypto.Cipher
import javax.crypto.KeyGenerator
import javax.crypto.SecretKeyFactory
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.PBEKeySpec
import javax.crypto.spec.SecretKeySpec

import java.io.{File, FileInputStream}
import java.math.BigInteger
import java.util.Date

import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.bouncycastle.jce.X509Principal
import org.bouncycastle.x509.X509V1CertificateGenerator

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

class PEMWriter(file: File) {
  val fw = new java.io.FileWriter(file)

  def writeHeader(kind: String) {
    fw.write("-----BEGIN %s-----\n".format(kind))
  }
  def writeFooter(kind: String) {
    fw.write("-----END %s-----\n".format(kind))
  }
  def writeChunk(data: Array[Byte]) {
    val buf = Base64.encodeBase64String(data)
    fw.write(buf.replaceAll("\r\n", "\n"))
  }
  def writeText(line: String) {
    fw.write(line)
    fw.write('\n')
  }
  def close() = fw.close()
}

class PEMReader(file: File) {
  val reader = new java.io.BufferedReader(new java.io.FileReader(file))
  def close() = reader.close()
  def readHeader(kind: String) {
    val line = reader.readLine()
    if (line != "-----BEGIN %s-----".format(kind))
      error("%s invalid file, expecting BEGIN %s".format(this.getClass, kind))
  }
  def readChunk(): Array[Byte] = {
    val buf = new StringBuilder
    def loop {
      val line = reader.readLine()
      if (!line.startsWith("-----END ")) {
        buf.append(line)
        loop
      }
    }
    loop
    Base64.decodeBase64(buf.toString)
  }
}
class PrivateKeyReader(file: File) extends PEMReader(file) {
  def readProc() {
    val line = reader.readLine()
    if (line != "Proc-Type: 4,ENCRYPTED")
      error("Expecting Proc-Type")
  }
  def readSalt(): Array[Byte] = {
    val line = reader.readLine()
    val prefix = "DEK-INFO: AES-CBC-BC,"
    if (!line.startsWith(prefix))
      error("Expecting " + prefix)
    Hex.decodeHex(line.substring(prefix.length).toCharArray)
  }
}

// RSA keypair management.
abstract class RSAInfo {
  def public: PublicKey
  def priv: PrivateKey
  def cert: X509Certificate

  // Write the certificate to the given file.
  def saveCert(file: File) {
    val pw = new PEMWriter(file)
    pw.writeHeader("CERTIFICATE")
    pw.writeChunk(cert.getEncoded)
    pw.writeFooter("CERTIFICATE")
    pw.close()
  }

  // Write the private key to a file, encrypted with the given
  // password.
  def savePrivate(file: File, secret: Array[Char]) {
    val salt = new Array[Byte](8)
    Crypto.rand.nextBytes(salt)
    val keyspec = new PBEKeySpec(secret, salt, 100)
    val kf = SecretKeyFactory.getInstance("PBEWithSHA1And256BitAES-CBC-BC")
    val key = kf.generateSecret(keyspec)
    val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding", "BC")
    cipher.init(Cipher.ENCRYPT_MODE, key, Crypto.rand)
    val ctext = cipher.doFinal(priv.getEncoded)

    val pw = new PEMWriter(file)
    pw.writeHeader("JPOOL SECRET KEY")
    pw.writeText("Proc-Type: 4,ENCRYPTED")
    pw.writeText("DEK-INFO: AES-CBC-BC," + Hex.encodeHexString(salt))
    pw.writeText("")
    pw.writeChunk(ctext)
    pw.writeFooter("JPOOL SECRET KEY")
    pw.close()

    // Test recovery.
    restore(ctext, secret, salt)
  }
  def restore(ctext: Array[Byte], secret: Array[Char], salt: Array[Byte]) {
    val keyspec = new PBEKeySpec(secret, salt, 100)
    val kf = SecretKeyFactory.getInstance("PBEWithSHA1And256BitAES-CBC-BC")
    val key = kf.generateSecret(keyspec)
    val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding", "BC")
    cipher.init(Cipher.DECRYPT_MODE, key, Crypto.rand)
  }

  // Verify that the private key matches the public key.
  def verifyMatch() {
    val text = new Array[Byte](32)
    val ctext = encrypt(text)
    val text2 = decrypt(ctext)
    if (!java.util.Arrays.equals(text, text2))
      error("Private key and certificate do not match")
  }
  def encrypt(data: Array[Byte]): Array[Byte] = {
    val cipher = Cipher.getInstance("RSA/NONE/OAEPWithSHA1AndMGF1Padding", "BC")
    cipher.init(Cipher.ENCRYPT_MODE, public, Crypto.rand)
    cipher.doFinal(data)
  }
  def decrypt(data: Array[Byte]): Array[Byte] = {
    val cipher = Cipher.getInstance("RSA/NONE/OAEPWithSHA1AndMGF1Padding", "BC")
    cipher.init(Cipher.DECRYPT_MODE, priv, Crypto.rand)
    cipher.doFinal(data)
  }
}
object RSAInfo {

  // Generate a keypair and certificate.
  def generate(): RSAInfo = {
    val kgen = KeyPairGenerator.getInstance("RSA", "BC")
    kgen.initialize(2048, Crypto.rand)
    val kp = kgen.genKeyPair()

    val issuer = new X509Principal("C=US, O=org.davidb.jpool, OU=Jpool Backup Certificate")
    val v1CertGen = new X509V1CertificateGenerator()
    v1CertGen.setSerialNumber(new BigInteger(128, Crypto.rand))
    v1CertGen.setIssuerDN(issuer)
    v1CertGen.setNotBefore(new Date(System.currentTimeMillis() - 1000L * 60 * 60 * 24 * 30))
    v1CertGen.setNotAfter(new Date(System.currentTimeMillis() + 1000L * 60 * 60 * 24 * 365 * 10))
    v1CertGen.setSubjectDN(issuer)
    v1CertGen.setPublicKey(kp.getPublic)
    v1CertGen.setSignatureAlgorithm("SHA1WithRSAEncryption")

    val lcert = v1CertGen.generate(kp.getPrivate, "BC", Crypto.rand)
    lcert.checkValidity(new Date())
    lcert.verify(kp.getPublic)

    new RSAInfo {
      val public = kp.getPublic
      val priv = kp.getPrivate
      val cert = lcert
    }
  }

  // Load a public key from a certificate.  The info returned from
  // this will not have a private key, and requesting the private key
  // will raise an exception.
  protected def readCert(file: File): X509Certificate = {
    val cf = CertificateFactory.getInstance("X.509", "BC")
    val lcert = cf.generateCertificate(new FileInputStream(file)).asInstanceOf[X509Certificate]
    lcert.checkValidity(new Date())
    lcert.verify(lcert.getPublicKey)
    lcert
  }

  def loadCert(file: File): RSAInfo = {
    val lcert = readCert(file)
    new RSAInfo {
      val public = lcert.getPublicKey
      def priv = error("No private key available")
      val cert = lcert
    }
  }

  // Load a private key.
  def readPrivate(file: File, secret: Array[Char]): PrivateKey = {
    val pr = new PrivateKeyReader(file)
    pr.readHeader("JPOOL SECRET KEY")
    pr.readProc()
    val salt = pr.readSalt()
    val ctext = pr.readChunk()
    pr.close()

    val keyspec = new PBEKeySpec(secret, salt, 100)
    val kf = SecretKeyFactory.getInstance("PBEWithSHA1And256BitAES-CBC-BC")
    val key = kf.generateSecret(keyspec)
    val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding", "BC")
    cipher.init(Cipher.DECRYPT_MODE, key, Crypto.rand)
    val text = cipher.doFinal(ctext)

    // Decode the key
    val keyFact = KeyFactory.getInstance("RSA", "BC")
    val spec = new PKCS8EncodedKeySpec(text)
    keyFact.generatePrivate(spec)
  }

  // Load a keypair from public/private info.
  def loadPair(certFile: File, keyFile: File, secret: Array[Char]): RSAInfo = {
    val lcert = readCert(certFile)
    val lpriv = readPrivate(keyFile, secret)
    new RSAInfo {
      val public = lcert.getPublicKey
      val priv = lpriv
      val cert = lcert
    }
  }

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
    // bench1()
    val info = RSAInfo.generate()
    info.saveCert(new File("backup.crt"))
    info.savePrivate(new File("backup.key"), "secret".toCharArray)
    info.verifyMatch()

    val info2 = RSAInfo.loadCert(new File("backup.crt"))
    val priv = RSAInfo.loadPair(new File("backup.crt"), new File("backup.key"), "secret".toCharArray)
    if (priv.priv != info.priv)
      error("Unable to reload key")
    priv.verifyMatch()
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
