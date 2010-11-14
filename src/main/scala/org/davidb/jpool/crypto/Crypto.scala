// Backup encryption.

package org.davidb.jpool
package crypto

import org.apache.commons.codec.binary.{Base64, Hex}

import java.security.Key
import java.security.KeyFactory
import java.security.KeyPairGenerator
import java.security.PublicKey
import java.security.PrivateKey
import java.security.MessageDigest
import java.security.SecureRandom
import java.security.Security
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
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

class BackupSecret private (val key: Key, val ivSeed: Array[Byte]) {
  // Return the key encrypted with an RSA public key.
  def wrap(rsa: RSAInfo): Array[Byte] = {
    // Buffer big enough to hold the key and the IV seed.

    val buf = new Array[Byte](32)
    val plain = key.getEncoded()
    try {
      assert(plain.length == 16)
      assert(ivSeed.length == 16)
      Array.copy(plain, 0, buf, 0, 16)
      Array.copy(ivSeed, 0, buf, 16, 16)
      rsa.encrypt(buf)
    } finally {
      Crypto.wipe(plain)
      Crypto.wipe(buf)
    }
  }

  // Generate an IV for a given offset.  The offset is a string of
  // bytes that will be hashed to make this IV unique.  It is
  // important to never encrypt more than one thing with the same
  // key/offset.
  def makeIV(offset: Array[Byte]): IvParameterSpec = {
    val md = MessageDigest.getInstance("SHA-1")
    md.update(ivSeed)
    md.update(offset)
    val iv = new Array[Byte](16)
    Array.copy(md.digest(), 0, iv, 0, 16)
    new IvParameterSpec(iv)
  }

  // Encrypt with a given offset.
  def encrypt(plainText: Array[Byte], offset: Array[Byte]): Array[Byte] = {
    val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding", "BC")
    cipher.init(Cipher.ENCRYPT_MODE, key, makeIV(offset), Crypto.rand)
    cipher.doFinal(plainText)
  }

  // Decrypt with a given offset.
  def decrypt(cipherText: Array[Byte], offset: Array[Byte]): Array[Byte] = {
    val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding", "BC")
    cipher.init(Cipher.DECRYPT_MODE, key, makeIV(offset), Crypto.rand)
    cipher.doFinal(cipherText)
  }

  override def equals(other: Any): Boolean = other match {
    case oth: BackupSecret =>
      key == oth.key && java.util.Arrays.equals(ivSeed, oth.ivSeed)
    case _ => false
  }

  override def hashCode(): Int = {
    // Use the ivSeed, since that doesn't need to be secret.
    (ivSeed(0) << 24) |
      ((ivSeed(1) & 0xff) << 16) |
      ((ivSeed(2) & 0xff) << 8) |
      (ivSeed(3) & 0xff)
  }
}
object BackupSecret {
  def generate() = {
    // Get the random source first to register the provider.
    val rand = Crypto.rand
    val kgen = KeyGenerator.getInstance("AES", "BC")
    kgen.init(128, rand)
    val ivSeed = new Array[Byte](16) // TODO: Get from cipher.
    Crypto.rand.nextBytes(ivSeed)
    new BackupSecret(kgen.generateKey(), ivSeed)
  }

  // Unwrap a secret based on a given key.
  def unwrap(rsa: RSAInfo, wrapped: Array[Byte]): BackupSecret = {
    val bytes = rsa.decrypt(wrapped)
    assert(bytes.length == 32)
    val keyBytes = new Array[Byte](16)
    val ivSeed = new Array[Byte](16)
    Array.copy(bytes, 0, keyBytes, 0, 16)
    Array.copy(bytes, 16, ivSeed, 0, 16)
    val key = new SecretKeySpec(keyBytes, "AES")
    new BackupSecret(key, ivSeed)
  }
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

  def getFingerprint(): Array[Byte] = {
    val md = MessageDigest.getInstance("SHA-1")
    md.digest(cert.getEncoded())
  }

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
  def savePrivate(file: File, pin: PinReader) {
    val secret = pin.getInitial()
    if (secret.length == 0)
      error("Need a pin to be able to save")
    val salt = new Array[Byte](8)
    Crypto.rand.nextBytes(salt)
    val keyspec = new PBEKeySpec(secret, salt, 100)
    val kf = SecretKeyFactory.getInstance("PBEWithSHA1And256BitAES-CBC-BC")
    val key = kf.generateSecret(keyspec)
    val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding", "BC")
    cipher.init(Cipher.ENCRYPT_MODE, key, Crypto.rand)
    val ctext = cipher.doFinal(priv.getEncoded)
    pin.wipePin(secret)

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

  // Force the crypto provider to be registered.
  val rand = Crypto.rand

  // Generate a keypair and certificate.
  def generate(): RSAInfo = {
    val kgen = KeyPairGenerator.getInstance("RSA", "BC")
    kgen.initialize(2048, rand)
    val kp = kgen.genKeyPair()

    val issuer = new X509Principal("C=US, O=org.davidb.jpool, OU=Jpool Backup Certificate")
    val v1CertGen = new X509V1CertificateGenerator()
    v1CertGen.setSerialNumber(new BigInteger(128, rand))
    v1CertGen.setIssuerDN(issuer)
    v1CertGen.setNotBefore(new Date(System.currentTimeMillis() - 1000L * 60 * 60 * 24 * 30))
    v1CertGen.setNotAfter(new Date(System.currentTimeMillis() + 1000L * 60 * 60 * 24 * 365 * 10))
    v1CertGen.setSubjectDN(issuer)
    v1CertGen.setPublicKey(kp.getPublic)
    v1CertGen.setSignatureAlgorithm("SHA1WithRSAEncryption")

    val lcert = v1CertGen.generate(kp.getPrivate, "BC", rand)
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
  def readPrivate(file: File, pin: PinReader): PrivateKey = {
    val pr = new PrivateKeyReader(file)
    pr.readHeader("JPOOL SECRET KEY")
    pr.readProc()
    val salt = pr.readSalt()
    val ctext = pr.readChunk()
    pr.close()

    val secret = pin.getPin("Enter key password: ")
    if (secret.length == 0)
      error("Need password to read key")
    val keyspec = new PBEKeySpec(secret, salt, 100)
    val kf = SecretKeyFactory.getInstance("PBEWithSHA1And256BitAES-CBC-BC")
    val key = kf.generateSecret(keyspec)
    val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding", "BC")
    cipher.init(Cipher.DECRYPT_MODE, key, rand)
    val text = cipher.doFinal(ctext)
    pin.wipePin(secret)

    // Decode the key
    val keyFact = KeyFactory.getInstance("RSA", "BC")
    val spec = new PKCS8EncodedKeySpec(text)
    keyFact.generatePrivate(spec)
  }

  // Load a keypair from public/private info.  Won't prompt for the
  // pin until the private key is actually needed.
  def loadPair(certFile: File, keyFile: File, pin: PinReader): RSAInfo = {
    val lcert = readCert(certFile)
    new RSAInfo {
      val public = lcert.getPublicKey
      lazy val priv = readPrivate(keyFile, pin)
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

  val rand = new SecureRandom()

  def wipe(bytes: Array[Byte]) {
    val len = bytes.length
    var offset = 0
    while (offset < len) {
      bytes(offset) = 0
      offset += 1
    }
  }

  def bench1() {
    val secretBlock = Array.tabulate(1024 * 1024)(_.toByte)
    val smallBlock = Array.tabulate(16)(_.toByte)
    for (i <- 1 to 5) {
      val secret = BackupSecret.generate()
      val offset = "42".getBytes("UTF-8")
      Timer.time(secret.encrypt(secretBlock, offset), "encrypt 1024k")
      Timer.time(secret.encrypt(smallBlock, offset), "encrypt 16")
      val bigCipher = secret.encrypt(secretBlock, offset)
      val smallCipher = secret.encrypt(smallBlock, offset)
      Timer.time(secret.decrypt(bigCipher, offset), "decrypt 1024k")
      Timer.time(secret.decrypt(smallCipher, offset), "decrypt 16")
      printf("---\n")
    }
  }
}
