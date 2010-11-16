//////////////////////////////////////////////////////////////////////
// Pool metadata database.

package org.davidb.jpool
package pool

import java.io.File
import java.io.{InputStreamReader, BufferedReader}
import java.io.{FileOutputStream, OutputStream, OutputStreamWriter, BufferedWriter}
import java.util.Properties
import java.util.UUID
import scala.io.Source

// To start with, let's just port the existing code over.  We can
// extend this later.

class PoolDb(metaPrefix: File) {

  // Load the backup list from the backup text file (if present).
  private def loadBackups(): Set[Hash] = {
    val backupTxt = new File(metaPrefix, "backups.txt")
    if (backupTxt.isFile) {
      Set.empty ++ Source.fromFile(backupTxt).getLines().map(Hash.ofString(_))
    } else
      Set.empty
  }
  protected var backups = loadBackups()

  // Load the properties from the properties file (if present).
  private def loadProps(): Properties = {
    val propsTxt = new File(metaPrefix, "props.txt")
    val props = new Properties
    if (propsTxt.isFile) {
      val fd = new java.io.FileReader(propsTxt)
      props.load(fd)
      fd.close()
    }
    props
  }
  protected var props = loadProps()

  val uuid = checkUUID

  // Passthroughs to the underlying database.
  def close() = {}
  def getProperty(key: String, default: String) = props.getProperty(key, default)
  def setProperty(key: String, value: String) {
    props.setProperty(key, value)
    writeProperties()
  }

  // Add a record that the specified backup is present.
  def addBackup(hash: Hash) {
    backups += hash
    writeBackups()
  }

  def getBackups(): Set[Hash] = backups

  private def checkUUID: UUID = {
    getProperty("uuid", null) match {
      case null =>
        val uuid = UUID.randomUUID()
        setProperty("uuid", uuid.toString)
        uuid
      case uuidText => UUID.fromString(uuidText)
    }
  }

  // Write all of the metadata to plain text files as well.  Assume
  // all of the fields are plain text, and the keys don't have any
  // equal signs in them.
  abstract class DataWriter(base: String) {
    private val realName = new File(metaPrefix, base + ".txt")
    private val tmpName = new File(metaPrefix, base + ".tmp")
    def write() {
      try {
        val fos = new FileOutputStream(tmpName)
        emitItems(fos)
        fos.getChannel.force(true)
        fos.close()
        tmpName.renameTo(realName)
      } catch {
        case _ =>
      }
    }

    // Emit the items.  The 'out' should not be closed so it can be
    // forced properly.
    def emitItems(out: OutputStream)

    def checkWrite() {
      if (!realName.exists())
        write()
    }
  }

  object PropWriter extends DataWriter("props") {
    def emitItems(out: OutputStream) {
      props.store(out, "Jpool metadata properties")
    }
  }
  def writeProperties() = PropWriter.write()
  PropWriter.checkWrite()

  object BackWriter extends DataWriter("backups") {
    def emitItems(out: OutputStream) {
      val writer = new BufferedWriter(new OutputStreamWriter(out))
      for (back <- getBackups()) {
        writer.write(back.toString())
        writer.newLine()
      }
      writer.flush()
    }
  }
  def writeBackups() = BackWriter.write()
  BackWriter.checkWrite()
}
