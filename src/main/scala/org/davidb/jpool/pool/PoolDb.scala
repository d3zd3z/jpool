//////////////////////////////////////////////////////////////////////
// Pool metadata database.

package org.davidb.jpool.pool

import java.io.File
import java.io.{InputStream, InputStreamReader, BufferedReader}
import java.sql.{Connection, Driver, ResultSet, PreparedStatement}
import java.security.MessageDigest
import java.util.UUID

// To start with, let's just port the existing code over.  We can
// extend this later.

object PoolDbSchema extends DbSchema {
  val schemaVersion = "1:2009-05-30"
  val schemaText = Array(
    "create table backups (hash binary(20) not null primary key)")
  def schemaUpgrade(oldVersion: String, db: Db) = error("TODO: Upgrade")
}

class PoolDb(metaPrefix: File) {
  private val db = new Db(new File(metaPrefix, "meta").getAbsolutePath(), PoolDbSchema)

  val uuid = checkUUID

  // Passthroughs to the underlying database.
  def close() = db.close()
  def getProperty(key: String, default: String) = db.getProperty(key, default)
  def setProperty(key: String, value: String) {
    db.setProperty(key, value)
    db.commit()
  }

  // Add a record that the specified backup is present.
  def addBackup(hash: Hash) {
    val b = hash.getBytes()
    db.updateQuery("delete from backups where hash = ?", b)
    db.updateQuery("insert into backups values (?)", b)
    db.commit()
  }

  def getBackups(): Set[Hash] = {
    var result = Set[Hash]()
    def getHash1(rs: ResultSet): Hash = Hash.raw(rs.getBytes(1))
    for (hash <- db.query(getHash1 _, "select hash from backups")) {
      result += hash
    }
    result
  }

  private def checkUUID: UUID = {
    db.getProperty("uuid", null) match {
      case null =>
        val uuid = (legacyUUID orElse Some(UUID.randomUUID())).get
        db.setProperty("uuid", uuid.toString)
        db.commit()
        uuid
      case uuidText => UUID.fromString(uuidText)
    }
  }

  // Try to extract the UUID from the legacy sqlite database.  Creates
  // a process to run a query using the sqlite3 interactive query
  // program.  If anything goes wrong, give up.
  private def legacyUUID: Option[UUID] = {
    val sqlite3Db = new File(new File(metaPrefix, ".."), "pool-info.sqlite3")
    if (sqlite3Db.isFile()) {
      val proc = new ProcessBuilder("sqlite3", sqlite3Db.getPath,
        "select value from config where key = 'uuid'")
        .redirectErrorStream(true)
        .start()
      val rd = new BufferedReader(new InputStreamReader(proc.getInputStream))
      val value = rd.readLine()

      // Skip the rest of the output, if for some reason it generated
      // a bunch.
      var tmp = value
      while (tmp ne null) {
        tmp = rd.readLine()
      }

      val status = proc.waitFor()
      rd.close
      if (status == 0)
        Some(UUID.fromString(value))
      else
        None
    } else None
  }
}
