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

class Db(metaPrefix: File) {
  private final val schema = Array(
    "create table config (key varchar not null primary key, value varchar)",
    "create table backups (hash binary(20) not null primary key)")

  private val conn = connectDatabase
  conn.setAutoCommit(false)
  checkSchema
  val uuid = checkUUID

  // These are the various types of fields we can store or retrieve.
  def close() = conn.close()

  // Add a record that the specified backup is present.
  def addBackup(hash: Hash) {
    val b = hash.getBytes()
    updateQuery("delete from backups where hash = ?", b)
    updateQuery("insert into backups values (?)", b)
    conn.commit()
  }

  def getBackups(): Set[Hash] = {
    var result = Set[Hash]()
    def getHash1(rs: ResultSet): Hash = Hash.raw(rs.getBytes(1))
    for (hash <- query(getHash1 _, "select hash from backups"))
      result += hash
    result
  }

  private def makeQuery(sql: String, args: Any*): PreparedStatement = {
    val stmt = conn.prepareStatement(sql)
    var pos = 0
    while (pos < args.size) {
      args(pos) match {
        case b: Array[Byte] => stmt.setBytes(pos + 1, b)
        case s: String => stmt.setString(pos + 1, s)
        case _ => error ("Unsupported type for SQL statement")
      }
      pos += 1
    }
    stmt
  }

  private def updateQuery(sql: String, args: Any*) {
    val stmt = makeQuery(sql, args: _*)
    stmt.executeUpdate()
    stmt.close()
  }

  private def query[A](convert: ResultSet => A,
    sql: String, args: Any*): Iterator[A] =
  {
    val stmt = makeQuery(sql, args: _*)
    val result = stmt.executeQuery()
    new Iterator[A] {
      def hasNext: Boolean = !(result.isLast())
      def next: A = {
        if (result.next()) {
          convert(result)
        } else error("next on empty iterator")
      }
    }
  }
  private def getString1(rs: ResultSet): String = rs.getString(1)

  private def connectDatabase: Connection = {
    Class.forName("org.h2.Driver")
    val url = "jdbc:h2:" + new File(metaPrefix, "meta").getAbsolutePath()
    val user = "sa"
    val pass = ""
    java.sql.DriverManager.getConnection(url, user, pass)
  }

  private def checkSchema {
    val hash = getSchemaHash
    if (tablePresent("CONFIG")) {
      query(getString1, "select value from config where key = 'schema-hash'").toList match {
        case List(dbHash) if hash == dbHash =>
        case List(dbHash) => error("TODO: Implement Schema upgrade")
        case _ => error("TODO: Handle corrupt database (missing hash)")
      }
    } else {
      setSchema(hash)
    }
  }

  private def setSchema(hash: String) {
    println("Creating table")
    schema.foreach(updateQuery(_))

    updateQuery("insert into config values (?, ?)", "schema-hash", hash)
    conn.commit()
  }

  private def getSchemaHash: String = {
    val md = MessageDigest.getInstance("SHA-1")
    for (line <- schema) {
      md.update(line.getBytes("UTF-8"))
      md.update(';'.toByte)
    }
    val digest = md.digest()
    val result = new StringBuilder(digest.size * 2)
    for (b <- digest) {
      result.append("%02x".format(b & 0xff))
    }
    result.toString
  }

  private def checkUUID: UUID = {
    // TODO: Use query.
    val stmt = conn.prepareStatement("select value from config where key = 'uuid'")
    val rs = stmt.executeQuery()
    if (rs.first()) {
      val uuid = UUID.fromString(rs.getString(1))
      stmt.close()
      uuid
    } else {
      val uuid = (legacyUUID orElse Some(UUID.randomUUID())).get
      stmt.close()
      updateQuery("insert into config values ('uuid', ?)", uuid.toString)
      conn.commit()
      uuid
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

  private def tablePresent(name: String): Boolean = {
    val md = conn.getMetaData()
    val res = md.getTables(null, null, name, Array("TABLE"))
    val answer = res.first()
    res.close()
    answer
  }
}
