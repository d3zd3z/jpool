//////////////////////////////////////////////////////////////////////
// General database access.

package org.davidb.jpool

import java.sql.{Connection, Driver, ResultSet, PreparedStatement}

import org.davidb.logging.Logger

// Each database has a particular schema, and potentially the ability
// to upgrade schema versions.
trait DbSchema {
  // A string that describes the version of this schema.
  val schemaVersion: String

  // The SQL statements that will create this schema in a blank
  // database.
  // Note that the Db code itself will create a table called
  // 'properties' mapping strings to strings.
  val schemaText: Array[String]

  // Attempt to upgrade this database to the latest schema version,
  // given the old version.  Can raise an exception if the upgrade is
  // not possible.  In general, should not 'commit' the database,
  // since upon completion the management code will update the version
  // number and do the commit, putting the entire upgrade inside of a
  // single transaction.
  def schemaUpgrade(oldVersion: String, db: Db)
}

class Db(val path: String, schema: DbSchema) extends AnyRef with Logger {
  private val conn = connectDatabase
  conn.setAutoCommit(false)
  checkSchema

  def close() = conn.close()
  def commit() = conn.commit()

  // Perform a query on the database that expects no results.
  def updateQuery(sql: String, args: Any*) {
    val stmt = makeQuery(sql, args: _*)
    stmt.executeUpdate()
    stmt.close()
  }

  // Perform a query on the database.  Returns an iterator over the
  // result set.
  def query[A](convert: ResultSet => A, sql: String, args: Any*): Iterator[A] = {
    val stmt = makeQuery(sql, args: _*)
    val result = stmt.executeQuery()
    iterateResult(convert, result,  stmt)
  }

  def getProperty(key: String, default: String): String = {
    query(getString1 _, "select value from properties where key = ?", key).toList match {
      case List(item) => item
      case Nil => default
      case _ => error("Multiple property entries for %s in properties table" format key)
    }
  }

  // This does _not_ auto commit.
  def setProperty(key: String, value: String) {
    updateQuery("delete from properties where key = ?", key)
    updateQuery("insert into properties values (?, ?)", key, value)
  }

  private def connectDatabase: Connection = {
    Class.forName("org.h2.Driver")
    val url = "jdbc:h2:" + path
    val user = "sa"
    val pass = ""
    java.sql.DriverManager.getConnection(url, user, pass)
  }

  // Useful conversion functions.
  def getString1(rs: ResultSet): String = rs.getString(1)
  def getLong1(rs: ResultSet): Long = rs.getLong(1)
  def getBytes1(rs: ResultSet): Array[Byte] = rs.getBytes(1)

  private def checkSchema {
    val goodVersion = schema.schemaVersion
    if (tablePresent("PROPERTIES")) {
      getProperty("schema-version", null) match {
        case oldVersion if oldVersion == goodVersion =>
        case null => error("TODO: Handle corrupt database (missing version)")
        case oldVersion =>
          // Invoke the upgrade, using the old version.  If it
          // completes without raising an exception, assume the
          // upgrade was successful, and change the version number.
          // In general, the upgrade should be done within a single
          // transaction, with the commit happening here, not in the
          // upgrade.
          schema.schemaUpgrade(oldVersion, this)
          setProperty("schema-version", goodVersion)
          commit()
      }
    } else {
      setSchema(goodVersion)
    }
  }

  private def setSchema(version: String) {
    warn("Creating database: %s", path)
    schema.schemaText.foreach(updateQuery(_))
    updateQuery("create table properties (key varchar not null primary key, value varchar)")
    setProperty("schema-version", version)
    commit()
  }

  // Turn a ResultSet into an iterator, calling the convert on it.  It
  // is important to not use the ResultSet beyond the invocation of
  // convert, since the iterator will have already advanced or even
  // closed it.
  private def iterateResult[A](convert: ResultSet => A,
    result: ResultSet, stmt: PreparedStatement): Iterator[A] =
  {
    // It is necessary to pre-fetch the results, because the ResultSet
    // class has weird behavior on empty results.
    new Iterator[A] {
      var item: Option[A] = _
      private def advance {
        item = if (result.next()) Some(convert(result)) else None
        if (item eq None)
           stmt.close()
      }
      advance

      def hasNext: Boolean = item != None
      def next: A = {
        item match {
          case None => error("next on empty iterator")
          case Some(r) =>
            val result = r
            advance
            result
        }
      }
    }
  }

  private def makeQuery(sql: String, args: Any*): PreparedStatement = {
    val stmt = conn.prepareStatement(sql)
    var pos = 0
    while (pos < args.size) {
      args(pos) match {
        case b: Array[Byte] => stmt.setBytes(pos + 1, b)
        case s: String => stmt.setString(pos + 1, s)
        case i: Int => stmt.setInt(pos + 1, i)
        case l: Long => stmt.setLong(pos + 1, l)
        case _ => error("Unsupported type for SQL statement")
      }
      pos += 1
    }
    stmt
  }

  private def tablePresent(name: String): Boolean = {
    val md = conn.getMetaData()
    val res = md.getTables(null, null, name, Array("TABLE"))
    val answer = res.first()
    res.close()
    answer
  }
}
