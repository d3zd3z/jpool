//////////////////////////////////////////////////////////////////////
// Database of seen files.

package org.davidb.jpool

import java.util.Date
import java.sql.ResultSet

object SeenDbSchema extends DbSchema {
  val schemaVersion = "1:2009-05-30"
  val schemaText = Array(
    "create table seen (pino bigint not null, ino bigint not null," +
    " ctime bigint not null, hash binary(20) not null, expire bigint not null)")
  def schemaUpgrade(oldVersion: String, db: Db) = error("Cannot upgrade schema")
}

// Represents the nodes present in a directory.
case class SeenNode(inode: Long, ctime: Long, hash: Hash, expire: Long) {
  // Use to indicate a not-yet determined expire time.
  def this(inode: Long, ctime: Long, hash: Hash) = this(inode, ctime, hash, -1)
}

class SeenDb(prefix: String, id: String) {
  private val db = new Db(prefix + '/' + id, SeenDbSchema)
  lazy val startTime = new Date().getTime() / 1000L
  private val random = new scala.util.Random()

  // Replace all of the nodes in a given directory (by inode number)
  // with those from the specified sequence.
  def update(parentIno: Long, files: Seq[SeenNode]) {
    db.updateQuery("delete from seen where pino = ?", parentIno)
    for (file <- files) {
      db.updateQuery("insert into seen values (?, ?, ?, ?, ?)",
        parentIno, file.inode, file.ctime, file.hash.getBytes,
        if (file.expire >= 0) file.expire else getExpire)
    }
    db.commit()
  }

  // Retrieve all of the nodes that were last recorded for a given
  // directory (and not expired).
  def getFiles(parentIno: Long): Seq[SeenNode] = {
    db.query(getSeen _, "select (ino, ctime, hash, expire) from seen" +
      " where pino = ? and expire > ?",
      parentIno, startTime).toList
  }

  // Run an expiration.  This can take quite some time, but is needed
  // to purge entries from directories that are no longer present.
  // Files in directories that are present will be purged already.
  def purge() {
    db.updateQuery("delete from seen where expire < ?", startTime)
    db.commit()
  }

  private def getSeen(rs: ResultSet): SeenNode = {
    new SeenNode(rs.getLong(1), rs.getLong(2), Hash.raw(rs.getBytes(3)),
      rs.getLong(4))
  }

  // Get a new expiration time, evenly distributed random numbers
  // between 2 and 6 weeks in the future.
  private def getExpire: Long = {
    random.nextInt(28 * 86400) + (14 * 86400) + startTime
  }

  def close() = db.close()
}
