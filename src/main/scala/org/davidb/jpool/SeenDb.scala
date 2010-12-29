/**********************************************************************/
// Database of seen files.

package org.davidb.jpool

import java.nio.ByteBuffer
import java.util.Date
import java.sql.ResultSet

import scala.collection.mutable.ArrayBuffer

import org.davidb.logging.Loggable

object SeenDbSchema extends DbSchema with Loggable {
  val schemaVersion = "4:2010-04-30"
  val schemaText = Array(
    "create table seen (pino bigint not null, expire bigint not null," +
    " info binary not null)",
    "create index seen_index on seen(pino)",
    "create index seen_expire_index on seen(expire)")
  def schemaUpgrade(oldVersion: String, db: Db) = oldVersion match {
    case "2:2009-05-30" => upgradeFrom2(db)
    case "3:2009-06-12" => upgradeFrom3(db)
    case _ => error("Cannot upgrade schema")
  }

  private def upgradeFrom2(db: Db) {
    // Table creation seems to happen even in the transaction is
    // aborted.  I'm not sure how atomic all of this stuff really is
    // in H2.
    db.updateQuery("drop table if exists newseen")
    db.updateQuery("create table newseen (pino bigint not null, expire bigint not null," +
      " info binary not null)")
    val total = db.query(db.getLong1 _, "select count(distinct pino) from seen").next
    logger.info("Performing schema upgrade of %s, %d inodes" format (db.path, total))
    val meter = new BackupProgressMeter
    ProgressMeter.register(meter)
    for (pino <- db.query(db.getLong1 _, "select distinct pino from seen")) {
      val nodes = new ArrayBuffer[SeenNode]
      for (node <- db.query(getSeen2 _, "select ino, ctime, hash, expire from seen" +
        " where pino = ?", pino))
      {
        nodes += node
      }
      meter.addNode()
      // info("pino: %d, min-expire: %d", pino, SeenDb.maxExpire(nodes))
      // Pdump.dump(SeenDb.encodeNodes(nodes))
      val (encoded, maxExpire) = SeenDb.encodeNodes(nodes)
      db.updateQuery("insert into newseen values (?, ?, ?)",
        pino, maxExpire, encoded)
    }
    db.updateQuery("drop index seen_index")
    db.updateQuery("drop table seen")
    db.updateQuery("alter table newseen rename to seen")
    db.updateQuery("create index seen_index on seen(pino)")
    ProgressMeter.unregister(meter)
  }

  private def upgradeFrom3(db: Db) {
    // Only change is the addition of an index on the expiration.
    logger.info("Adding seen database index")
    db.updateQuery("create index seen_expire_index on seen(expire)")
    logger.info("Done adding seen database index")
  }

  private def getSeen2(rs: ResultSet): SeenNode = {
    new SeenNode(rs.getLong(1), rs.getLong(2), Hash.raw(rs.getBytes(3)),
      rs.getLong(4))
  }
}

// Represents the nodes present in a directory.
case class SeenNode(inode: Long, ctime: Long, hash: Hash, expire: Long) {
  // Use to indicate a not-yet determined expire time.
  def this(inode: Long, ctime: Long, hash: Hash) = this(inode, ctime, hash, -1)
}

object SeenDb {
  private final val RecordLength = 8 * 3 + Hash.HashLength

  // Computes the encoded node values as well as a maximum expiration.
  def encodeNodes(nodes: Seq[SeenNode]): (Array[Byte], Long) = {
    val buf = ByteBuffer.allocate(RecordLength * nodes.length)
    var maximum = -1L
    for (node <- nodes) {
      val expire = if (node.expire >= 0) node.expire else getExpire
      maximum = maximum max expire

      buf.putLong(node.inode)
      buf.putLong(node.ctime)
      buf.putLong(expire)
      buf.put(node.hash.getBytes)
    }
    (buf.array, maximum)
  }

  def decodeNodes(bytes: Array[Byte]): Seq[SeenNode] = {
    require((bytes.length % RecordLength) == 0)
    val result = new ArrayBuffer[SeenNode]
    val buf = ByteBuffer.wrap(bytes)
    while (buf.remaining > 0) {
      val inode = buf.getLong()
      val ctime = buf.getLong()
      val expire = buf.getLong()
      val tmp = new Array[Byte](Hash.HashLength)
      buf.get(tmp)
      val hash = Hash.raw(tmp)
      if (expire > startTime)
        result += new SeenNode(inode, ctime, hash, expire)
    }
    result
  }

  private lazy val startTime = new Date().getTime() / 1000L
  private val random = new scala.util.Random()

  // Get a new expiration time, evenly distributed random numbers
  // between 2 and 6 weeks in the future.
  private def getExpire: Long = {
    random.nextInt(28 * 86400) + (14 * 86400) + startTime
  }

  // Utility, shows the number of nodes in the seen database, and runs
  // a purge (which will give an exception if the database is not
  // writable).
  def main(args: Array[String]) {
    if (args.length != 2)
      error("Usage: SeenDb prefix id")

    val sdb = new SeenDb(args(0), args(1))
    val expCount = sdb.db.query(sdb.db.getLong1 _, "select count(*) from seen where expire < 0")
    printf("bogus expires: %d\n", expCount.next)
    val count = sdb.db.query(sdb.db.getLong1 _, "select count(*) from seen")
    printf("total expires: %d\n", count.next)
    sdb.purge()
    sdb.close()
  }
}

class SeenDb(prefix: String, id: String) {
  private val db = new Db(prefix + '/' + id, SeenDbSchema)

  // Replace all of the nodes in a given directory (by inode number)
  // with those from the specified sequence.
  def update(parentIno: Long, files: Seq[SeenNode]) {
    db.updateQuery("delete from seen where pino = ?", parentIno)
    if (!files.isEmpty) {
      val (encoded, maxExpire) = SeenDb.encodeNodes(files)
      db.updateQuery("insert into seen values (?, ?, ?)",
        parentIno, maxExpire, encoded)
    }
    db.commit()
  }

  // Retrieve all of the nodes that were last recorded for a given
  // directory (and not expired).
  def getFiles(parentIno: Long): Seq[SeenNode] = {
    db.query(db.getBytes1 _, "select info from seen" +
      " where pino = ?", parentIno).toList match
    {
      case List(node) => SeenDb.decodeNodes(node)
      case Nil => new Array[SeenNode](0)
      case _ => error("Duplicate database entries for seen nodes")
    }
  }

  // Run an expiration.  This can take quite some time, but is needed
  // to purge entries from directories that are no longer present.
  // Files in directories that are present will be purged already.
  def purge() {
    db.updateQuery("delete from seen where expire < ?", SeenDb.startTime)
    db.commit()
  }

  def close() = db.close()
}
