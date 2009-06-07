//////////////////////////////////////////////////////////////////////

package org.davidb.jpool.tools

import java.io.File
import java.nio.ByteBuffer
import java.sql.ResultSet

import scala.collection.mutable.ArrayBuffer

object Convert {
  import bdb.Codecs._

  def main(args: Array[String]) {
    if (args.length != 2) {
      printf("Usage: convert h2-seen-db-name db-path%n")
      exit(1)
    }
    val oldDb = new Db(args(0), SeenDbSchema)
    val uuid = args(0).split("/").last
    val newFile = new File(args(1))
    require(newFile.isDirectory)

    val newEnv = bdb.Environment.openEnvironment(newFile)
    newEnv.begin()
    val newDb = newEnv.openDatabase("index-" + uuid)

    var longest = 0
    printf("Convert: %s%n", uuid)
    for (pino <- oldDb.query(getPino _, "select distinct pino from seen")) {
      val totalData = new ArrayBuffer[SeenNode]
      var minExpire = java.lang.Long.MAX_VALUE
      for ((data, expire) <-
        oldDb.query(getSeen _, "select ino, ctime, hash, expire from seen where pino = ?", pino))
      {
        totalData += data
        minExpire = minExpire min expire
        // printf("  %d%n", expire)
        // Pdump.dump(data)
      }
      longest = longest max totalData.length
      // TMP: For analysis of the image, turn the entire buffer into a
      // pattern so we can see how much of the space is actually used.
      // val payload = buf.array()
      // for (i <- 0 until payload.length) {
      //   payload(i) = '*'.toByte
      // }
      // printf("%d (%d)%n", pino, minExpire)
      newDb.put(pino, new SeenArrayEncoder(totalData))
    }
    newEnv.commit()
    newDb.db.close()
    newEnv.env.cleanLog()
    newEnv.env.close()
    printf("%d entries in largest dir%n", longest)
  }

  class SeenArrayEncoder(seens: Seq[SeenNode]) extends bdb.Encodable {
    def encode: ByteBuffer = {
      val len = (3*8 + Hash.HashLength) * seens.length
      val buf = ByteBuffer.allocate(len)
      for (item <- seens) {
        buf.putLong(item.inode)
        buf.putLong(item.ctime)
        buf.put(item.hash.getBytes)
        buf.putLong(item.expire)
      }
      buf.flip()
      buf
    }
  }

  private def getPino(rs: ResultSet): Long = rs.getLong(1)

  private def getSeen(rs: ResultSet): (SeenNode, Long) = {
    val ino = rs.getLong(1)
    val ctime = rs.getLong(2)
    val hash = rs.getBytes(3)
    val expire = rs.getLong(4)
    (SeenNode(ino, ctime, Hash.raw(hash), expire), expire)
  }
}
