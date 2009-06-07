//////////////////////////////////////////////////////////////////////
// Berkeley DB Scala interface.

package org.davidb.jpool.bdb

import com.sleepycat.je
import java.nio.ByteBuffer

class Database(val db: je.Database, env: Environment) {

  def rawPut(key: Array[Byte], data: Array[Byte]) {
    val ekey = new je.DatabaseEntry(key)
    val edata = new je.DatabaseEntry(data)
    val stat = db.put(env.currentTransaction, ekey, edata)
    if (stat != je.OperationStatus.SUCCESS)
      throw new Exception("Database write error")
  }

  def rawGet(key: Array[Byte]): Option[Array[Byte]] = {
    val ekey = new je.DatabaseEntry(key)
    val edata = new je.DatabaseEntry()
    val stat = db.get(env.currentTransaction, ekey, edata, null)
    if (stat == je.OperationStatus.SUCCESS)
      Some(edata.getData)
    else
      None
  }

  def put(key: Encodable, data: Encodable) {
    val ekey = entryOf(key)
    val edata = entryOf(data)
    val stat = db.put(env.currentTransaction, ekey, edata)
    if (stat != je.OperationStatus.SUCCESS)
      throw new Exception("Database write error")
  }

  def get[A](key: Encodable, decoder: Decoder[A]): Option[A] = {
    val ekey = entryOf(key)
    val edata = new je.DatabaseEntry()
    val stat = db.get(env.currentTransaction, ekey, edata, null)
    if (stat == je.OperationStatus.SUCCESS)
      Some(decoder.decode(ByteBuffer.wrap(edata.getData)))
    else
      None
  }

  private def entryOf(item: Encodable): je.DatabaseEntry = {
    val buf = item.encode
    new je.DatabaseEntry(buf.array, buf.arrayOffset + buf.position, buf.remaining)
  }
}
