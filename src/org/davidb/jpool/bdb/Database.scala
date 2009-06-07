//////////////////////////////////////////////////////////////////////
// Berkeley DB Scala interface.

package org.davidb.jpool.bdb

import com.sleepycat.je

class Database(val db: je.Database, env: Environment) {
  def put(key: Array[Byte], data: Array[Byte]) {
    val ekey = new je.DatabaseEntry(key)
    val edata = new je.DatabaseEntry(data)
    val stat = db.put(env.currentTransaction, ekey, edata)
    if (stat != je.OperationStatus.SUCCESS)
      throw new Exception("Database write error")
  }

  def get(key: Array[Byte]): Option[Array[Byte]] = {
    val ekey = new je.DatabaseEntry(key)
    val edata = new je.DatabaseEntry()
    val stat = db.get(env.currentTransaction, ekey, edata, null)
    if (stat == je.OperationStatus.SUCCESS)
      Some(edata.getData)
    else
      None
  }
}
