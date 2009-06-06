//////////////////////////////////////////////////////////////////////
// Berkeley DB convenience.

package org.davidb.jpool

import com.sleepycat.je.{
  DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
  OperationStatus }
import java.io.File

object BerkeleyDb {
  def makeEnvironment(path: File): Environment = {
    val conf = new EnvironmentConfig()
    conf.setAllowCreate(true)
    new Environment(path, conf)
  }
}

class BerkeleyDb(env: Environment, name: String) {
  private val conf = new DatabaseConfig()
  conf.setAllowCreate(true)
  private val db = env.openDatabase(null, name, conf)

  def put(key: Array[Byte], data: Array[Byte]) {
    val ekey = new DatabaseEntry(key)
    val edata = new DatabaseEntry(data)
    val stat = db.put(null, ekey, edata)
    if (stat != OperationStatus.SUCCESS)
      throw new Exception("Database write error")
  }

  def get(key: Array[Byte]): Option[Array[Byte]] = {
    val ekey = new DatabaseEntry(key)
    val edata = new DatabaseEntry()
    val stat = db.get(null, ekey, edata, null)
    if (stat == OperationStatus.SUCCESS)
      Some(edata.getData)
    else
      None
  }
}
