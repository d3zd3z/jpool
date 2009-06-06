//////////////////////////////////////////////////////////////////////
// Berkeley DB convenience.

package org.davidb.jpool

import com.sleepycat.je.{
  DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
  OperationStatus, Transaction }
import java.io.File

object BerkeleyDb {
  def makeEnvironment(path: File): Environment = {
    val conf = new EnvironmentConfig()
    conf.setAllowCreate(true)
    conf.setTransactional(true)
    new Environment(path, conf)
  }
}

// TODO: Sharing transactions across databases.
// TODO: Performing the open within a transaction.

class BerkeleyDb(env: Environment, name: String) {
  private val conf = new DatabaseConfig()
  conf.setAllowCreate(true)
  conf.setTransactional(true)

  private val db = env.openDatabase(null, name, conf)

  private var currentTransaction: Transaction = null
  def transaction: Transaction = currentTransaction

  def begin() {
    require(currentTransaction == null)
    currentTransaction = env.beginTransaction(null, null)
  }

  def commit() {
    require(currentTransaction != null)
    currentTransaction.commit()
    currentTransaction = null
  }

  def abort() {
    require(currentTransaction != null)
    currentTransaction.abort()
    currentTransaction = null
  }

  def put(key: Array[Byte], data: Array[Byte]) {
    val ekey = new DatabaseEntry(key)
    val edata = new DatabaseEntry(data)
    val stat = db.put(currentTransaction, ekey, edata)
    if (stat != OperationStatus.SUCCESS)
      throw new Exception("Database write error")
  }

  def get(key: Array[Byte]): Option[Array[Byte]] = {
    val ekey = new DatabaseEntry(key)
    val edata = new DatabaseEntry()
    val stat = db.get(currentTransaction, ekey, edata, null)
    if (stat == OperationStatus.SUCCESS)
      Some(edata.getData)
    else
      None
  }
}
