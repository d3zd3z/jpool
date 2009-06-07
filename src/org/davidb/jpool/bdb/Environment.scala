//////////////////////////////////////////////////////////////////////
// Berkeley DB Scala interface.

package org.davidb.jpool.bdb

import com.sleepycat.je
import java.io.File

//////////////////////////////////////////////////////////////////////
// The Environment class wraps a BerkeleyDb environment, but also
// allows tracks a single transaction that can be shared through
// multiple databases.

object Environment {
  def openEnvironment(path: File): Environment = {
    val conf = new je.EnvironmentConfig()
    conf.setAllowCreate(true)
    conf.setTransactional(true)
    val env = new je.Environment(path, conf)
    new Environment(env)
  }
}

class Environment private (val env: je.Environment) {
  private[bdb] var currentTransaction: je.Transaction = null

  def begin() = synchronized {
    require(currentTransaction == null)
    currentTransaction = env.beginTransaction(null, null)
  }

  def commit() = synchronized {
    require(currentTransaction != null)
    currentTransaction.commit()
    currentTransaction = null
  }

  def abort() {
    require(currentTransaction != null)
    currentTransaction.abort()
    currentTransaction = null
  }

  private val dbConf = new je.DatabaseConfig()
  dbConf.setAllowCreate(true)
  dbConf.setTransactional(true)

  def openDatabase(name: String): Database = {
    val db = env.openDatabase(currentTransaction, name, dbConf)
    new Database(db, this)
  }
}
