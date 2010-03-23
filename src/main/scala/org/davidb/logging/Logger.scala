//////////////////////////////////////////////////////////////////////
// My logger trait.

package org.davidb.logging

import org.apache.log4j

object Logger {
  private var setupDone = false
  protected def setupLogger = synchronized {
    if (!setupDone) {
      log4j.BasicConfigurator.configure
      setupDone = true
    }
  }
}

trait Logger {
  private var myLogger = log4j.Logger.getLogger(this.getClass().getName())
  Logger.setupLogger

  /* If you get strange errors with these methods and are using Scala
   * version < 2.7.7, comment out the 'protected' declarations to work
   * around the bug. */
  protected def trace(text: String, args: Any*) { myLogger.trace(text format (args : _*)) }
  protected def debug(text: String, args: Any*) { myLogger.debug(text format (args : _*)) }
  protected def info(text: String, args: Any*) { myLogger.info(text format (args : _*)) }
  protected def warn(text: String, args: Any*) { myLogger.warn(text format (args : _*)) }
  protected def logError(text: String, args: Any*) { myLogger.error(text format (args : _*)) }
  protected def fatal(text: String, args: Any*) { myLogger.fatal(text format (args : _*)) }

  // Queries.
  protected def isTraceEnabled: Boolean = myLogger.isTraceEnabled
  protected def isDebugEnabled: Boolean = myLogger.isDebugEnabled
  protected def isInfoEnabled: Boolean = myLogger.isInfoEnabled
}
