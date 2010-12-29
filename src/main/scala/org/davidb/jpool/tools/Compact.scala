/**********************************************************************/

package org.davidb.jpool
package tools

import java.io.File

import org.davidb.logging.Loggable

import org.h2.tools.{RunScript, Script, DeleteDbFiles}

object Compact extends AnyRef with Loggable {
  def main(args: Array[String]) {
    if (args.length != 2) {
      printf("Usage: compact prefix h2-seen-db-name%n")
      exit(1)
    }

    // Just open the seen database perform an expire, and close it.
    // This should be sufficient to perform the conversion.
    val db = new SeenDb(args(0), args(1))
    db.purge()
    db.close()

    // This is out of the H2 manual.
    logger.info("Compacting database")
    val url = "jdbc:h2:" + args(0) + "/" + args(1)
    val file = args(0) + "/" + args(1) + ".sql"
    Script.execute(url, "sa", "", file)
    DeleteDbFiles.execute(args(0), args(1), true)
    RunScript.execute(url, "sa", "", file, null, false)
    new File(file).delete()
  }
}
