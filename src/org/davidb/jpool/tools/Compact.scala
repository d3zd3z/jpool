//////////////////////////////////////////////////////////////////////

package org.davidb.jpool.tools

import java.io.File

import org.davidb.logging.Logger

import org.h2.tools.{RunScript, Script, DeleteDbFiles}

object Compact extends AnyRef with Logger {
  def main(args: Array[String]) {
    if (args.length != 2) {
      printf("Usage: compact prefix h2-seen-db-name%n")
      exit(1)
    }

    // Just open the seen database perform an expire, and close it.
    // This should be sufficient to perform the conversion.
    val db = new SeenDb(args(0), args(1))
    db.close()

    // This is out of the H2 manual.
    info("Compacting database")
    val url = "jdbc:h2:" + args(0) + "/" + args(1)
    val file = args(0) + "/" + args(1) + ".sql"
    Script.execute(url, "sa", "", file)
    DeleteDbFiles.execute(args(0), args(1), true)
    RunScript.execute(url, "sa", "", file, null, false)
    new File(file).delete()
  }
}
