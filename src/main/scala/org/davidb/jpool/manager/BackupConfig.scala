// Configurations for the backup manager.

package org.davidb.jpool
package manager

import java.io.File
import com.typesafe.config._


class BackupConfig(path: File) { confThis =>
  import scala.collection.JavaConversions._

  def load(path: File): Config = {
    if (!path.isFile) {
      sys.error("Config file doesn't exist: '%s'".format(path.getName))
    }
    val local = ConfigFactory.parseFile(path)
    ConfigFactory.load(local)
  }

  private val conf = load(path)

  // Lookup a command.
  def getCommand(name: String): File = {
    // Try using the config first.
    val foundName = try {
      conf.getString("commands." + name)
    } catch {
      case _: ConfigException.Missing =>
        sys.error("TODO: Search path for command")
    }

    val result = new File(foundName)
    if (!result.canExecute)
      sys.error("Command doesn't seem to be executable: '%s'"
        .format(foundName))
    result
  }

  class System(name: String) { sysThis =>
    def outer = confThis
    val config = conf.getConfig(name)
    val vol = config.getString("vol")
    val host = config.getString("host")
    val mirror = config.getString("mirror")
    val fsNames = config.getStringList("fsNames").toList
    val fs = config.getConfig("fs")

    class Fs(val fsName: String) {
      def outer = sysThis
      val fsConfig = fs.getConfig(fsName)
      val volume = fsConfig.getString("volume")
    }
    def getFs(fsName: String) = new Fs(fsName)
  }

}

object BackupConfig {
  def main(args: Array[String]) {
    val conf = new BackupConfig(new File("test.conf"))
    val sys: BackupConfig#System = new conf.System("a64")
    printf("%s\n", sys.fsNames)
  }
}
