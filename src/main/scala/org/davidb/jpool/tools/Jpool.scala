// Jpool single entry main.

package org.davidb.jpool
package tools

import java.net.URI
import pool._

// An option parser.  Although the parser is build mutably, it is
// reusable, and reasonable to initialize as part of class
// initialization.
class Getopt(programName: String) {
  var options = List.empty[Argument]

  abstract class Argument(val long: String, val help: String) {
    def handle(arg: String, rest: List[String]): List[String]

    def handle2(arg: String, rest: List[String])(action: String => Unit): List[String] = {
      rest match {
        case List() =>
          usage("Argument '%s' requires one parameter".format(arg))
        case arg :: rest =>
          action(arg)
          rest
      }
    }

    // TODO: This could do partial matches with some sophistication.
    // It's Also silly to build a string for each match.
    def matches(arg: String): Boolean = arg == long

    // Add self to argument list.
    options = this :: options

    def showHelp() {
      printf("  %-15s      %s\n", long, help)
    }
  }

  def stringArgument(long: String, help: String)(action: String => Unit): Argument = {
    new Argument(long, help) {
      def handle(arg: String, rest: List[String]): List[String] =
        handle2(arg, rest)(action)

      override def showHelp() {
        printf("  %-15s      %s\n", long + " arg", help)
      }
    }
  }

  def usage(problem: Option[String]): Nothing = {
    problem map { printf("%s\n\n", _) }
    printf("Usage: %s [global-options] <command> [<args>]\n\n", programName)
    printf("\noptions:\n")
    for (opt <- options sortBy { _.long }) {
      opt.showHelp()
    }
    sys.exit(1)
  }

  def usage(): Nothing = usage(None)
  def usage(problem: String): Nothing = usage(Some(problem))

  def parse(args: List[String]) {
    def decode(args: List[String]): Unit = args match {
      case List() =>
        empty()
      case key :: rest =>
        val next = options.find { _.matches(key) } match {
          case None =>
            bare(key, rest)
          case Some(arg) =>
            arg.handle(key, rest)
        }
        decode(next)
    }
    decode(args)
  }

  // Process the end of the argument list.  Can be overridden to give
  // differing ending behavior.
  def empty() {}

  // Handle an argument that didn't match.  By default, prints an
  // error message.  Should return the arguments after any have been
  // consumed (for more processing).
  def bare(key: String, rest: List[String]): List[String] = {
    usage("Unknown argument: '%s'".format(key))
  }
}

object Jpool {

  // Configuration entities, readable by other classes.
  var pool: Option[String] = None
  var config = "/etc/jpool.conf"

  def getPool(): ChunkSource = {
    pool match {
      case Some(p) => PoolFactory.getInstance(new URI(p))
      case None =>
        opts.usage("Must specify pool")
    }
  }

  def setPool(path: String) {
    pool match {
      case None => pool = Some(path)
      case Some(_) =>
        opts.usage("Must only set pool once")
    }
  }

  val opts = new Getopt("jpool") {
    var handled = false

    override def empty() {
      if (!handled)
        usage("Must give command")
    }

    def restArgument(long: String, help: String)(action: List[String] => Unit): Argument = {
      new Argument(long, help) {
        def handle(arg: String, rest: List[String]): List[String] = {
          if (handled)
            usage("Not expecting multiple commands")
          handled = true
          // Load config file here, if possible.
          action(rest)
          Nil
        }
      }
    }

    def oldMain(long: String, help: String)(action: Array[String] => Unit): Argument = {
      restArgument(long, help) { args => action(args.toArray) }
    }
  }
  opts.stringArgument("-pool", "Path to storage Pool")(setPool _)
  opts.stringArgument("-config", "Config file, default: /etc/jpool.conf") { c => config = c }
  opts.restArgument("version", "Print program version") { args =>
    printf("Version would be printed here\n")
  }
  opts.restArgument("list", "List backups available in a pool") { ListCommand.entry _ }

  // Compatibility with old commands.
  opts.oldMain("save", "Save a tarfile")(Save.main _)
  opts.oldMain("restore", "Restore a backup")(Restore.main _)
  opts.oldMain("dump", "Perform a backup")(Dump.main _)
  opts.oldMain("show", "Show contents of a backup")(Show.main _)
  opts.oldMain("compact", "Repack an index file")(Compact.main _)
  opts.restArgument("h2", "Run the h2 shell") { args => org.h2.tools.Shell.main(args: _*) }
  opts.restArgument("h2console", "Run the h2 console") { args => org.h2.tools.Console.main(args: _*) }
  opts.oldMain("mkindex", "Generate an index file (testing)")(MkIndex.main _)
  opts.oldMain("check", "Check the hashes of a pool file")(Check.main _)
  opts.oldMain("clone", "Clone a backup")(Clone.main _)
  opts.oldMain("seendb", "Manage the seen database")(SeenDb.main _)
  opts.oldMain("seentotext", "Convert seendb to text")(SeenToText.main _)

  var commands = Map.empty[String, List[String] => Unit]

  def main(args: Array[String]) {
    opts.parse(args.toList)
  }

}
