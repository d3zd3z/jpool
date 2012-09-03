// Managed backups.

package org.davidb.jpool
package manager

import java.io.File

import scala.sys.process._

class ExecutionError(message: String) extends Exception(message)

object Steps extends Enumeration {
  val CheckPaths, StartSnapshot, MountSnapshot, RunClean, SureUpdate,
    SureWrite = Value
}

object Managed {
  printf("Values: %s\n", Steps.values)

  trait Activity {
    def setup()
    def teardown()
  }

  def main(args: Array[String]) {
    // val confName = args match {
    //   case Array(file) => file
    //   case _ => scala.sys.error("Usage: jpool managed file.conf")
    // }

    val conf = new BackupConfig(new File("simple.conf"))
    val sys = new conf.System("simple")

    val sysNames = sys.fsNames
    val sysInfos = sysNames map { name: String => new LvmManager(sys.getFs(name)) }

    var undos = List[(Manager, Steps.Value)]()

    for (step <- Steps.values) {
      printf("*** setup %s ***\n", step)
      for (info <- sysInfos) {
        info.setup(step)
        undos = (info, step) :: undos
      }
    }

    printf("*** Teardown ***\n")
    while (!undos.isEmpty) {
      val (info, step) = undos.head
      undos = undos.tail
      info.teardown(step)
    }
  }

  def run(args: String*) {
    printf("run: %s\n", args.reduceLeft {_ + " " + _})
    args! match {
      case 0 => ()
      case n =>
        throw new ExecutionError("Error running command: %d (%s)".format(n, args))
    }
  }

  def run(args: Seq[String], cwd: Option[File]) {
    printf("run: %s (%s)\n", args.reduceLeft {_ + " " + _},
      cwd.map{_.getPath}.getOrElse("??"))
    Process(args, cwd)! match {
      case 0 => ()
      case n =>
        throw new ExecutionError("Error running command: %d (%s)".format(n, args))
    }
  }
}

trait Manager {
  private var setups = Map.empty[Steps.Value, () => Unit]
  private var teardowns = Map.empty[Steps.Value, () => Unit]

  def setup(step: Steps.Value) = setups.get(step) map {_ ()}
  def teardown(step: Steps.Value) = teardowns.get(step) map {_ ()}

  def addSetup(step: Steps.Value)(action: => Unit) = setups += (step -> (() => action))
  def addTeardown(step: Steps.Value)(action: => Unit) = teardowns += (step -> (() => action))
}

// TODO: Handle direct filesystem (such as /boot).

class LvmManager(fs: BackupConfig#System#Fs) extends Manager {

  val mirror = new File(fs.outer.mirror, fs.volume)
  val snapDest = new File("/mnt/snap/" + fs.fsName)
  val regVol = new File("/dev/" + fs.outer.vol + '/' + fs.volume)
  val snapVol = new File("/dev/" + fs.outer.vol + '/' + fs.volume + ".snap")

  val bc = fs.outer.outer

  val lvcreate = bc.getCommand("lvcreate")
  val lvremove = bc.getCommand("lvremove")
  val mount = bc.getCommand("mount")
  val umount = bc.getCommand("umount")
  val clean = bc.getCommand("clean.sh")
  val gosure = bc.getCommand("gosure")

  // Verify that the Mirrors is sane.
  addSetup(Steps.CheckPaths) {
    if (!mirror.isDirectory)
      sys.error("Mirror dir doesn't exist '%s'".format(mirror.getPath))

    if (!snapDest.isDirectory)
      sys.error("Snapshot destination doesn't exist '%s'".format(snapDest.getPath))
  }

  // Create the snapshot.
  addSetup(Steps.StartSnapshot) {
    Managed.run(lvcreate.getPath, "-L", "5g", "-n", fs.volume + ".snap",
      "-s", "/dev/" + fs.outer.vol + '/' + fs.volume)
  }

  addTeardown(Steps.StartSnapshot) {
    Managed.run(lvremove.getPath, "-f", snapVol.getPath)
  }

  addSetup(Steps.MountSnapshot) {
    Managed.run(mount.getPath, "-o", "nouuid",
      regVol.getPath, snapDest.getPath)
  }

  addTeardown(Steps.MountSnapshot) {
    Managed.run(umount.getPath, snapDest.getPath)
  }

  addSetup(Steps.RunClean) {
    Managed.run(clean.getPath)
  }

  addSetup(Steps.SureUpdate) {
    // TODO: Stick results in log file.
    Managed.run(List(gosure.getPath, "update"), Some(snapDest))
  }

}
