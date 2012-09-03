// Managed backups.

package org.davidb.jpool
package manager

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import scala.sys.process._

class ExecutionError(message: String) extends Exception(message)

object Steps extends Enumeration {
  val CheckPaths, StartSnapshot, MountSnapshot, RunClean, SureUpdate,
    SureWrite, Rsync, Dump = Value
}

object Managed {
  printf("Values: %s\n", Steps.values)

  trait Activity {
    def setup()
    def teardown()
  }

  def main(args: Array[String]) {
    val (confName, host) = args match {
      case Array(file, host) => (file, host)
      case _ => scala.sys.error("Usage: jpool managed file.conf host")
    }

    val conf = new BackupConfig(new File(confName))
    val sys = new conf.System(host)

    val sysNames = sys.fsNames
    val sysInfos = sysNames map { name: String => getManager(sys, name) }

    logRotate(conf)

    var undos = List[(Manager, Steps.Value)]()

    try {
      for (step <- Steps.values) {
        printf("*** setup %s ***\n", step)
        for (info <- sysInfos) {
          info.setup(step)
          undos = (info, step) :: undos
        }
      }
    } finally {
      printf("*** Teardown ***\n")
      while (!undos.isEmpty) {
        val (info, step) = undos.head
        undos = undos.tail
        info.teardown(step)
      }
    }
  }

  def logRotate(conf: BackupConfig) {
    rotate(conf.surelog)
    rotate(conf.rsynclog)
  }

  def rotate(path: String) {
    val f = new File(path)
    if (f.exists) {
      f.renameTo(new File(path + ".bak"))
    }
  }

  def run(args: String*): Unit = run(args, None)

  def run(args: Seq[String], cwd: Option[File]) {
    printf("   : %s%s\n", args.reduceLeft {_ + " " + _},
      cwd.map{" (" + _.getPath + ')'}.getOrElse(""))
    Process(args, cwd)! match {
      case 0 => ()
      case n =>
        throw new ExecutionError("Error running command: %d (%s)".format(n, args))
    }
  }

  def runLogged(args: Seq[String], cwd: Option[File], log: ProcessLogger) {
    printf("   : %s (%s)\n", args.reduceLeft {_ + " " + _},
      cwd.map{_.getPath}.getOrElse("??"))
    Process(args, cwd)!(log) match {
      case 0 => ()
      case n =>
        throw new ExecutionError("Error running command: %d (%s)".format(n, args))
    }
  }

  def getManager(sys: BackupConfig#System, name: String): Manager = {
    val fs = sys.getFs(name)
    fs.style match {
      case "xfs-lvm" => new LvmManager(fs)
      case "plain" => new PlainManager(fs)
      case _ => scala.sys.error("Unknown fs style: '%s'".format(fs.style))
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

// Traits for managers associated with filesystems.
trait FsManager {
  val fs: BackupConfig#System#Fs
  val bc = fs.outer.outer

  val mirror = new File(fs.outer.mirror, fs.volume)
  val regDest = new File(fs.base)

  val gosure = bc.getCommand("gosure")
  val cp = bc.getCommand("cp")
  val rsync = bc.getCommand("rsync")

  val pool = new File(bc.pool)

  def checkFs() {
    if (!regDest.isDirectory)
      sys.error("Backup base directory doesn't exist '%s'".format(regDest.getPath))

    if (!mirror.isDirectory)
      sys.error("Mirror dir doesn't exist '%s'".format(mirror.getPath))

    if (!pool.isDirectory)
      sys.error("Pool dir doesn't exist '%s'".format(pool.getPath))
  }

  def banner(log: ProcessLogger, task: String, dest: File) {
    val fmt = new SimpleDateFormat("yyyy-MM-dd_hh:mm")
    val line = "--- %s of %s (%s) on %s ---".format(task, fs.fsName, dest.getPath, fmt.format(new Date))
    val banner = "-" * line.length
    log.out(banner)
    log.out(line)
    log.out(banner)
  }
}

class LvmManager(val fs: BackupConfig#System#Fs) extends Manager with FsManager {

  val snapDest = new File("/mnt/snap/" + fs.fsName)
  val regVol = new File("/dev/" + fs.outer.vol + '/' + fs.volume)
  val snapVol = new File("/dev/" + fs.outer.vol + '/' + fs.volume + ".snap")

  val lvcreate = bc.getCommand("lvcreate")
  val lvremove = bc.getCommand("lvremove")
  val mount = bc.getCommand("mount")
  val umount = bc.getCommand("umount")

  // Verify that the Mirrors is sane.
  addSetup(Steps.CheckPaths) {
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
      snapVol.getPath, snapDest.getPath)
  }

  addTeardown(Steps.MountSnapshot) {
    Managed.run(umount.getPath, snapDest.getPath)
  }

  addSetup(Steps.RunClean) {
    Managed.run(fs.clean.getPath, snapDest.getPath)
  }

  addSetup(Steps.SureUpdate) {
    Managed.run(List(gosure.getPath, "update"), Some(snapDest))
  }

  addSetup(Steps.SureWrite) {
    val logfile = new File(bc.surelog)
    val log = ProcessLogger(logfile)
    banner(log, "sure", snapDest)
    try {
      Managed.runLogged(List(gosure.getPath, "signoff"), Some(snapDest), log)
    } finally {
      log.close()
    }

    Managed.run(cp.getPath, "-p", snapDest.getPath + "/2sure.dat.gz",
      regDest + "/2sure.dat.gz")
  }

  addSetup(Steps.Rsync) {
    val logfile = new File(bc.rsynclog)
    val log = ProcessLogger(logfile)
    banner(log, "rsync", snapDest)
    try {
      Managed.runLogged(List(rsync.getPath, "-aiH", "--delete",
        snapDest.getPath + '/', mirror.getPath), None, log)
    } finally {
      log.close()
    }
  }

  addSetup(Steps.Dump) {
    tools.Dump.main(Array(pool.getPath, snapDest.getPath, "fs=" + fs.fsName,
      "host=" + fs.outer.host))
  }

}

class PlainManager(val fs: BackupConfig#System#Fs) extends Manager with FsManager {

  addSetup(Steps.CheckPaths) {
    checkFs()
  }

  // TODO: These are almost entirely shared, except for the missing
  // copy at the end, and the directory involved.
  addSetup(Steps.SureUpdate) {
    Managed.run(List(gosure.getPath, "update"), Some(regDest))
  }

  addSetup(Steps.SureWrite) {
    val logfile = new File(bc.surelog)
    val log = ProcessLogger(logfile)
    banner(log, "sure", regDest)
    try {
      Managed.runLogged(List(gosure.getPath, "signoff"), Some(regDest), log)
    } finally {
      log.close()
    }
  }

  // TODO: Sharing, and this might want to have exclusions.
  addSetup(Steps.Rsync) {
    val logfile = new File(bc.rsynclog)
    val log = ProcessLogger(logfile)
    banner(log, "rsync", regDest)
    try {
      Managed.runLogged(List(rsync.getPath, "-aiH", "--delete",
        regDest.getPath + '/', mirror.getPath), None, log)
    } finally {
      log.close()
    }
  }

  addSetup(Steps.Dump) {
    tools.Dump.main(Array(pool.getPath, regDest.getPath, "fs=" + fs.fsName,
      "host=" + fs.outer.host))
  }
}
