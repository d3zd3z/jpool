// Native plugin

import sbt._
import sbt.Keys._
import java.util.UUID

object NativePlugin extends Plugin
{
  val blort = SettingKey[String]("blort")

  val native = TaskKey[Seq[File]]("native", "Compile native sources")

  var lastTmpLib: Option[File] = None
  def runCleanup {
    lastTmpLib foreach { x =>
      x.delete()
      lastTmpLib = None
    }
  }

  class Cleanup extends Thread {
    override def run() {
      runCleanup
    }
  }

  val newSettings = Seq(
    blort := "blort",

    // This generator needs the compilation to have finished (but
    // doesn't use the results), this could be done with depends on as
    // well.
    resourceGenerators in Compile <+= (resourceManaged in Compile, sourceDirectory, classDirectory in Compile,
      managedClasspath in Compile, compile in Compile) map
    { (dir, src, classDir, runPath, _) =>
      runCleanup
      val home = System.getProperty("java.home")
      val basePath = runPath.map(_.data.toString).reduceLeft(_ + ":" + _)
      val classpath = classDir.toString + ":" + basePath
      val result = (Process(
        "make" :: "-f" :: "Makefile.native" :: Nil,
        None,
        "COMPILE_PATH" -> classDir.toString,
        "CLASSPATH" -> classpath,
        "JAVA_HOME" -> home
        ) !)

      if (result != 0)
        error("Error compiling native library")

      // Make a temporary copy of the .so
      val tmpSrc = file("target/so/liblinux.so")
      val tmpLib = file("/tmp/") / (UUID.randomUUID().toString + ".so")
      lastTmpLib = Some(tmpLib)
      IO.copyFile(tmpSrc, tmpLib)
      System.setProperty("linux.lib", tmpLib.absolutePath)

      Seq(tmpSrc)
    }
  )
}
