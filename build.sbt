name := "Jpool"

version := "1.0.1-SNAPSHOT"

organization := "org.davidb"

scalaVersion := "2.9.0-1"

// Although I think I've locked sufficiently, sometimes I get test
// failures without this.
parallelExecution in Test := false

// Scalatest was not rebuilt for 2.9.0-1, so use the 2.9.0 version.
libraryDependencies += "org.scalatest" % "scalatest_2.9.0" % "1.6.1" % "test"

libraryDependencies += "commons-codec" % "commons-codec" % "1.4"

libraryDependencies += "com.h2database" % "h2" % "1.2.147"

libraryDependencies += "org.bouncycastle" % "bcprov-jdk16" % "1.45"

libraryDependencies += "log4j" % "log4j" % "1.2.16"

scalacOptions += "-deprecation"

// TODO: Set the desired library, instead of hardcoding it into the
// plugin.
seq(NativePlugin.newSettings : _*)

// Put the classpath needed to run the application into a file
// 'target/classpath.sh'.  The run script can source this to get a
// classpath appropriate to run the application.
// TODO: Figure out how to run this automatically.  Currently, this
// must be run by hand the first time, and any time the dependencies
// change.
TaskKey[Unit]("mkrun") <<=
  (fullClasspath in Runtime,
   target) map {
  (cp: Classpath,
   target: File) =>
    val script = "classpath='" + cp.files.absString + "'"
    val out = target / "classpath.sh"
    IO.write(out, script)
}
