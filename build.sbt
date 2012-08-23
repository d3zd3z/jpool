name := "Jpool"

version := "1.0.1-SNAPSHOT"

organization := "org.davidb"

scalaVersion := "2.9.2"

// Although I think I've locked sufficiently, sometimes I get test
// failures without this.
parallelExecution in Test := false

libraryDependencies += "org.scalatest" %% "scalatest" % "1.8" % "test"

libraryDependencies += "commons-codec" % "commons-codec" % "1.6"

libraryDependencies += "com.h2database" % "h2" % "1.3.168"

libraryDependencies += "log4j" % "log4j" % "1.2.17"

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
