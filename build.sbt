name := "Jpool"

version := "1.0.1-SNAPSHOT"

organization := "org.davidb"

scalaVersion := "2.9.0"

// Although I think I've locked sufficiently, sometimes I get test
// failures without this.
parallelExecution in Test := false

libraryDependencies += "org.scalatest" %% "scalatest" % "1.6.1" % "test"

libraryDependencies += "commons-codec" % "commons-codec" % "1.4"

libraryDependencies += "com.h2database" % "h2" % "1.2.147"

libraryDependencies += "org.bouncycastle" % "bcprov-jdk16" % "1.45"

libraryDependencies += "log4j" % "log4j" % "1.2.16"

// We need the managed code for the 'run' scripts to work.
retrieveManaged := true

scalacOptions += "-deprecation"

// TODO: Set the desired library, instead of hardcoding it into the
// plugin.
seq(NativePlugin.newSettings : _*)
