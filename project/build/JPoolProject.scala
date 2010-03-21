import sbt._
import sbt.FileUtilities._
import Process._

class JPoolProject(info: ProjectInfo) extends DefaultProject(info) {
  override def ivyXML =
    <dependencies>
      <dependency org="log4j" name="log4j" rev="1.2.15">
        <exclude org="com.sun.jdmk"/>
        <exclude org="com.sun.jmx"/>
        <exclude org="javax.mail"/>
        <exclude org="javax.activation"/>
        <exclude org="javax.jms"/>
      </dependency>
    </dependencies>

  val scalatest = "org.scalatest" % "scalatest" % "1.0" % "test"
  val codec = "commons-codec" % "commons-codec" % "1.3"
  val h2 = "com.h2database" % "h2" % "1.2.131"

  // Compiling native code.  The work is done by a regular makefile.
  lazy val native = task {
    val cp = runClasspath +++ mainDependencies.scalaJars
    val home = System.getProperty("java.home")
    val p = "make" :: "-f" :: "Makefile.native" ::
      "CLASSPATH=" + Path.makeString(cp.get) ::
      "HOME=" + home ::
      "COMPILE_PATH=" + mainCompilePath ::
      Nil
    val status = Process(p) ! log
    if (status == 0)
      None
    else
      Some("Make error: " + status)
  } dependsOn(compile)
  override def testAction = super.testAction dependsOn(native)

  // The native library will look for this property to load an
  // explicit path.
  System.setProperty("linux.lib", (path("target") / "so" / "liblinux.so").absolutePath)
}
