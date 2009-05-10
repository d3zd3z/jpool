/* Test runner. */
/* Invokes a series of tests with given names, generating the result
 * file that Aegis expects. */

/* Ok.  This is very annoying.  I have to put this code _inside_ of
 * the scalatest package, because vast parts of !@#$ Scalatest are
 * private[scalatest]. */

package org.scalatest;

import java.io.FileWriter
import scala.collection.mutable.ListBuffer

object AegisRunner {
  val GetName = """.*/test/(.*)\.scala""".r

  class SavingReporter extends StandardOutReporter {
    private var succeeded = 0
    private var failed = 0
    private var ignored = 0

    override def testSucceeded(report: Report) {
      succeeded += 1
      super.testSucceeded(report)
    }
    override def testIgnored(report: Report) {
      ignored += 1
      super.testIgnored(report)
    }
    override def testFailed(report: Report) {
      failed += 1
      super.testFailed(report)
    }

    // Return the aegis style status.
    def aegisStatus: Int = {
      if (failed > 0)
        1
      else if (succeeded > 0)
        0
      else
        2
    }
  }

  def main(args: Array[String]) {
    val ofile = args(0)
    val results = new ListBuffer[(String, Int)]

    for (test <- args.toList.tail) {
      test match {
        case GetName(name) =>
          printf("TestRunner: %s%n", name)
          val suiteClass = Class.forName(name.replace('/', '.'))
          val suite = suiteClass.newInstance.asInstanceOf[Suite]
          val reporter = new SavingReporter
          suite.execute(None, reporter, new Stopper {}, Set(), Set("org.scalatest.Ignore"), Map(), None)

          results += (test, reporter.aegisStatus)
        case _ =>
          printf("Unknown test name: %s%n", test)
          exit(1)
      }
    }

    val fd = new FileWriter(ofile)
    fd.write("test_result = [\n")
    for ((name, status) <- results) {
      fd.write("  { file_name = \"%s\"; exit_status = %d; },\n" format (name, status))
      // print("  { file_name = \"%s\"; exit_status = %d; },\n" format (name, status))
    }
    fd.write("];")
    fd.close()
  }
}
