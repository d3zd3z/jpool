# JPool Backup software

Jpool is a suite of backup and archival software written in
[Scala][scala].  It uses a content addressible store to minimize
duplication.

## Dependencies

Jpool is built with the [Simple Build Tool][sbt], version 0.10.1.  The
SBT launcher and a JVM is all you will need to build.  Simply do:

    sbt test
    sbt package

There is a <code>bin/jpool</code> script to run the various commands.

Although the dependencies are spelled out in the SBT project file,
they are as follows:

*   [Scala Test][scalatest] &mdash; test framework for Scala code
*   [Apache Commons Codec][codec] &mdash; used for the Base64 and Hex encoder and decoder.
*   [H2 Database][h2] &mdash; used to store a cache of file hashes based on inode times.
*   [Bouncy Castle][bc] &mdash; cryptography library used when encrypting backups.

## Using

TODO

# Cryptographic Software

Jpool uses cryptographic software to maintain backup security.  Due to
U.S. Exports Regulations, jpool is subject to the following legal
notice:

> This site includes publicly available encryption source code which,
> together with object code resulting from the compiling of publicly
> available source code, may be exported from the United States under
> License Exception "TSU" pursuant to 15 C.F.R. Section 740.13(e).

This notice applies to *cryptographic software only*.  Please see the
[Bureau of Industry and Security][bis] for more information about
current U.S. regulations.

[scala]: http://www.scala-lang.org/
[sbt]: http://code.google.com/p/simple-build-tool/
[scalatest]: http://www.scalatest.org/
[codec]: http://commons.apache.org/codec/
[h2]: http://www.h2database.com/html/main.html
[bc]: http://bouncycastle.org/java.html
[bis]: http://www.bis.doc.gov/
