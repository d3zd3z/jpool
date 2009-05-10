#! /bin/sh

set -e

# This script 'may' be envoked with EPATH set, which contains the path
# to use for the tests.  This needs to be turned into a classpath for
# the main code (but not for the tests).

JPOOL=`echo ${EPATH:-.} | sed -e 's/:.*//'`/target/jpool.jar
TESTS=`echo ${SPATH:-.} | sed -e 's/:.*//'`/target/jpool-tests.jar
LIB=`echo ${SPATH:-.} | sed -e 's/:.*//'`/lib/'*'

echo ${JPOOL}:${TESTS}:${LIB}

# Invoke tests.
scala -classpath ${JPOOL}:${TESTS}:${LIB} \
	org.scalatest.AegisRunner "$@"

exit 0
