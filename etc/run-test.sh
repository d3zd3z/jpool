#! /bin/sh

# This script 'may' be envoked with EPATH set, which contains the path
# to use for the tests.  This needs to be turned into a classpath for
# the main code (but not for the tests).

JPOOL=`echo ${EPATH:-.} | sed -e 's/:.*//'`/target/jpool.jar
TESTS=`echo ${SPATH:-.} | sed -e 's/:.*//'`/target/jpool-tests.jar
LIB=`echo ${SPATH:-.} | sed -e 's/:.*//'`/lib/'*'

echo ${JPOOL}:${TESTS}:${LIB}

genfailure() {
	# Unfortunately, at this point, we only know that things
	# failed to link.  Ideally, we would split these things apart
	# and use a dynamic classloader to load them individually, so
	# we could detect failures.
	echo "*** Warning: Mass failing all tests"
	desc="$1"
	echo "test_result = [" > ${desc}
	shift
	for t in "$@"; do
		echo "  {file_name = "\""${t}"\"";" >>${desc}
		echo "  exit_status = 1;}," >> ${desc}
	done
	echo "];" >> ${desc}
	# cat ${desc}
	exit 0
}

# Invoke tests.
scala -classpath ${JPOOL}:${TESTS}:${LIB} \
	org.scalatest.AegisRunner "$@" || genfailure "$@"
exit 0
