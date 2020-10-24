#!/bin/sh
test -z "$JF" && JF=./jobflow
TMP=/tmp/jobflow.test.$$
gcc tests/stdin_printer.c -o tests/stdin_printer.out || { error compiling tests/stdin_printer.c ; exit 1 ; }
tmp() {
	echo $TMP.$testno
}
md5() {
	md5sum "$1"|cut -d " " -f 1
}
fs() {
	wc -c "$1"|cut -d " " -f 1
}
equal() {
	test $(md5 "$1") = $(md5 "$2")
}
equal_size() {
	test $(fs "$1") = $(fs "$2")
}
cleanup() {
	rm -f $(tmp).1 $(tmp).2 $(tmp).3 $(tmp).4
}
test_equal() {
	if equal "$1" "$2" ; then
		cleanup
	else
		echo "test $testno failed."
		echo "inspect $(tmp).* for analysis"
	fi
}
test_equal_size() {
	if equal_size "$1" "$2" ; then
		cleanup
	else
		echo "test $testno failed."
		echo "inspect $(tmp).* for analysis"
	fi
}
dotest() {
	[ -z "$testno" ] && testno=0
	testno=$((testno + 1))
	echo "running test $testno ($1)"
}

dotest "seq 10 catmode skip 5"
seq 10 > $(tmp).1
$JF -skip=5 < $(tmp).1 > $(tmp).2
tail -n 5 < $(tmp).1 > $(tmp).3
test_equal $(tmp).2 $(tmp).3

dotest "seq 10000 bulk skip 1337"
seq 10000 | sort -u > $(tmp).1
$JF -bulk=4K -skip=1337 -exec tests/stdin_printer.out < $(tmp).1 | sort -u > $(tmp).2
tail -n $((10000 - 1337)) < $(tmp).1 > $(tmp).3
test_equal $(tmp).2 $(tmp).3

dotest "seq 100000 bulk skip 31337 3x"
seq 100000 | sort -u > $(tmp).1
$JF -bulk=4K -threads=3 -skip=31337 -exec tests/stdin_printer.out < $(tmp).1 | sort -u > $(tmp).2
tail -n $((100000 - 31337)) < $(tmp).1 > $(tmp).3
test_equal $(tmp).2 $(tmp).3

dotest "seq 100 catmode"
seq 100 > $(tmp).1
$JF < $(tmp).1 > $(tmp).2
test_equal $(tmp).1 $(tmp).2

dotest "seq 100 echo"
seq 100 > $(tmp).1
$JF -threads=1 -exec echo {} < $(tmp).1 > $(tmp).2
test_equal $(tmp).1 $(tmp).2

dotest "seq 10000 pipe cat"
seq 10000 | sort -u > $(tmp).1
$JF -threads=1 -exec cat < $(tmp).1 > $(tmp).2
test_equal $(tmp).1 $(tmp).2

dotest "seq 10000 pipe cat 3x"
# since cat reads input in chunks and not in lines, we can
# observe interesting effects: if one of the chunks processed
# by one of the cat instances happens not to end after a newline
# character, the contents of that line will be written up to
# the last character, and then another process will dump its
# stdout, so we'll have a line containing ouput from both
# processes. so it may happen that e.g. one process dumps first
# 2 bytes of string "100", i.e. "10" without newline, then another
# process will write "1\n", so the end result may have "101\n"
# twice, which would get filtered out by sort -u.
seq 10000 > $(tmp).1
$JF -threads=3 -exec cat < $(tmp).1 > $(tmp).2
test_equal_size $(tmp).1 $(tmp).2

dotest "seq 10000 pipe cat buffered 3x"
# same restrictions as above apply, but since we use -buffered
seq 10000 | sort -u > $(tmp).1
$JF -threads=3 -buffered -exec cat < $(tmp).1 | sort -u > $(tmp).2
test_equal $(tmp).1 $(tmp).2

dotest "seq 10000 pipe linecat 3x"
seq 10000 | sort -u > $(tmp).1
$JF -threads=3 -exec tests/stdin_printer.out < $(tmp).1 | sort -u > $(tmp).2
test_equal $(tmp).1 $(tmp).2

dotest "seq 10000 echo 3x"
seq 10000 | sort -u > $(tmp).1
$JF -threads=3 -exec echo {} < $(tmp).1 | sort -u > $(tmp).2
test_equal $(tmp).1 $(tmp).2

RNDLINES=7331

dotest "random skip echo"
od < /dev/urandom | head -n $RNDLINES > $(tmp).1
$JF -threads=1 -skip=$((RNDLINES - 10)) -exec echo {} < $(tmp).1 > $(tmp).2
tail -n 10 < $(tmp).1 > $(tmp).3
test_equal $(tmp).2 $(tmp).3

dotest "random echo"
od < /dev/urandom | head -n $RNDLINES > $(tmp).1
$JF -threads=1 -exec echo {} < $(tmp).1 > $(tmp).2
test_equal $(tmp).1 $(tmp).2

dotest "random echo 2x"
od < /dev/urandom | head -n $RNDLINES > $(tmp).1
$JF -threads=2 -exec echo {} < $(tmp).1 | sort -u > $(tmp).2
test_equal $(tmp).1 $(tmp).2

dotest "random echo 3x"
od < /dev/urandom | head -n $RNDLINES > $(tmp).1
$JF -threads=3 -exec echo {} < $(tmp).1 | sort -u > $(tmp).2
test_equal $(tmp).1 $(tmp).2

dotest "random echo 4x"
od < /dev/urandom | head -n $RNDLINES > $(tmp).1
$JF -threads=4 -exec echo {} < $(tmp).1 | sort -u > $(tmp).2
test_equal $(tmp).1 $(tmp).2

dotest "random echo 17x"
od < /dev/urandom | head -n $RNDLINES > $(tmp).1
$JF -threads=17 -exec echo {} < $(tmp).1 | sort -u > $(tmp).2
test_equal $(tmp).1 $(tmp).2

dotest "random echo buffered 17x"
od < /dev/urandom | head -n $RNDLINES > $(tmp).1
$JF -threads=17 -buffered -exec echo {} < $(tmp).1 | sort -u > $(tmp).2
test_equal $(tmp).1 $(tmp).2

dotest "random pipe"
od < /dev/urandom | head -n $RNDLINES > $(tmp).1
$JF -threads=1 -exec cat < $(tmp).1 > $(tmp).2
test_equal $(tmp).1 $(tmp).2

dotest "random pipe 3x"
od < /dev/urandom | head -n $RNDLINES > $(tmp).1
$JF -threads=3 -exec cat < $(tmp).1 | sort -u > $(tmp).2
test_equal $(tmp).1 $(tmp).2

dotest "random pipe 17x"
od < /dev/urandom | head -n $RNDLINES > $(tmp).1
$JF -threads=17 -exec cat < $(tmp).1 | sort -u > $(tmp).2
test_equal $(tmp).1 $(tmp).2

dotest "random pipe buffered 17x"
od < /dev/urandom | head -n $RNDLINES > $(tmp).1
$JF -threads=17 -buffered -exec tests/stdin_printer.out < $(tmp).1 | sort -u > $(tmp).2
test_equal $(tmp).1 $(tmp).2
