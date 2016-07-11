jobflow by rofl0r
=================

this program is inspired by GNU parallel, but has the following differences

 + written in C (orders of magnitude less memory used, a few KB vs 50-60 MB)
 + does not leak memory
 + much faster
 + supports rlimits passed to started processes
 - doesn't support ssh (usage of remote cpus)
 - doesn't support all kinds of argument permutations

basically, it works by processing stdin, launching one process per line.
the actual line can be passed to the started program as an argv.
this allows for easy parallelization of standard unix tasks.

it is possible to save the current processed line, so when the task is killed
it can be continued later.

example usage
-------------

you have a list of things, and a tool that processes a single thing.

    cat things.list | jobflow -threads=8 -exec ./mytask {}

    seq 100 | jobflow -threads=100 -exec echo {}

    cat urls.txt | jobflow -threads=32 -exec wget {}

    find . -name '*.bmp' | jobflow -threads=8 -exec bmp2jpeg {.}.bmp {.}.jpg

run jobflow without arguments to see a list of possible command line options,
and argument permutations.

BUILD
-----

grab the latest release tarball from the releases page, and just run `make`.
it contains all library dependencies.
https://github.com/rofl0r/jobflow/releases

instructions to build from git:

    cd /tmp
    mkdir jobflow-0000
    cd jobflow-0000/
    git clone https://github.com/rofl0r/libulz lib
    git clone https://github.com/rofl0r/jobflow
    git clone https://github.com/rofl0r/rcb
    export PATH=$PATH:/tmp/jobflow-0000/rcb
    ln -s /tmp/jobflow-0000/rcb/rcb.pl /tmp/jobflow-0000/rcb/rcb
    cd jobflow
    CC="gcc -static" CFLAGS="-O0 -g -Wall -Wextra" rcb jobflow.c

