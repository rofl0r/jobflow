jobflow by rofl0r
=================

this program is inspired by the functionality of GNU parallel, but tries
to keep low overhead and follow the UNIX philosophy of doing one thing well.

how it works
------------

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

Comparison with GNU parallel
----------------------------

GNU parallel is written in perl, which has the following disadvantages:
- requires a perl installation
  even though most people already have perl installed anyway, installing it
  just for this purpose requires up to 50 MB storage (and potentially up to
  several hours of time to compile it from source on slow devices)
- requires a lot of time on startup (parsing sources, etc)
- requires a lot of memory (typically between 5-60 MB)

jobflow OTOH is written in C, which has numerous advantages.
- once compiled to a tiny static binary, can be used without 3rd party stuff
- very little and constant memory usage (typically a few KB)
- no startup overhead
- much higher execution speed

apart from the chosen language and related performance differences, the
following other differences exist between GNU parallel and jobflow:

+ supports rlimits passed to started processes
- doesn't support ssh (usage of remote cpus)
- doesn't support all kinds of argument permutations:
  while GNU parallel has a rich set of options to permute the input,
  this doesn't adhere to the UNIX philosophy.
  jobflow can achieve the same result by passing the unmodified input
  to a user-created script that does the required permutations with other
  standard tools.

BUILD
-----

just run `make`.

you may override variables used in the Makefile and set optimization
CFLAGS and similar thing using a file called `config.mak`, e.g.:

    echo "CFLAGS=-O2 -g" > config.mak
    make -j2
