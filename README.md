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

starting from version 1.3.1, jobflow can also be used to extract a range of
lines, e.g.:

    seq 100 | jobflow -skip 10 -count 10  # print lines 11 to 20

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

available command line options
------------------------------

    -skip N -threads N -resume -statefile=/tmp/state -delayedflush
    -delayedspinup N -buffered -joinoutput -limits mem=16M,cpu=10
    -eof=XXX
    -exec ./mycommand {}

-skip N

    N=number of entries to skip
-count N

    N=only process count lines (after skipping)
-threads N (alternative: -j N)

    N=number of parallel processes to spawn
-resume

    resume from last jobnumber stored in statefile
-eof XXX

    use XXX as the EOF marker on stdin
    if the marker is encountered, behave as if stdin was closed
    not compatible with pipe/bulk mode
-statefile XXX

    XXX=filename
    saves last launched jobnumber into a file
-delayedflush

    only write to statefile whenever all processes are busy,
    and at program end
-delayedspinup N

    N=maximum amount of milliseconds
    ...to wait when spinning up a fresh set of processes
    a random value between 0 and the chosen amount is used to delay initial
    spinup.
    this can be handy to circumvent an I/O lockdown because of a burst of
    activity on program startup
-buffered

    store the stdout and stderr of launched processes into a temporary file
    which will be printed after a process has finished.
    this prevents mixing up of output of different processes.
-joinoutput

    if -buffered, write both stdout and stderr into the same file.
    this saves the chronological order of the output, and the combined output
    will only be printed to stdout.
-bulk N

    do bulk copies with a buffer of N bytes. only usable in pipe mode.
    this passes (almost) the entire buffer to the next scheduled job.
    the passed buffer will be truncated to the last line break boundary,
    so jobs always get entire lines to work with.
    this option is useful when you have huge input files and relatively short
    task runtimes. by using it, syscall overhead can be reduced to a minimum.
    N must be a multiple of 4KB. the suffixes G/M/K are detected.
    actual memory allocation will be twice the amount passed.
    note that pipe buffer size is limited to 64K on linux, so anything higher
    than that probably doesn't make sense.
-limits [mem=N,cpu=N,stack=N,fsize=N,nofiles=N]

    sets the rlimit of the new created processes.
    see "man setrlimit" for an explanation. the suffixes G/M/K are detected.
-exec command with args

    everything past -exec is treated as the command to execute on each line of
    stdin received. the line can be passed as an argument using {}.
    {.} passes everything before the last dot in a line as an argument.
    it is possible to use multiple substitutions inside a single argument,
    but currently only of one type.
    if -exec is omitted, input will merely be dumped to stdout (like cat).


BUILD
-----

just run `make`.

you may override variables used in the Makefile and set optimization
CFLAGS and similar thing using a file called `config.mak`, e.g.:

    echo "CFLAGS=-O2 -g" > config.mak
    make -j2
