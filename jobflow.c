/*
MIT License
Copyright (C) 2012-2021 rofl0r

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the “Software”), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/


#define VERSION "1.3.1"


#undef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#undef _XOPEN_SOURCE
#define _XOPEN_SOURCE 700
#undef _GNU_SOURCE
#define _GNU_SOURCE

#include "sblist.h"

#define ARRAY_SIZE(x) (sizeof(x) / sizeof((x)[0]))

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <unistd.h>
#include <stdint.h>
#include <stddef.h>
#include <errno.h>
#include <time.h>
#include <assert.h>
#include <ctype.h>
#include <sys/mman.h>

/* process handling */

#include <fcntl.h>
#include <spawn.h>
#include <sys/wait.h>
#include <sys/stat.h>

#include <sys/resource.h>

#if defined(__GLIBC__) && (__GLIBC__ < 3) && (__GLIBC_MINOR__ < 13)
/* http://repo.or.cz/w/glibc.git/commitdiff/c08fb0d7bba4015078406b28d3906ccc5fda9d5a ,
 * http://repo.or.cz/w/glibc.git/commitdiff/052fa7b33ef5deeb4987e5264cf397b3161d8a01 */
#warning to use prlimit() you have to use musl libc 0.8.4+ or glibc 2.13+
static int prlimit(int pid, ...) {
	(void) pid;
	dprintf(2, "prlimit() not implemented on this system\n");
	errno = EINVAL;
	return -1;
}
#endif

#include <sys/time.h>

/* some small helper funcs from libulz */

static int msleep(long millisecs) {
        struct timespec req, rem;
        req.tv_sec = millisecs / 1000;
        req.tv_nsec = (millisecs % 1000) * 1000 * 1000;
        int ret;
        while((ret = nanosleep(&req, &rem)) == -1 && errno == EINTR) req = rem;
        return ret;
}

static const char ulz_conv_cypher[] =
	"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
#define ulz_conv_cypher_len (sizeof(ulz_conv_cypher) - 1)
static char* ulz_mkdtemp(char* templ) {
	size_t i, l = strlen(templ);
	if(l < 6) {
		errno = EINVAL;
		return NULL;
	}
	loop:
	for(i = l - 6; i < l; i++)
		templ[i] = ulz_conv_cypher[rand() % ulz_conv_cypher_len];
	if(mkdir(templ, S_IRWXU) == -1) {
		if(errno == EEXIST) goto loop;
		return NULL;
	}
	return templ;
}

static size_t gen_fn(char* buf, const char* prefix, size_t pl, const char* tmpdir) {
	size_t tl = strlen(tmpdir);
	size_t a = 0;
	memcpy(buf+a, tmpdir, tl);
	a+=tl;
	memcpy(buf+a,prefix,pl);
	a+=pl;
	memcpy(buf+a,"XXXXXX", 7);
	return a+6;
}

/* calls mkdtemp on /dev/shm and on failure on /tmp, to get the fastest possible
 * storage. returns size of the string returned in buffer */
static size_t mktempdir(const char* prefix, char* buffer, size_t bufsize) {
	size_t ret, pl = strlen(prefix);
	if(bufsize < sizeof("/dev/shm/") -1 + pl + sizeof("XXXXXX")) return 0;
	ret = gen_fn(buffer, prefix, pl, "/dev/shm/");
	if(!ulz_mkdtemp(buffer)) {
		ret = gen_fn(buffer, prefix, pl, "/tmp/");
		if(!ulz_mkdtemp(buffer)) return 0;
	}
	return ret;
}


typedef struct {
	pid_t pid;
	int pipe;
	posix_spawn_file_actions_t fa;
} job_info;

typedef struct {
	int limit;
	struct rlimit rl;
} limit_rec;

typedef struct {
	char temp_state[256];
	char* cmd_argv[4096];
	sblist* job_infos;
	sblist* subst_entries;
	sblist* limits;
	char* tempdir;
	unsigned long long lineno;

	char* statefile;
	char* eof_marker;
	unsigned long numthreads;
	unsigned long threads_running;
	unsigned long skip;
	unsigned long count;
	unsigned long delayedspinup_interval; /* use a random delay until the queue gets filled for the first time.
				the top value in ms can be supplied via a command line switch.
				this option makes only sense if the interval is somewhat smaller than the
				expected runtime of the average job.
				this option is useful to not overload a network app due to hundreds of
				parallel connection tries on startup.
				*/
	unsigned long bulk_bytes;

	bool pipe_mode;
	bool use_seqnr;
	bool buffered; /* write stdout and stderr of each task into a file,
			and print it to stdout once the process ends.
			this prevents mixing up of the output of multiple tasks. */
	bool delayedflush; /* only write to statefile whenever all processes are busy, and at program end.
			   this means faster program execution, but could also be imprecise if the number of
			   jobs is small or smaller than the available threadcount. */
	bool join_output; /* join stdout and stderr of launched jobs into stdout */

	unsigned cmd_startarg;
} prog_state_s;

static prog_state_s prog_state;


extern char** environ;

static int makeLogfilename(char* buf, size_t bufsize, size_t jobindex, int is_stderr) {
	int ret = snprintf(buf, bufsize, "%s/jd_proc_%.5lu_std%s.log",
			   prog_state.tempdir, (unsigned long) jobindex, is_stderr ? "err" : "out");
	return ret > 0 && (size_t) ret < bufsize;
}

static void launch_job(size_t jobindex, char** argv) {
	char stdout_filename_buf[256];
	char stderr_filename_buf[256];
	job_info* job = sblist_get(prog_state.job_infos, jobindex);

	if(job->pid != -1) return;

	if(prog_state.buffered) {
		if((!makeLogfilename(stdout_filename_buf, sizeof(stdout_filename_buf), jobindex, 0)) ||
		   ((!prog_state.join_output) && !makeLogfilename(stderr_filename_buf, sizeof(stderr_filename_buf), jobindex, 1)) ) {
			dprintf(2, "temp filename too long!\n");
			return;
		}
	}

	errno = posix_spawn_file_actions_init(&job->fa);
	if(errno) goto spawn_error;

	errno = posix_spawn_file_actions_addclose(&job->fa, 0);
	if(errno) goto spawn_error;

	int pipes[2];
	if(prog_state.pipe_mode) {
		if(pipe(pipes)) {
			perror("pipe");
			goto spawn_error;
		}
		job->pipe = pipes[1];
		errno = posix_spawn_file_actions_adddup2(&job->fa, pipes[0], 0);
		if(errno) goto spawn_error;
		errno = posix_spawn_file_actions_addclose(&job->fa, pipes[0]);
		if(errno) goto spawn_error;
		errno = posix_spawn_file_actions_addclose(&job->fa, pipes[1]);
		if(errno) goto spawn_error;
	}

	if(prog_state.buffered) {
		errno = posix_spawn_file_actions_addclose(&job->fa, 1);
		if(errno) goto spawn_error;
		errno = posix_spawn_file_actions_addclose(&job->fa, 2);
		if(errno) goto spawn_error;
	}

	if(!prog_state.pipe_mode) {
		errno = posix_spawn_file_actions_addopen(&job->fa, 0, "/dev/null", O_RDONLY, 0);
		if(errno) goto spawn_error;
	}

	if(prog_state.buffered) {
		errno = posix_spawn_file_actions_addopen(&job->fa, 1, stdout_filename_buf, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
		if(errno) goto spawn_error;
		if(prog_state.join_output)
			errno = posix_spawn_file_actions_adddup2(&job->fa, 1, 2);
		else
			errno = posix_spawn_file_actions_addopen(&job->fa, 2, stderr_filename_buf, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
		if(errno) goto spawn_error;
	}

	errno = posix_spawnp(&job->pid, argv[0], &job->fa, NULL, argv, environ);
	if(errno) {
		spawn_error:
		job->pid = -1;
		perror("posix_spawn");
	} else {
		prog_state.threads_running++;
		if(prog_state.limits) {
			limit_rec* limit;
			sblist_iter(prog_state.limits, limit) {
				if(prlimit(job->pid, limit->limit, &limit->rl, NULL) == -1)
					perror("prlimit");
			}
		}
	}
	if(prog_state.pipe_mode)
		close(pipes[0]);
}

static void dump_output(size_t job_id, int is_stderr) {
	char out_filename_buf[256];
	char buf[4096];
	FILE* dst, *out_stream = is_stderr ? stderr : stdout;
	size_t nread;

	makeLogfilename(out_filename_buf, sizeof(out_filename_buf), job_id, is_stderr);

	dst = fopen(out_filename_buf, "r");
	if(dst) {
		while((nread = fread(buf, 1, sizeof(buf), dst))) {
			fwrite(buf, 1, nread, out_stream);
			if(nread < sizeof(buf)) break;
		}
		fclose(dst);
		fflush(out_stream);
		unlink(out_filename_buf);
	}
}

static void write_all(int fd, void* buf, size_t size) {
	size_t left = size;
	const char *p = buf;
	while(1) {
		if(left == 0) break;
		ssize_t n = write(fd, p, left);
		switch(n) {
		case -1:
			if(errno == EINTR) continue;
			else {
				perror("write");
				return;
			}
		default:
			p += n;
			left -= n;
		}
	}
}

static void pass_stdin(char *line, size_t len) {
	static size_t next_child = 0;
	if(next_child >= sblist_getsize(prog_state.job_infos))
		next_child = 0;
	job_info *job = sblist_get(prog_state.job_infos, next_child);
	write_all(job->pipe, line, len);
	next_child++;
}

static void close_pipes(void) {
	size_t i;
	for(i = 0; i < sblist_getsize(prog_state.job_infos); i++) {
		job_info *job = sblist_get(prog_state.job_infos, i);
		close(job->pipe);
	}
}

/* wait till a child exits, reap it, and return its job index for slot reuse */
static size_t reap_child(void) {
	size_t i;
	job_info* job;
	int ret, retval;

	do ret = waitpid(-1, &retval, 0);
	while(ret == -1 || !WIFEXITED(retval));

	for(i = 0; i < sblist_getsize(prog_state.job_infos); i++) {
		job = sblist_get(prog_state.job_infos, i);
		if(job->pid == ret) {
			job->pid = -1;
			posix_spawn_file_actions_destroy(&job->fa);
			prog_state.threads_running--;
			if(prog_state.buffered) {
				dump_output(i, 0);
				if(!prog_state.join_output)
					dump_output(i, 1);
			}
			return i;
		}
	}
	assert(0);
	return -1;
}

static size_t free_slots(void) {
	return prog_state.numthreads - prog_state.threads_running;
}

#define die(...) do { dprintf(2, "error: " __VA_ARGS__); exit(1); } while(0)

static unsigned long parse_human_number(const char* num) {
	unsigned long ret = 0;
	static const unsigned long mul[] = {1024, 1024 * 1024, 1024 * 1024 * 1024};
	const char* kmg = "KMG";
	const char* kmgind, *p;
	ret = atol(num);
	p = num;
	while(isdigit(*(++p)));
	if(*p && (kmgind = strchr(kmg, *p)))
		ret *= mul[kmgind - kmg];
	return ret;
}

static int syntax(void) {
	dprintf(2,
		"jobflow " VERSION " (C) rofl0r\n"
		"------------------------\n"
		"this program is intended to be used as a recipient of another programs output.\n"
		"it launches processes to which the current line can be passed as an argument\n"
		"using {} for substitution (as in find -exec).\n"
		"if no input substitution argument ({} or {.}) is provided, input is piped into\n"
		"stdin of child processes. input will be then evenly distributed to jobs,\n"
		"until EOF is received. we call this 'pipe mode'.\n"
		"\n"
		"available options:\n\n"
		"-skip N -count N -threads N -resume -statefile=/tmp/state -delayedflush\n"
		"-delayedspinup N -buffered -joinoutput -limits mem=16M,cpu=10\n"
		"-eof=XXX\n"
		"-exec ./mycommand {}\n"
		"\n"
		"-skip N\n"
		"    N=number of entries to skip\n"
		"-count N\n"
		"    N=only process count lines (after skipping)\n"
		"-threads N (alternative: -j N)\n"
		"    N=number of parallel processes to spawn\n"
		"-resume\n"
		"    resume from last jobnumber stored in statefile\n"
		"-eof XXX\n"
		"    use XXX as the EOF marker on stdin\n"
		"    if the marker is encountered, behave as if stdin was closed\n"
		"    not compatible with pipe/bulk mode\n"
		"-statefile XXX\n"
		"    XXX=filename\n"
		"    saves last launched jobnumber into a file\n"
		"-delayedflush\n"
		"    only write to statefile whenever all processes are busy,\n"
		"    and at program end\n"
		"-delayedspinup N\n"
		"    N=maximum amount of milliseconds\n"
		"    ...to wait when spinning up a fresh set of processes\n"
		"    a random value between 0 and the chosen amount is used to delay initial\n"
		"    spinup.\n"
		"    this can be handy to circumvent an I/O lockdown because of a burst of \n"
		"    activity on program startup\n"
		"-buffered\n"
		"    store the stdout and stderr of launched processes into a temporary file\n"
		"    which will be printed after a process has finished.\n"
		"    this prevents mixing up of output of different processes.\n"
		"-joinoutput\n"
		"    if -buffered, write both stdout and stderr into the same file.\n"
		"    this saves the chronological order of the output, and the combined output\n"
		"    will only be printed to stdout.\n"
		"-bulk N\n"
		"    do bulk copies with a buffer of N bytes. only usable in pipe mode.\n"
		"    this passes (almost) the entire buffer to the next scheduled job.\n"
		"    the passed buffer will be truncated to the last line break boundary,\n"
		"    so jobs always get entire lines to work with.\n"
		"    this option is useful when you have huge input files and relatively short\n"
		"    task runtimes. by using it, syscall overhead can be reduced to a minimum.\n"
		"    N must be a multiple of 4KB. the suffixes G/M/K are detected.\n"
		"    actual memory allocation will be twice the amount passed.\n"
		"    note that pipe buffer size is limited to 64K on linux, so anything higher\n"
		"    than that probably doesn't make sense.\n"
		"-limits [mem=N,cpu=N,stack=N,fsize=N,nofiles=N]\n"
		"    sets the rlimit of the new created processes.\n"
		"    see \"man setrlimit\" for an explanation. the suffixes G/M/K are detected.\n"
		"-exec command with args\n"
		"    everything past -exec is treated as the command to execute on each line of\n"
		"    stdin received. the line can be passed as an argument using {}.\n"
		"    {.} passes everything before the last dot in a line as an argument.\n"
		"    {#} will be replaced with the sequence (aka line) number.\n"
		"    usage of {#} does not affect the decision whether pipe mode is used.\n"
		"    it is possible to use multiple substitutions inside a single argument,\n"
		"    but only of one type.\n"
		"    if -exec is omitted, input will merely be dumped to stdout (like cat).\n"
		"\n"
	);
	return 1;
}

static int parse_args(unsigned argc, char** argv) {
	unsigned i, j, r = 0;
	static bool resume = 0;
	static char *limits = 0;
	static const struct {
		const char lname[14];
		const char sname;
		const char flag;
		union {
			bool *b;
			unsigned long *i;
			char **s;
		} dest;
	} opt_tab[] = {
		{"threads", 'j', 'i', .dest.i = &prog_state.numthreads },
		{"statefile", 0, 's', .dest.s = &prog_state.statefile },
		{"eof", 0, 's', .dest.s = &prog_state.eof_marker },
		{"skip", 0, 'i', .dest.i = &prog_state.skip },
		{"count", 0, 'i', .dest.i = &prog_state.count },
		{"resume", 0, 'b', .dest.b = &resume },
		{"delayedflush", 0, 'b', .dest.b = &prog_state.delayedflush },
		{"delayedspinup", 0, 'i', .dest.i = &prog_state.delayedspinup_interval },
		{"buffered", 0, 'b', .dest.b =&prog_state.buffered},
		{"joinoutput", 0, 'b', .dest.b =&prog_state.join_output},
		{"bulk", 0, 'i', .dest.i = &prog_state.bulk_bytes},
		{"limits", 0, 's', .dest.s = &limits},
	};

	prog_state.numthreads = 1;
	prog_state.count = -1UL;

	for(i=1; i<argc; ++i) {
		char *p = argv[i], *q = strchr(p, '=');
		if(*(p++) != '-') die("expected option instead of %s\n", argv[i]);
		if(*p == '-') p++;
		if(!*p) die("invalid option %s\n", argv[i]);
		for(j=0;j<ARRAY_SIZE(opt_tab);++j) {
			if(((!p[1] || p[1] == '=') && *p == opt_tab[j].sname) ||
			   (!strcmp(p, opt_tab[j].lname)) ||
			   (q && strlen(opt_tab[j].lname) == q-p && !strncmp(p, opt_tab[j].lname, q-p))) {
				switch(opt_tab[j].flag) {
				case 'b': *opt_tab[j].dest.b=1; break;
				case 'i': case 's':
					if(!q) {
						if(argc <= i+1 || argv[i+1][0] == '-') {
						e_expect_op:;
							die("option %s requires operand\n", argv[i]);
						}
						q = argv[++i];
					} else {
						if(*(++q) == 0) goto e_expect_op;
					}
					if(opt_tab[j].flag == 'i') {
						if(!isdigit(*q))
							die("expected numeric operand for %s at %s\n", p, q);
						*opt_tab[j].dest.i=parse_human_number(q);
					} else
						*opt_tab[j].dest.s=q;
					break;
				}
				break;
			}
		}
		if(j>=ARRAY_SIZE(opt_tab)) {
			if(!strcmp(p, "exec")) {
				r = i+1;
				break;
			} else if(!strcmp(p, "help")) {
				return syntax();
			} else die("unknown option %s\n", argv[i]);
		}
	}

	if((long)prog_state.numthreads <= 0) die("threadcount must be >= 1\n");

	if(resume) {
		if(!prog_state.statefile) die("-resume needs -statefile\n");
		if(access(prog_state.statefile, W_OK | R_OK) != -1) {
			FILE *f = fopen(prog_state.statefile, "r");
			if(f) {
				char nb[64];
				if(fgets(nb, sizeof nb, f)) prog_state.skip = strtoll(nb,0,10);
				fclose(f);
			}
		}
	}

	if(prog_state.delayedflush && !prog_state.statefile)
		die("-delayedflush needs -statefile\n");

	prog_state.pipe_mode = 1;
	prog_state.cmd_startarg = r;
	prog_state.subst_entries = NULL;

	if(r) {
		uint32_t subst_ent;
		if(r < (unsigned) argc) {
			prog_state.cmd_startarg = r;
		} else die("-exec without arguments\n");

		prog_state.subst_entries = sblist_new(sizeof(uint32_t), 16);

		// save entries which must be substituted, to save some cycles.
		for(i = r; i < (unsigned) argc; i++) {
			subst_ent = i - r;
			char *a, *b, *c=0;
			if((a = strstr(argv[i], "{}")) ||
			   (b = strstr(argv[i], "{.}")) ||
			   (c = strstr(argv[i], "{#}"))) {
				if(!c) prog_state.pipe_mode = 0;
				else prog_state.use_seqnr = 1;
				sblist_add(prog_state.subst_entries, &subst_ent);
			}
		}
		if(sblist_getsize(prog_state.subst_entries) == 0) {
			sblist_free(prog_state.subst_entries);
			prog_state.subst_entries = 0;
		}
	}

	if(prog_state.join_output && !prog_state.buffered)
		die("-joinoutput needs -buffered\n");

	if(prog_state.bulk_bytes % 4096)
		die("bulk size must be a multiple of 4096\n");

	if(limits) {
		unsigned i;
		while(1) {
			limits += strspn(limits, ",");
			size_t l = strcspn(limits, ",");
			if(!l) break;
			size_t l2 = strcspn(limits, "=");
			if(l2 >= l) die("syntax error in limits argument\n");
			limit_rec lim;
			if(!prog_state.limits)
				prog_state.limits = sblist_new(sizeof(limit_rec), 4);
			static const struct { int lim_val; const char lim_name[8]; } lim_tab[] = {
				{ RLIMIT_AS, "mem" },
				{ RLIMIT_CPU, "cpu" },
				{ RLIMIT_STACK, "stack" },
				{ RLIMIT_FSIZE, "fsize" },
				{ RLIMIT_NOFILE, "nofiles" },
			};
			for(i=0; i<ARRAY_SIZE(lim_tab);++i)
				if(!strncmp(limits, lim_tab[i].lim_name, l2)) {
					lim.limit = lim_tab[i].lim_val;
					break;
				}
			if(i >= ARRAY_SIZE(lim_tab))
				die("unknown option passed to -limits\n");
			if(getrlimit(lim.limit, &lim.rl) == -1) {
				perror("getrlimit");
				die("could not query rlimits\n");
			}
			lim.rl.rlim_cur = parse_human_number(limits+l2+1);
			sblist_add(prog_state.limits, &lim);
			limits += l;
		}
	}
	return 0;
}

static void init_queue(void) {
	unsigned i;
	job_info ji = {.pid = -1};

	for(i = 0; i < prog_state.numthreads; i++)
		sblist_add(prog_state.job_infos, &ji);
}

static void write_statefile(unsigned long long n, const char* tempfile) {
	int fd = open(tempfile, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
	if(fd != -1) {
		dprintf(fd, "%llu\n", n + 1ULL);
		close(fd);
		if(rename(tempfile, prog_state.statefile) == -1)
			perror("rename");
	} else
		perror("open");
}

static int str_here(char* haystack, size_t hay_size, size_t bufpos,
		    char* needle, size_t needle_size) {
	if(needle_size <= hay_size - bufpos) {
		if(!memcmp(needle, haystack + bufpos, needle_size))
			return 1;
	}
	return 0;
}
// returns numbers of substitutions done, -1 on out of buffer.
// dest is always overwritten. if no substitutions were done, it contains a copy of source.
static int substitute_all(char *dest, ssize_t dest_size,
		   char *src, size_t src_size,
		   char *what, size_t what_size,
		   char *whit, size_t whit_size) {
	size_t i;
	int ret = 0;
	for(i = 0; dest_size > 0 && i < src_size; ) {
		if(str_here(src, src_size, i, what, what_size)) {
			if(dest_size < (ssize_t) whit_size) return -1;
			memcpy(dest, whit, whit_size);
			dest += whit_size;
			dest_size -= whit_size;
			ret++;
			i += what_size;
		} else {
			*dest = src[i];
			dest++;
			dest_size--;
			i++;
		}
	}
	if(!dest_size) return -1;
	*dest = 0;
	return ret;
}

static char* mystrnchr(const char *in, int ch, size_t end) {
	const char *e = in+end;
	const char *p = in;
	while(p != e && *p != ch) p++;
	if(p != e) return (char*)p;
	return 0;
}
static char* mystrnrchr(const char *in, int ch, size_t end) {
	const char *e = in+end-1;
	const char *p = in;
	while(p != e && *e != ch) e--;
	if(*e == ch) return (char*)e;
	return 0;
}
static char* mystrnrchr_chk(const char *in, int ch, size_t end) {
	if(!end) return 0;
	return mystrnrchr(in, ch, end);
}

static int need_linecounter(void) {
	return !!prog_state.skip || prog_state.statefile ||
	       prog_state.use_seqnr || prog_state.count != -1UL;
}
static size_t count_linefeeds(const char *buf, size_t len) {
	const char *p = buf, *e = buf+len;
	size_t cnt = 0;
	while(p < e) {
		if(*p == '\n') cnt++;
		p++;
	}
	return cnt;
}

static int match_eof(char* inbuf, size_t len) {
	if(!prog_state.eof_marker) return 0;
	size_t l = strlen(prog_state.eof_marker);
	return l == len-1 && !memcmp(prog_state.eof_marker, inbuf, l);
}

static inline int islb(int p) { return p == '\n' || p == '\r'; }
static void chomp(char *s, size_t *len) {
	while(*len && islb(s[*len-1])) s[--(*len)] = 0;
}

#define MAX_SUBSTS 16
static int dispatch_line(char* inbuf, size_t len, char** argv) {
	char subst_buf[MAX_SUBSTS][4096];
	static unsigned spinup_counter = 0;

	if(!prog_state.bulk_bytes)
		prog_state.lineno++;
	else if(need_linecounter()) {
		prog_state.lineno += count_linefeeds(inbuf, len);
	}

	if(prog_state.skip) {
		if(!prog_state.bulk_bytes) {
			prog_state.skip--;
			return 1;
		} else {
			while(len && prog_state.skip) {
				char *q = mystrnchr(inbuf, '\n', len);
				if(q) {
					ptrdiff_t diff = (q - inbuf) + 1;
					inbuf += diff;
					len -= diff;
					prog_state.skip--;
				} else {
					return 1;
				}
			}
			if(!len) return 1;
		}
	} else if(prog_state.count != -1UL) {
		if(!prog_state.count) return -1;
		--prog_state.count;
	}

	if(!prog_state.cmd_startarg) {
		write_all(1, inbuf, len);
		return 1;
	}

	if(!prog_state.pipe_mode)
		chomp(inbuf, &len);

	char *line = inbuf;
	size_t line_size = len;

	if(prog_state.subst_entries) {
		unsigned max_subst = 0;
		uint32_t* index;
		sblist_iter(prog_state.subst_entries, index) {
			if(max_subst >= MAX_SUBSTS) break;
			char *source = argv[*index + prog_state.cmd_startarg];
			size_t source_len = strlen(source);
			int ret;
			ret = substitute_all(subst_buf[max_subst], 4096,
					     source, source_len,
					     "{}", 2,
					     line, line_size);
			if(ret == -1) {
				too_long:
				dprintf(2, "fatal: line too long for substitution: %s\n", line);
				return 0;
			} else if(!ret) {
				char* lastdot = mystrnrchr_chk(line, '.', line_size);
				size_t tilLastDot = line_size;
				if(lastdot) tilLastDot = lastdot - line;
				ret = substitute_all(subst_buf[max_subst], 4096,
						     source, source_len,
						     "{.}", 3,
						     line, tilLastDot);
				if(ret == -1) goto too_long;
			}
			if(!ret) {
				char lineno[32];
				sprintf(lineno, "%llu", prog_state.lineno);
				ret = substitute_all(subst_buf[max_subst], 4096,
						     source, source_len,
						     "{#}", 3,
						     lineno, strlen(lineno));
				if(ret == -1) goto too_long;
			}
			if(ret) {
				prog_state.cmd_argv[*index] = subst_buf[max_subst];
				max_subst++;
			}
		}
	}


	if(prog_state.delayedspinup_interval && spinup_counter < (prog_state.numthreads * 2)) {
		msleep(rand() % (prog_state.delayedspinup_interval + 1));
		spinup_counter++;
	}

	if(free_slots())
		launch_job(prog_state.threads_running, prog_state.cmd_argv);
	else if(!prog_state.pipe_mode)
		launch_job(reap_child(), prog_state.cmd_argv);

	if(prog_state.statefile && (prog_state.delayedflush == 0 || free_slots() == 0)) {
		write_statefile(prog_state.lineno, prog_state.temp_state);
	}

	if(prog_state.pipe_mode)
		pass_stdin(line, line_size);

	return 1;
}

int main(int argc, char** argv) {
	unsigned i;

	char tempdir_buf[256];

	srand(time(NULL));

	if(argc > 4096) argc = 4096;

	prog_state.threads_running = 0;

	if(parse_args(argc, argv)) return 1;

	if(prog_state.statefile)
		snprintf(prog_state.temp_state, sizeof(prog_state.temp_state), "%s.%u", prog_state.statefile, (unsigned) getpid());

	prog_state.tempdir = NULL;

	if(prog_state.buffered) {
		prog_state.tempdir = tempdir_buf;
		if(mktempdir("jobflow", tempdir_buf, sizeof(tempdir_buf)) == 0) {
			perror("mkdtemp");
			die("could not create tempdir\n");
		}
	} else {
		/* if the stdout/stderr fds are not in O_APPEND mode,
		   the dup()'s of the fds in posix_spawn can cause different
		   file positions, causing the different processes to overwrite each others output.
		   testcase:
		   seq 100 | ./jobflow.out -threads=100 -exec echo {} > test.tmp ; wc -l test.tmp
		*/
		if(fcntl(1, F_SETFL, O_APPEND) == -1) perror("fcntl");
		if(fcntl(2, F_SETFL, O_APPEND) == -1) perror("fcntl");
	}

	if(prog_state.cmd_startarg) {
		for(i = prog_state.cmd_startarg; i < (unsigned) argc; i++) {
			prog_state.cmd_argv[i - prog_state.cmd_startarg] = argv[i];
		}
		prog_state.cmd_argv[argc - prog_state.cmd_startarg] = NULL;
	}

	prog_state.job_infos = sblist_new(sizeof(job_info), prog_state.numthreads);
	init_queue();

	prog_state.lineno = 0;

	size_t left = 0, bytes_read = 0;
	const size_t chunksize = prog_state.bulk_bytes ? prog_state.bulk_bytes : 16*1024;

	char *mem = mmap(NULL, chunksize*2, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
	char *buf1 = mem;
	char *buf2 = mem+chunksize;
	char *in, *inbuf;

	int exitcode = 1;

	while(1) {
		inbuf = buf1+chunksize-left;
		memcpy(inbuf, buf2+bytes_read-left, left);
		ssize_t n = read(0, buf2, chunksize);
		if(n == -1) {
			perror("read");
			goto out;
		}
		bytes_read = n;
		left += n;
		in = inbuf;
		while(left) {
			char *p;
			if(prog_state.pipe_mode && prog_state.bulk_bytes)
				p = mystrnrchr(in, '\n', left);
			else
				p = mystrnchr (in, '\n', left);

			if(!p) break;
			ptrdiff_t diff = (p - in) + 1;
			if(match_eof(in, diff)) {
				exitcode = 0;
				goto out;
			}
			if(!dispatch_line(in, diff, argv))
				goto out;
			left -= diff;
			in += diff;
		}
		if(!n) {
			if(left && !match_eof(in, left)) dispatch_line(in, left, argv);
			break;
		}
		if(left > chunksize) {
			dprintf(2, "error: input line length exceeds buffer size\n");
			goto out;
		}
	}

	exitcode = 0;

	out:

	if(prog_state.pipe_mode) {
		close_pipes();
	}

	if(prog_state.delayedflush)
		write_statefile(prog_state.lineno - 1, prog_state.temp_state);

	while(prog_state.threads_running) reap_child();

	if(prog_state.subst_entries) sblist_free(prog_state.subst_entries);
	if(prog_state.job_infos) sblist_free(prog_state.job_infos);
	if(prog_state.limits) sblist_free(prog_state.limits);

	if(prog_state.tempdir)
		rmdir(prog_state.tempdir);


	fflush(stdout);
	fflush(stderr);


	return exitcode;
}
