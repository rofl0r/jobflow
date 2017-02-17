/*
Copyright (C) 2012,2014,2016 rofl0r

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/


#define VERSION "1.1.1"


#undef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#undef _XOPEN_SOURCE
#define _XOPEN_SOURCE 700
#undef _GNU_SOURCE
#define _GNU_SOURCE

#include "../lib/include/optparser.h"
#include "../lib/include/stringptr.h"
#include "../lib/include/stringptrlist.h"
#include "../lib/include/sblist.h"
#include "../lib/include/strlib.h"
#include "../lib/include/timelib.h"
#include "../lib/include/filelib.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <stddef.h>
#include <errno.h>
#include <time.h>
#include <assert.h>
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
	unsigned long long lineno;
	unsigned numthreads;
	unsigned threads_running;
	char* statefile;
	unsigned long long skip;
	sblist* job_infos;
	sblist* subst_entries;
	sblist* limits;
	unsigned cmd_startarg;
	char* tempdir;
	int delayedspinup_interval; /* use a random delay until the queue gets filled for the first time.
				the top value in ms can be supplied via a command line switch.
				this option makes only sense if the interval is somewhat smaller than the
				expected runtime of the average job.
				this option is useful to not overload a network app due to hundreds of
				parallel connection tries on startup.
				*/
	int buffered:1; /* write stdout and stderr of each task into a file,
			and print it to stdout once the process ends.
			this prevents mixing up of the output of multiple tasks. */
	int delayedflush:1; /* only write to statefile whenever all processes are busy, and at program end.
			   this means faster program execution, but could also be imprecise if the number of
			   jobs is small or smaller than the available threadcount. */
	int join_output:1; /* join stdout and stderr of launched jobs into stdout */
	int pipe_mode:1;
	size_t bulk_bytes;
} prog_state_s;

prog_state_s prog_state;


extern char** environ;

int makeLogfilename(char* buf, size_t bufsize, size_t jobindex, int is_stderr) {
	int ret = snprintf(buf, bufsize, "%s/jd_proc_%.5lu_std%s.log",
			   prog_state.tempdir, (unsigned long) jobindex, is_stderr ? "err" : "out");
	return ret > 0 && (size_t) ret < bufsize;
}

void launch_job(size_t jobindex, char** argv) {
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

static void pass_stdin(stringptr *line) {
	static size_t next_child = 0;
	if(next_child >= sblist_getsize(prog_state.job_infos))
		next_child = 0;
	job_info *job = sblist_get(prog_state.job_infos, next_child);
	write_all(job->pipe, line->ptr, line->size);
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

__attribute__((noreturn))
static void die(const char* msg) {
	dprintf(2, msg);
	exit(1);
}

static unsigned long parse_human_number(stringptr* num) {
	unsigned long ret = 0;
	static const unsigned long mul[] = {1024, 1024 * 1024, 1024 * 1024 * 1024};
	const char* kmg = "KMG";
	char* kmgind;
	if(num && num->size) {
		ret = atol(num->ptr);
		if((kmgind = strchr(kmg, num->ptr[num->size -1])))
			ret *= mul[kmgind - kmg];
	}
	return ret;
}

static int syntax(void) {
	dprintf(2,
		"jobflow " VERSION " (C) rofl0r\n"
		"------------------\n"
		"this program is intended to be used as a recipient of another programs output\n"
		"it launches processes to which the current line can be passed as an argument\n"
		"using {} for substitution (as in find -exec).\n"
		"if no substitution argument ({} or {.}) is provided, input is piped into\n"
		"stdin of child processes. input will be then evenly distributed to jobs,\n"
		"until EOF is received.\n"
		"\n"
		"available options:\n\n"
		"-skip=XXX -threads=XXX -resume -statefile=/tmp/state -delayedflush\n"
		"-delayedspinup=XXX -buffered -joinoutput -limits=mem=16M,cpu=10\n"
		"-exec ./mycommand {}\n"
		"\n"
		"-skip=XXX\n"
		"    XXX=number of entries to skip\n"
		"-threads=XXX\n"
		"    XXX=number of parallel processes to spawn\n"
		"-resume\n"
		"    resume from last jobnumber stored in statefile\n"
		"-statefile=XXX\n"
		"    XXX=filename\n"
		"    saves last launched jobnumber into a file\n"
		"-delayedflush\n"
		"    only write to statefile whenever all processes are busy,\n"
		"    and at program end\n"
		"-delayedspinup=XXX\n"
		"    XXX=maximum amount of milliseconds\n"
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
		"-bulk=XXX\n"
		"    do bulk copies with a buffer of XXX bytes. only usable in pipe mode.\n"
		"    this passes (almost) the entire buffer to the next scheduled job.\n"
		"    the passed buffer will be truncated to the last line break boundary,\n"
		"    so jobs always get entire lines to work with.\n"
		"    this option is useful when you have huge input files and relatively short\n"
		"    task runtimes. by using it, syscall overhead can be reduced to a minimum.\n"
		"    XXX must be a multiple of 4KB. the suffixes G/M/K are detected.\n"
		"    actual memory allocation will be twice the amount passed.\n"
		"    note that pipe buffer size is limited to 64K on linux, so anything higher\n"
		"    than that probably doesn't make sense.\n"
		"    if no size is passed (i.e. only -bulk), a default of 4K will be used.\n"
		"-limits=[mem=XXX,cpu=XXX,stack=XXX,fsize=XXX,nofiles=XXX]\n"
		"    sets the rlimit of the new created processes.\n"
		"    see \"man setrlimit\" for an explanation. the suffixes G/M/K are detected.\n"
		"-exec command with args\n"
		"    everything past -exec is treated as the command to execute on each line of\n"
		"    stdin received. the line can be passed as an argument using {}.\n"
		"    {.} passes everything before the last dot in a line as an argument.\n"
		"    it is possible to use multiple substitutions inside a single argument,\n"
		"    but currently only of one type.\n"
		"    if -exec is omitted, input will merely be dumped to stdout (like cat).\n"
		"\n"
	);
	return 1;
}

#undef strtoll
#define strtoll(a,b,c) strtoint64(a, strlen(a))
static int parse_args(int argc, char** argv) {
	op_state op_b, *op = &op_b;
	op_init(op, argc, argv);
	char *op_temp;
	if(op_hasflag(op, SPL("-help")))
		return syntax();

	op_temp = op_get(op, SPL("threads"));
	long long x = op_temp ? strtoll(op_temp,0,10) : 1;
	if(x <= 0) die("threadcount must be >= 1\n");
	prog_state.numthreads = x;

	op_temp = op_get(op, SPL("statefile"));
	prog_state.statefile = op_temp;

	op_temp = op_get(op, SPL("skip"));
	prog_state.skip = op_temp ? strtoll(op_temp,0,10) : 0;
	if(op_hasflag(op, SPL("resume"))) {
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

	prog_state.delayedflush = 0;
	if(op_hasflag(op, SPL("delayedflush"))) {
		if(!prog_state.statefile) die("-delayedflush needs -statefile\n");
		prog_state.delayedflush = 1;
	}

	prog_state.pipe_mode = 0;

	op_temp = op_get(op, SPL("delayedspinup"));
	prog_state.delayedspinup_interval = op_temp ? strtoll(op_temp,0,10) : 0;

	prog_state.cmd_startarg = 0;
	prog_state.subst_entries = NULL;

	if(op_hasflag(op, SPL("exec"))) {
		uint32_t subst_ent;
		unsigned i, r = 0;
		for(i = 1; i < (unsigned) argc; i++) {
			if(str_equal(argv[i], "-exec")) {
				r = i + 1;
				break;
			}
		}
		if(r && r < (unsigned) argc) {
			prog_state.cmd_startarg = r;
		}

		prog_state.subst_entries = sblist_new(sizeof(uint32_t), 16);

		// save entries which must be substituted, to save some cycles.
		for(i = r; i < (unsigned) argc; i++) {
			subst_ent = i - r;
			if(strstr(argv[i], "{}") || strstr(argv[i], "{.}")) {
				sblist_add(prog_state.subst_entries, &subst_ent);
			}
		}
		if(sblist_getsize(prog_state.subst_entries) == 0) {
			prog_state.pipe_mode = 1;
			sblist_free(prog_state.subst_entries);
			prog_state.subst_entries = 0;
		}
	}

	prog_state.buffered = 0;
	if(op_hasflag(op, SPL("buffered"))) {
		prog_state.buffered = 1;
	}

	prog_state.join_output = 0;
	if(op_hasflag(op, SPL("joinoutput"))) {
		if(!prog_state.buffered) die("-joinoutput needs -buffered\n");
		prog_state.join_output = 1;
	}

	prog_state.bulk_bytes = 0;
	op_temp = op_get(op, SPL("bulk"));
	if(op_temp) {
		SPDECLAREC(value, op_temp);
		prog_state.bulk_bytes = parse_human_number(value);
		if(prog_state.bulk_bytes % 4096)
			die("bulk size must be a multiple of 4096\n");
	} else if(op_hasflag(op, SPL("bulk")))
		prog_state.bulk_bytes = 4096;

	prog_state.limits = NULL;
	op_temp = op_get(op, SPL("limits"));
	if(op_temp) {
		unsigned i;
		SPDECLAREC(limits, op_temp);
		stringptrlist* limit_list = stringptr_splitc(limits, ',');
		stringptrlist* kv;
		stringptr* key, *value;
		limit_rec lim;
		if(stringptrlist_getsize(limit_list)) {
			prog_state.limits = sblist_new(sizeof(limit_rec), stringptrlist_getsize(limit_list));
			for(i = 0; i < stringptrlist_getsize(limit_list); i++) {
				kv = stringptr_splitc(stringptrlist_get(limit_list, i), '=');
				if(stringptrlist_getsize(kv) != 2) continue;
				key = stringptrlist_get(kv, 0);
				value = stringptrlist_get(kv, 1);
				if(EQ(key, SPL("mem")))
					lim.limit = RLIMIT_AS;
				else if(EQ(key, SPL("cpu")))
					lim.limit = RLIMIT_CPU;
				else if(EQ(key, SPL("stack")))
					lim.limit = RLIMIT_STACK;
				else if(EQ(key, SPL("fsize")))
					lim.limit = RLIMIT_FSIZE;
				else if(EQ(key, SPL("nofiles")))
					lim.limit = RLIMIT_NOFILE;
				else
					die("unknown option passed to -limits");

				if(getrlimit(lim.limit, &lim.rl) == -1) {
					perror("getrlimit");
					die("could not query rlimits");
				}
				lim.rl.rlim_cur = parse_human_number(value);
				sblist_add(prog_state.limits, &lim);
				stringptrlist_free(kv);
			}
			stringptrlist_free(limit_list);
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

// returns numbers of substitutions done, -1 on out of buffer.
// dest is always overwritten. if not substitutions were done, it contains a copy of source.
int substitute_all(char* dest, ssize_t dest_size, stringptr* source, stringptr* what, stringptr* whit) {
	size_t i;
	int ret = 0;
	for(i = 0; dest_size > 0 && i < source->size; ) {
		if(stringptr_here(source, i, what)) {
			if(dest_size < (ssize_t) whit->size) return -1;
			memcpy(dest, whit->ptr, whit->size);
			dest += whit->size;
			dest_size -= whit->size;
			ret++;
			i += what->size;
		} else {
			*dest = source->ptr[i];
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
	if(*p == ch) return (char*)p;
	return 0;
}
static char* mystrnrchr(const char *in, int ch, size_t end) {
	const char *e = in+end-1;
	const char *p = in;
	while(p != e && *e != ch) e--;
	if(*e == ch) return (char*)e;
	return 0;
}

static int need_linecounter(void) {
	return !!prog_state.skip || prog_state.statefile;
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
static int dispatch_line(char* inbuf, size_t len, char** argv) {
	char subst_buf[16][4096];
	static unsigned spinup_counter = 0;

	stringptr line_b, *line = &line_b;

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
	}
	if(!prog_state.cmd_startarg) {
		write_all(1, inbuf, len);
		return 1;
	}

	line->ptr = inbuf; line->size = len;

	if(!prog_state.pipe_mode)
		stringptr_chomp(line);

	if(prog_state.subst_entries) {
		unsigned max_subst = 0;
		uint32_t* index;
		sblist_iter(prog_state.subst_entries, index) {
			SPDECLAREC(source, argv[*index + prog_state.cmd_startarg]);
			int ret;
			ret = substitute_all(subst_buf[max_subst], 4096, source, SPL("{}"), line);
			if(ret == -1) {
				too_long:
				dprintf(2, "fatal: line too long for substitution: %s\n", line->ptr);
				return 0;
			} else if(!ret) {
				char* lastdot = stringptr_rchr(line, '.');
				stringptr tilLastDot = *line;
				if(lastdot) tilLastDot.size = lastdot - line->ptr;
				ret = substitute_all(subst_buf[max_subst], 4096, source, SPL("{.}"), &tilLastDot);
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
		pass_stdin(line);

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

	size_t left = 0;
	const size_t chunksize = prog_state.bulk_bytes ? prog_state.bulk_bytes : 16*1024;

	char *mem = mmap(NULL, chunksize*2, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
	char *buf1 = mem;
	char *buf2 = mem+chunksize;
	char *in, *inbuf;

	int exitcode = 1;

	while(1) {
		inbuf = buf1+chunksize-left;
		memcpy(inbuf, buf2+chunksize-left, left);
		ssize_t n = read(0, buf2, chunksize);
		if(n == -1) {
			perror("read");
			goto out;
		}
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
			if(!dispatch_line(in, diff, argv))
				goto out;
			left -= diff;
			in += diff;
		}
		if(!n) {
			if(left) dispatch_line(in, left, argv);
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
