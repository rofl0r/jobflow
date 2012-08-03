/*
Copyright (C) 2012  rofl0r

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

/* FIXME
when using more than 1 process, and not the -buffered option, the output of some processes gets lost.
this happens regardless of dynamic/static linking, libc, and whether the target program fflushes before exit.

piping the output into cat > file instead, everything arrives.
linux bug ?

test:
fail:
seq 100 | ./jobflow.out -threads=100 -exec echo {} > test.tmp ; wc -l test.tmp
should print 100, but does not always

success:
seq 100 | ./jobflow.out -threads=100 -exec echo {} | cat > test.tmp ; wc -l test.tmp
always prints 100
*/

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

/* defines the amount of milliseconds to sleep between each call to the reaper, 
 * once all free slots are exhausted */
#define SLEEP_MS 21

/* defines after how many milliseconds a reap of the running processes is obligatory. */
#define REAP_INTERVAL_MS 100

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
	fprintf(stderr, "prlimit() not implemented on this system\n");
	errno = EINVAL;
	return -1;
}
#endif


#include <sys/time.h>

typedef struct {
	pid_t pid;
	posix_spawn_file_actions_t fa;
} job_info;

typedef struct {
	int limit;
	struct rlimit rl;
} limit_rec;

/* defines how many slots our free_slots struct can take */
#define MAX_SLOTS 128

typedef struct {
	unsigned numthreads;
	unsigned threads_running;
	char* statefile;
	unsigned skip;
	sblist* job_infos;
	sblist* subst_entries;
	sblist* limits;
	unsigned cmd_startarg;
	size_t free_slots[MAX_SLOTS];
	unsigned free_slots_count;
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
			   jobs is small or smaller than the available threadcount / MAX_SLOTS. */
	int join_output:1; /* join stdout and stderr of launched jobs into stdout */
} prog_state_s;

prog_state_s prog_state;


extern char** environ;

int makeLogfilename(char* buf, size_t bufsize, size_t jobindex, int is_stderr) {
	int ret = snprintf(buf, bufsize, 
			   is_stderr ? "%s/jd_proc_%.5u_stdout.log" : "%s/jd_proc_%.5u_stderr.log",
			   prog_state.tempdir, (unsigned) jobindex);
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
			fprintf(stderr, "temp filename too long!\n");
			return;
		}
	}

	errno = posix_spawn_file_actions_init(&job->fa);
	if(errno) goto spawn_error;
	errno = posix_spawn_file_actions_addclose(&job->fa, 0);
	if(errno) goto spawn_error;
	
	if(prog_state.buffered) {
		errno = posix_spawn_file_actions_addclose(&job->fa, 1);
		if(errno) goto spawn_error;
		errno = posix_spawn_file_actions_addclose(&job->fa, 2);
		if(errno) goto spawn_error;
	}
	
	errno = posix_spawn_file_actions_addopen(&job->fa, 0, "/dev/null", O_RDONLY, 0);
	if(errno) goto spawn_error;
	
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
}

static void releaseJobSlot(size_t job_id) {
	if(prog_state.free_slots_count < MAX_SLOTS) {
		prog_state.free_slots[prog_state.free_slots_count] = job_id;
		prog_state.free_slots_count++;
	}
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

static void reapChilds(void) {
	size_t i;
	job_info* job;
	int ret, retval;
	
	prog_state.free_slots_count = 0;
	
	for(i = 0; i < sblist_getsize(prog_state.job_infos); i++) {
		job = sblist_get(prog_state.job_infos, i);
		if(job->pid != -1) {
			ret = waitpid(job->pid, &retval, WNOHANG);
			if(ret != 0) {
				// error or changed state.
				if(ret == -1) {
					perror("waitpid");
					continue;
				}
				if(!retval) {
					//log_put(js->log_fd, VARISL(" job finished: "), VARIS(job->prog), NULL);
				} else {
					//log_put(js->log_fd, VARISL(" got error "), VARII(WEXITSTATUS(retval)), VARISL(" from  "), VARIS(job->prog), NULL);
				}
				job->pid = -1;
				posix_spawn_file_actions_destroy(&job->fa);
				//job->passed = 0;
				releaseJobSlot(i);
				prog_state.threads_running--;
				
				if(prog_state.buffered) {
					dump_output(i, 0);
					if(!prog_state.join_output)
						dump_output(i, 1);
				}
			}
		} else 
			releaseJobSlot(i);
	}
}


__attribute__((noreturn))
static void die(const char* msg) {
	fprintf(stderr, msg);
	exit(1);
}

static long parse_human_number(stringptr* num) {
	long ret = 0;
	char buf[64];
	if(num && num->size && num->size < sizeof(buf)) {
		if(num->ptr[num->size -1] == 'G')
			ret = 1024 * 1024 * 1024;
		else if(num->ptr[num->size -1] == 'M')
			ret = 1024 * 1024;
		else if(num->ptr[num->size -1] == 'K')
			ret = 1024;
		if(ret) {
			memcpy(buf, num->ptr, num->size);
			buf[num->size] = 0;
			return atol(buf) * ret;
		}
		return atol(num->ptr);
	}
	return ret;
}

static int syntax(void) {
	puts(   
		"jobflow (C) rofl0r\n"
		"------------------\n"
		"this program is intended to be used as a recipient of another programs output\n"
		"it launches processes to which the current line can be passed as an argument\n"
		"using {} for substitution (as in find -exec).\n"
		"\n"
		"available options:\n\n"
		"-skip=XXX -threads=XXX -resume -statefile=/tmp/state -delayedflush\n"
		"-delayedspinup=XXX -buffered -joinoutput -limits=mem=16M,cpu=10\n"
		"-exec ./mycommand {}\n"
		"\n"
		"-skip=XXX\n"
		"    XXX=number of entries to skip\n"
		"-threads=XXX\n"
		"    XXX=number of parallel processes to spawn]\n"
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
		"-limits=[mem=XXX,cpu=XXX,stack=XXX,fsize=XXX,nofiles=XXX]\n"
		"    sets the rlimit of the new created processes.\n"
		"    see \"man setrlimit\" for an explanation. the suffixes G/M/K are detected.\n"
		"-exec command with args\n"
		"    everything past -exec is treated as the command to execute on each line of\n"
		"    stdin received. the line can be passed as an argument using {}.\n"
		"    {.} passes everything before the last dot in a line as an argument.\n"
		"    it is possible to use multiple substitutions inside a single argument,\n"
		"    but currently only of one type.\n"
	);
	return 1;
}

static int parse_args(int argc, char** argv) {
	op_state op_b, *op = &op_b;
	op_init(op, argc, argv);
	char *op_temp;
	if(argc == 1 || op_hasflag(op, SPL("-help")))
		return syntax();
	op_temp = op_get(op, SPL("threads"));
	prog_state.numthreads = op_temp ? atoi(op_temp) : 1;
	op_temp = op_get(op, SPL("statefile"));
	prog_state.statefile = op_temp;

	op_temp = op_get(op, SPL("skip"));
	prog_state.skip = op_temp ? atoi(op_temp) : 0;
	if(op_hasflag(op, SPL("resume"))) {
		if(!prog_state.statefile) die("-resume needs -statefile\n");
		if(access(prog_state.statefile, W_OK | R_OK) != -1) {
			stringptr* fc = stringptr_fromfile(prog_state.statefile);
			prog_state.skip = atoi(fc->ptr);
		}
	}
	
	prog_state.delayedflush = 0;
	if(op_hasflag(op, SPL("delayedflush"))) {
		if(!prog_state.statefile) die("-delayedflush needs -statefile\n");
		prog_state.delayedflush = 1;
	}
	
	op_temp = op_get(op, SPL("delayedspinup"));
	prog_state.delayedspinup_interval = op_temp ? atoi(op_temp) : 0;

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
		
		// save entries which must be substituted, to save some cycles.
		prog_state.subst_entries = sblist_new(sizeof(uint32_t), 16);
		for(i = r; i < (unsigned) argc; i++) {
			subst_ent = i - r;
			if(strstr(argv[i], "{}") || strstr(argv[i], "{.}")) sblist_add(prog_state.subst_entries, &subst_ent);
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
	job_info ji;
	
	ji.pid = -1;
	memset(&ji.fa, 0, sizeof(ji.fa));
	
	for(i = 0; i < prog_state.numthreads; i++) {
		sblist_add(prog_state.job_infos, &ji);
	}
}

static void write_statefile(uint64_t n, const char* tempfile) {
	char numbuf[64];
	stringptr num_b, *num = &num_b;

	num_b.ptr = uint64ToString(n + 1, numbuf);
	num_b.size = strlen(numbuf);
	stringptr_tofile((char*) tempfile, num);
	if(rename(tempfile, prog_state.statefile) == -1)
		perror("rename");
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

int main(int argc, char** argv) {
	char inbuf[4096]; char* fgets_result;
	stringptr line_b, *line = &line_b;
	char* cmd_argv[4096];
	char subst_buf[16][4096];
	unsigned max_subst;
	
	struct timeval reapTime;
	
	uint64_t n = 0;
	unsigned i;
	unsigned spinup_counter = 0;
	
	char tempdir_buf[256];
	char temp_state[256];
	
	srand(time(NULL));
	
	if(argc > 4096) argc = 4096;
	prog_state.threads_running = 0;
	prog_state.free_slots_count = 0;
	gettimestamp(&reapTime);
	
	if(parse_args(argc, argv)) return 1;
	
	if(prog_state.statefile)
		ulz_snprintf(temp_state, sizeof(temp_state), "%s.%u", prog_state.statefile, (unsigned) getpid());
	
	prog_state.tempdir = NULL;
	
	if(prog_state.buffered) {
		prog_state.tempdir = tempdir_buf;
		if(mktempdir("jobflow", tempdir_buf, sizeof(tempdir_buf)) == 0) {
			perror("mkdtemp");
			die("could not create tempdir\n");
		}
	}
	
	if(prog_state.cmd_startarg) {
		for(i = prog_state.cmd_startarg; i < (unsigned) argc; i++) {
			cmd_argv[i - prog_state.cmd_startarg] = argv[i];
		}
		cmd_argv[argc - prog_state.cmd_startarg] = NULL;
	}
	
	prog_state.job_infos = sblist_new(sizeof(job_info), prog_state.numthreads);
	init_queue();
	
	while((fgets_result = fgets(inbuf, sizeof(inbuf), stdin))) {
		if(prog_state.skip)
			prog_state.skip--;
		else {
			if(!prog_state.cmd_startarg)
				printf(fgets_result);
			else {
				stringptr_fromchar(fgets_result, line);
				stringptr_chomp(line);
				
				max_subst = 0;
				if(prog_state.subst_entries) {
					uint32_t* index;
					sblist_iter(prog_state.subst_entries, index) {
						SPDECLAREC(source, argv[*index + prog_state.cmd_startarg]);
						int ret;
						ret = substitute_all(subst_buf[max_subst], 4096, source, SPL("{}"), line);
						if(ret == -1) {
							too_long:
							fprintf(stderr, "fatal: line too long for substitution: %s\n", line->ptr);
							goto out;
						} else if(!ret) {
							char* lastdot = stringptr_rchr(line, '.');
							stringptr tilLastDot = *line;
							if(lastdot) tilLastDot.size = lastdot - line->ptr;
							ret = substitute_all(subst_buf[max_subst], 4096, source, SPL("{.}"), &tilLastDot);
							if(ret == -1) goto too_long;
						}
						if(ret) {
							cmd_argv[*index] = subst_buf[max_subst];
							max_subst++;
						}
					}
				}
				
				while(prog_state.free_slots_count == 0 || mspassed(&reapTime) > REAP_INTERVAL_MS) {
					reapChilds();
					gettimestamp(&reapTime);
					if(!prog_state.free_slots_count) msleep(SLEEP_MS);
				}
				
				if(prog_state.delayedspinup_interval && spinup_counter < (prog_state.numthreads * 2)) {
					msleep(rand() % (prog_state.delayedspinup_interval + 1));
					spinup_counter++;
				}
				
				launch_job(prog_state.free_slots[prog_state.free_slots_count-1], cmd_argv);
				prog_state.free_slots_count--;
				
				if(prog_state.statefile && (prog_state.delayedflush == 0 || prog_state.free_slots_count == 0)) {
					write_statefile(n, temp_state);
				}
			}
		}
		n++;
	}
	
	out:
	
	if(prog_state.delayedflush)
		write_statefile(n - 1, temp_state);
	
	while(prog_state.threads_running) {
		reapChilds();
		if(prog_state.threads_running) msleep(SLEEP_MS);
	}
	
	if(prog_state.subst_entries) sblist_free(prog_state.subst_entries);
	if(prog_state.job_infos) sblist_free(prog_state.job_infos);
	if(prog_state.limits) sblist_free(prog_state.limits);
	
	if(prog_state.tempdir) 
		rmdir(prog_state.tempdir);
	

	fflush(stdout);
	fflush(stderr);
	
	
	return 0;
}
