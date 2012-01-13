#undef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#undef _XOPEN_SOURCE
#define _XOPEN_SOURCE 700

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

typedef struct {
	pid_t pid;
	posix_spawn_file_actions_t fa;
} job_info;

/* defines how many slots our free_slots struct can take */
#define MAX_SLOTS 128

typedef struct {
	unsigned numthreads;
	unsigned threads_running;
	char* statefile;
	unsigned skip;
	sblist* job_infos;
	sblist* subst_entries;
	unsigned cmd_startarg;
	size_t free_slots[MAX_SLOTS];
	unsigned free_slots_count;
	char* tempdir;
	int delayedspinup_interval; /* use a random delay until the queue gets filled for the first time.
				the top value in ms can be supplied via a command line switch */
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
	return snprintf(buf, bufsize, is_stderr ? "%s/jd_proc_%.5u_stdout.log" : "%s/jd_proc_%.5u_stderr.log", prog_state.tempdir, (unsigned) jobindex) < bufsize;
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
	}
}

static void addJobSlot(size_t job_id) {
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
	while(dst && (nread = fread(buf, 1, sizeof(buf), dst))) {
		fwrite(buf, 1, nread, out_stream);
		if(nread < sizeof(buf)) break;
	}
	if(dst) 
		fclose(dst);
	
	fflush(out_stream);
}

/* reap childs and return pointer to a free "slot" or NULL */
void reapChilds(void) {
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
				}
				else {
					//log_put(js->log_fd, VARISL(" got error "), VARII(WEXITSTATUS(retval)), VARISL(" from  "), VARIS(job->prog), NULL);
				}
				job->pid = -1;
				posix_spawn_file_actions_destroy(&job->fa);
				//job->passed = 0;
				addJobSlot(i);
				prog_state.threads_running--;
				
				if(prog_state.buffered) {
					dump_output(i, 0);
					if(!prog_state.join_output)
						dump_output(i, 1);
				}
				
			}
		} else 
			addJobSlot(i);
	}
}


__attribute__((noreturn))
void die(const char* msg) {
	fprintf(stderr, msg);
	exit(1);
}

void parse_args(int argc, char** argv) {
	op_state op_b, *op = &op_b;
	op_init(op, argc, argv);
	char *op_temp;
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
			if(strstr(argv[i], "{}")) sblist_add(prog_state.subst_entries, &subst_ent);
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
	
}

void init_queue(void) {
	unsigned i;
	job_info ji;
	
	ji.pid = -1;
	memset(&ji.fa, 0, sizeof(ji.fa));
	
	for(i = 0; i < prog_state.numthreads; i++) {
		sblist_add(prog_state.job_infos, &ji);
	}
}

void write_statefile(uint64_t n, const char* tempfile) {
	char numbuf[64];
	stringptr num_b, *num = &num_b;

	num_b.ptr = uint64ToString(n + 1, numbuf);
	num_b.size = strlen(numbuf);
	stringptr_tofile((char*) tempfile, num);
	if(rename(tempfile, prog_state.statefile) == -1)
		perror("rename");
}

int main(int argc, char** argv) {
	char inbuf[4096]; char* fgets_result, *strstr_result, *p;
	stringptr line_b, *line = &line_b;
	char* cmd_argv[4096];
	char subst_buf[4096][16];
	unsigned max_subst;
	
	struct timeval reapTime;
	
	uint64_t n = 0;
	unsigned i, j;
	
	char tempdir_buf[256];
	char temp_state[256];
	
	srand(time(NULL));
	
	if(argc > 4096) argc = 4096;
	prog_state.threads_running = 0;
	prog_state.free_slots_count = 0;
	gettimestamp(&reapTime);
	
	parse_args(argc, argv);
	
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
						p = argv[*index + prog_state.cmd_startarg];
						if((strstr_result = strstr(p, "{}"))) {
							j = 0;
							j = strstr_result - p;
							if(j) memcpy(subst_buf[max_subst], p, j);
							strncpy(&subst_buf[max_subst][j], line->ptr, 4096 - j);
							j += line->size;
							if(j > 4096) {
								fprintf(stderr, "fatal: line too long for substitution: %s\n", line->ptr);
								goto out;
							}
							strncpy(&subst_buf[max_subst][j], strstr_result + 2, 4096 - j);
							
							cmd_argv[*index] = subst_buf[max_subst];
							max_subst++;
							if(max_subst >= 16) die("too many substitutions!\n");
						}
					}
				}
				
				while(prog_state.free_slots_count == 0 || mspassed(&reapTime) > REAP_INTERVAL_MS) {
					reapChilds();
					gettimestamp(&reapTime);
					if(!prog_state.free_slots_count) msleep(SLEEP_MS);
				}
				
				if(prog_state.delayedspinup_interval && n < prog_state.numthreads)
					msleep(rand() % (prog_state.delayedspinup_interval + 1));
				
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
	
	if(prog_state.tempdir) 
		rmdir(prog_state.tempdir);
	
	return 0;
}
