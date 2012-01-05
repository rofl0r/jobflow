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
	int buffered;
	sblist* job_infos;
	sblist* subst_entries;
	unsigned cmd_startarg;
	job_info* free_slots[MAX_SLOTS];
	unsigned free_slots_count;
	char* tempdir;
} prog_state_s;

prog_state_s prog_state;


extern char** environ;

int makeLogfilename(char* buf, ...) {
	return 0;
}

void launch_job(job_info* job, char** argv) {
	char buf[256];

	if(job->pid != -1) return;
	
	if(prog_state.buffered)
		if(!makeLogfilename(buf, sizeof(buf), argv[0])) {
			fprintf(stderr, " filename too long: %s\n", argv[0]);
			return;
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
		errno = posix_spawn_file_actions_addopen(&job->fa, 1, buf, O_WRONLY | O_CREAT | O_APPEND, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
		if(errno) goto spawn_error;
		errno = posix_spawn_file_actions_adddup2(&job->fa, 1, 2);
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

static void addJobSlot(job_info* job) {
	if(prog_state.free_slots_count < MAX_SLOTS) {
		prog_state.free_slots[prog_state.free_slots_count] = job;
		prog_state.free_slots_count++;
	}
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
				addJobSlot(job);
				prog_state.threads_running--;
			}
		} else 
			addJobSlot(job);
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

int main(int argc, char** argv) {
	char inbuf[4096]; char* fgets_result, *strstr_result, *p;
	stringptr line_b, *line = &line_b;
	char* cmd_argv[4096];
	char subst_buf[4096][16];
	unsigned max_subst;
	
	char numbuf[64];
	stringptr num_b, *num = &num_b;
	
	struct timeval reapTime;
	
	uint64_t n = 0;
	unsigned i, j;
	
	char tempdir_buf[256];
	char temp_state[256];
	
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
				
				launch_job(prog_state.free_slots[prog_state.free_slots_count-1], cmd_argv);
				prog_state.free_slots_count--;
				
				if(prog_state.statefile) {
					num_b.ptr = uint64ToString(n + 1, numbuf);
					num_b.size = strlen(numbuf);
					stringptr_tofile(temp_state, num);
					if(rename(temp_state, prog_state.statefile) == -1)
						perror("rename");
				}
			}
		}
		n++;
	}
	
	out:
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
