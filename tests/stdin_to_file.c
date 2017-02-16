#include "../../lib/include/stringptr.h"
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>

int syntax() {
	puts("prog name_of_file");
	puts("writes stdin to supplied filename, in the fashion of the > shell operator.");
	return 1;
}

int main(int argc, char** argv) {
	stringptr line;
	char buf[128 * 1024];
	
	if(argc != 2) return syntax();
	
	int fd = open(argv[1], O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
	if(fd == -1) {
		perror("file access");
		return 1;
	}
	
	while(line = read_stdin_line(buf, sizeof(buf), 0), line.ptr) {
		write(fd, line.ptr, line.size);
	}
	close(fd);
	return 0;
}
