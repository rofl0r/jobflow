/* like cat, but processing one line at a time. stdin only. */
#include <unistd.h>
#include <stdio.h>
#include <string.h>

int main() {
	char buf[1024];
	while(fgets(buf, sizeof buf, stdin)) {
		ssize_t l = strlen(buf);
		ssize_t n = write(1, buf, l);
		if(n != l) {
			perror("write");
			return 1;
		}
	}
	return 0;
}
