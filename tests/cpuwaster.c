#include "../lib/include/timelib.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

int syntax() {
	puts("prog seconds_of_cpu_time_to_burn");
	return 1;
}

long parse_human_number(char* num) {
	long ret = 0;
	char buf[64];
	if(!num) return 0;
	size_t l = strlen(num);
	
	if(l && l < sizeof(buf)) {
		if(num[l -1] == 'G')
			ret = 1024 * 1024 * 1024;
		else if(num[l -1] == 'M')
			ret = 1024 * 1024;
		else if(num[l -1] == 'K')
			ret = 1024;
		if(ret) {
			memcpy(buf, num, l);
			buf[l] = 0;
			return atol(buf) * ret;
		}
		return atol(num);
	}
	return ret;
}

int main(int argc, char** argv) {
	void* buf;
	long waste_secs;
	struct timeval start;
	if (argc != 2) return syntax();
	waste_secs = parse_human_number(argv[1]);
	gettimestamp(&start);
	
	while(mspassed(&start) < waste_secs * 1000) {}
	
	printf("successfully wasted %lu seconds of your cpu time\n", waste_secs);
	return 0;
}
