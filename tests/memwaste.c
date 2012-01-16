#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int syntax() {
	puts("prog size_of_mem_to_alloc");
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
	long mem;
	if (argc != 2) return syntax();
	mem = parse_human_number(argv[1]);
	if((buf = malloc(mem))) {
		fprintf(stdout, "malloc %lu bytes succeeded.\n", mem);
		return 0;
	} else {
		fprintf(stdout, "malloc %lu bytes failed.\n", mem);
		return 1;
	}
}
