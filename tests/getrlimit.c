#include <sys/resource.h>
#include <stdio.h>
int main() {
	struct rlimit rl;
	getrlimit(RLIMIT_AS, &rl);
	printf("%ld, %ld", rl.rlim_cur, rl.rlim_max);
	return 0;
}