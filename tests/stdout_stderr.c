#include <stdio.h>

int main() {
	fprintf(stdout, "stdout\n");
	fflush(stdout);
	fprintf(stderr, "stderr\n");
	fflush(stderr);
	return 0;
}
