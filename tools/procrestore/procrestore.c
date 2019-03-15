#include "slsmm.h"

#include <sys/ioctl.h>

#include <fcntl.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static struct option longopts[] = {
	{ "format", required_argument, NULL, 'f' },
	{ NULL, no_argument, NULL, 0 },
};
void
usage(void)
{
    printf("Usage: procrestore [<-f <filename> | --format> <file | memory | osd>] \n");
}

int main(int argc, char* argv[]) {
	int slsmm_fd;
	int error;
	int mode;
	int type;
	int opt;
	char *filename;
	struct restore_param param;

	printf("Warning: Only files can be used for restoring right now\n");
	param = (struct restore_param) { 
		.name = NULL,
		.len = 0,
		.pid = getpid(),
		.fd_type = SLSMM_FD_FILE,
	};

	while ((opt = getopt_long(argc, argv, "f:", longopts, NULL)) != -1) {
	    switch (opt) {

	    case 'f':
		if (strcmp(optarg, "file") == 0)
		    param.fd_type = SLSMM_FD_FILE; 
		else if (strcmp(optarg, "memory") == 0)
		    param.fd_type = SLSMM_FD_MEM; 
		else if (strcmp(optarg, "osd") == 0)
		    param.fd_type = SLSMM_FD_NVDIMM; 
		else 
		    printf("Invalid output type, defaulting to file\n");
		break;

	    default:
		usage();
		return 0;
	    }
	}

	if (optind == argc - 1) {
	    if (param.fd_type != SLSMM_FD_FILE) {
		usage();
		return 0;
	    }

	    filename = argv[optind];
	    param.name = filename;
	    param.len = strnlen(filename, 1024);
	} else if (param.fd_type == SLSMM_FD_FILE) {
	    usage();
	    return 0;
	}


	slsmm_fd = open("/dev/slsmm", O_RDWR);
	if (!slsmm_fd) {
		printf("ERROR: SLS device file not opened\n");
		exit(1); 
	}

	ioctl(slsmm_fd, SLSMM_RESTORE, &param);

	close(slsmm_fd);

	return 0;

}
