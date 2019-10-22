
#include <sys/types.h>
#include <sys/ioctl.h>

#include <fcntl.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "sls_private.h"

/* Generic ioctl for the SLS device. */
int
sls_ioctl(long ionum, void *args)
{
	int status;
	int fd;

	fd = open("/dev/sls", O_RDWR);
	if (fd < 0) {
		fprintf(stderr, "ERROR: Could not open SLS device: %m");
		return -1;
	}

	status = ioctl(fd, ionum, args);
	if (status < 0) {
		fprintf(stderr, "ERROR: SLS proc ioctl failed: %m");
	}

	close(fd);

	return status;
}

/* XXX Will be removed when SLS_PROC gets refactored. */
int
sls_proc(struct proc_param *param)
{
	return sls_ioctl(SLS_PROC, param);
}
