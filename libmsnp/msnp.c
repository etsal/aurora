#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <libgen.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <sys/ioctl.h>

#include <memsnap_ioctl.h>

int
slsfs_sas_create(char *path, size_t size)
{
	struct slsfs_sas_create_args args;
	char dupstr[PATH_MAX];
	int dirfd;
	int error;
	char *dir;

	/* Set up the creation ioctl arguments. */
	memset(args.path, '\0', PATH_MAX);
	strncpy(args.path, path, strlen(path));
	args.size = size;

	/* Open the memsnap root and create the object. */
	memset(dupstr, '\0', PATH_MAX);
	strncpy(dupstr, path, strlen(path));

	dir = dirname(dupstr);
	if (dir == NULL)
		return (EINVAL);

	dirfd = open(dir, O_RDONLY);
	error = ioctl(dirfd, SLSFS_SAS_CREATE, &args);
	close(dirfd);

	if (error != 0)
		return (error);


	return (open(path, O_RDWR));
}

int
slsfs_sas_map(int fd, void **addrp)
{
	struct slsfs_sas_create_args args;
	void *addr;
	int error;

	error = ioctl(fd, SLSFS_SAS_MAP, &addr);
	if (error != 0) {
		perror("sas_map");
		return (1);
	}

	*addrp = addr;

	return (0);
}

int
sas_trace_start(int fd)
{
	int error;

	error = ioctl(fd, SLSFS_SAS_TRACE_START);
	if (error != 0) {
		perror("sas_trace_start");
		return (1);
	}

	return (0);
}

int
sas_trace_end(int fd)
{
	int error;

	error = ioctl(fd, SLSFS_SAS_TRACE_END);
	if (error != 0) {
		perror("sas_trace_end");
		return (1);
	}

	return (0);
}

int
sas_trace_commit(int fd)
{
	int error;

	error = ioctl(fd, SLSFS_SAS_TRACE_COMMIT);
	if (error != 0) {
		perror("sas_trace_commit");
		return (1);
	}

	return (0);
}

int
sas_trace_abort(int fd)
{
	int error;

	error = ioctl(fd, SLSFS_SAS_TRACE_ABORT);
	if (error != 0) {
		perror("sas_trace_abort");
		return (1);
	}

	return (0);
}

int
sas_refresh_protection(int fd)
{
	int error;

	error = ioctl(fd, SLSFS_SAS_REFRESH_PROTECTION);
	if (error != 0) {
		perror("sas_trace_abort");
		return (1);
	}

	return (0);
}
