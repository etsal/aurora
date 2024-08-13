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
	char basestr[PATH_MAX];
	char dirstr[PATH_MAX];
	char *dir, *base;
	int dirfd;
	int error;

	/* Set up the creation ioctl arguments. */
	memset(basestr, '\0', PATH_MAX);
	strncpy(basestr, path, strlen(path));
	base = basename(basestr);

	strncpy(args.path, base, strnlen(base, PATH_MAX));
	args.size = strnlen(base, PATH_MAX);

	/* Open the memsnap root and create the object. */
	memset(dirstr, '\0', PATH_MAX);
	strncpy(dirstr, path, strlen(path));
	dir = dirname(dirstr);
	strncat(dir, "/", PATH_MAX - strnlen(dir, PATH_MAX));
	strncat(dir, MSNP_CTRLDEV, PATH_MAX - strnlen(dir, PATH_MAX));

	dirfd = open(dir, O_RDWR);
	if (dirfd < 0) {
		perror("open");
		return (-1);
	}

	error = ioctl(dirfd, SLSFS_SAS_CREATE, &args);
	close(dirfd);
	if (error != 0) {
		perror("ioctl");
		return (error);
	}

	return (0);
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
