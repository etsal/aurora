#include <assert.h>
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

static char *_msnp_dev = "/dev/msnp";
static int _msnp_fd = -1;
static pthread_mutex_t _msnp_mtx = PTHREAD_MUTEX_INITIALIZER;

static void
slsfs_init_devfd(void)
{
	if (_msnp_fd >= 0)
		return;

	pthread_mutex_lock(&_msnp_mtx);
	if (_msnp_fd >= 0) {
		pthread_mutex_unlock(&_msnp_mtx);
		return;
	}

	_msnp_fd = open(_msnp_dev, O_RDWR);
	assert(_msnp_fd >= 0);

	pthread_mutex_unlock(&_msnp_mtx);
	return;

}

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

	memset(args.path, 0, PATH_MAX);
	strncpy(args.path, base, strnlen(base, PATH_MAX));
	args.size = size;

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

/*
 * NOTE: The file descriptors in the signatures below are not actually used,
 * because the underlying pseudofs ioctl() does not scale for multithreaded
 * systems. We keep the signatures as below with compatibility with the 
 * original slsfs-backed MemSnap interface (same reason we use a seemingly
 * inconsistent naming scheme).
 */

int
sas_trace_start(int __unused fd)
{
	int error;

	slsfs_init_devfd();

	error = ioctl(_msnp_fd, SLSFS_SAS_TRACE_START);
	if (error != 0) {
		perror("sas_trace_start");
		return (1);
	}

	return (0);
}

int
sas_trace_end(int __unused fd)
{
	int error;

	slsfs_init_devfd();

	error = ioctl(_msnp_fd, SLSFS_SAS_TRACE_END);
	if (error != 0) {
		perror("sas_trace_end");
		return (1);
	}

	return (0);
}

int
sas_trace_commit(int __unused fd)
{
	int error;

	slsfs_init_devfd();

	error = ioctl(_msnp_fd, SLSFS_SAS_TRACE_COMMIT);
	if (error != 0) {
		perror("sas_trace_commit");
		return (1);
	}

	return (0);
}

int
sas_trace_abort(int __unused fd)
{
	int error;

	slsfs_init_devfd();

	error = ioctl(_msnp_fd, SLSFS_SAS_TRACE_ABORT);
	if (error != 0) {
		perror("sas_trace_abort");
		return (1);
	}

	return (0);
}

int
sas_refresh_protection(int __unused fd)
{
	int error;

	slsfs_init_devfd();

	error = ioctl(_msnp_fd, SLSFS_SAS_REFRESH_PROTECTION);
	if (error != 0) {
		perror("sas_trace_abort");
		return (1);
	}

	return (0);
}
