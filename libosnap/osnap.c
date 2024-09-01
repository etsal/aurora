#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/disk.h>

#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <stdio.h>
#include <fcntl.h>
#include <string.h>
#include <strings.h>
#include <stdbool.h>
#include <stdint.h>
#include <dirent.h>
#include <stdlib.h>

#include "objsnap.h"
#include "objsnap_ioctl.h"

#ifdef __cplusplus
extern "C" {
#endif

static char * OS_DEV = "/dev/objsnap";
static int OS_FD = -1;

static int 
objsnap_fd_check()
{
     if (OS_FD == -1) {
        int fd = open(OS_DEV, O_RDWR);
        if (fd < 0) {
            return errno;
        }

        OS_FD = fd;
    }

    return 0;
}

int 
objsnap_newfs(const char *path)
{
	int status;
	struct stat st;
	int fd;
	uint32_t bsize = 0;
	uint32_t ssize = 0;
	uint64_t size = 0;


	fd = open(path, O_RDWR);
	if (fd < 0) {
		perror("open");
		exit(1);
	}

	status = fstat(fd, &st);
	if (status < 0) {
		perror("lstat");
		exit(1);
	}

	if (!bsize || bsize < st.st_blksize) {
		bsize = st.st_blksize;
	}

	if (S_ISCHR(st.st_mode)) {
		int sectorsize;
		off_t disksize;

		if (ioctl(fd, DIOCGSECTORSIZE, &sectorsize) < 0) {
			perror("ioctl(DIOCGSECTORSIZE)");
			exit(1);
		}

		if (ioctl(fd, DIOCGMEDIASIZE, &disksize) < 0) {
			perror("ioctl(DIOGCGMEDIASIZE)");
			exit(1);
		}

		ssize = sectorsize;
		size = disksize;

		if (bsize == 0) {
			bsize = ssize;
		}
	} else if (S_ISREG(st.st_mode)) {
		if (!size || size < st.st_size) {
			size = st.st_size;
		}

		/*
		 * Set the sector size to 4 KiB and block size to the maximum
		 * block size.
		 */
		ssize = 4 * 1024;
		bsize = 4 * 1024;
	} else {
		fprintf(
		    stderr, "You can only create an OSD on a device or file\n");
		exit(1);
	}

	printf(
	    "%s: %lu GiB (%lu sectors), block size %u kiB, sector size %u B\n",
	    path, size / (1024 * 1024 * 1024), size / ssize, bsize / 1024,
	    ssize);

	// We have to allocate the appropriate super blocks


	printf("creating super blocks\n");
    	super_t *sb = (super_t *)malloc(BLOCKSIZE);
	memset(sb, 0, ssize);
	sb->super_ssize = ssize;
	sb->super_bsize = 512;
	sb->super_size = size / BLOCKSIZE;
	sb->super_asize = bsize;
	sb->super_max_inodes = MAXINODES;
	sb->super_next = 1;
	sb->super_version = 0;
	sb->super_blk = 0;

	ssize_t written = write(fd, sb, BLOCKSIZE);
	if (written == (-1)) {
		perror("writing superblock failed");
		free(sb);
		return (1);
	}

	// Increment to cover the sister super block.
	sb->super_blk = 1;

	written = write(fd, sb, BLOCKSIZE);
	if (written == (-1)) {
		perror("writing second superblock failed");
		free(sb);
		return (1);
	}

	free(sb);
	close(fd);

	return (0);
}

int 
objsnap_init(const char *path)
{
    int error = 0;
    struct objsnap_init_args args;

    if ((error = objsnap_fd_check()) != 0) {
        return error;
    }

    strcpy(args.path, path);
    return ioctl(OS_FD, OBJSNAP_INIT, &args);
}

index_t
objsnap_create()
{
    int error = 0;
    struct objsnap_create_args args;

    if ((error = objsnap_fd_check()) != 0) {
        return error;
    }

    error = ioctl(OS_FD, OBJSNAP_CREATEOBJ, &args);
    if (error) {
        return BADINDEX;
    }

    return (args.os_index);
}

int 
objsnap_dirty(index_t fd, int tid, void *ptr)
{
    int error = 0;
    struct objsnap_dirty_page_args args;

    if ((error = objsnap_fd_check()) != 0) {
        return error;
    }

    args.os_index = fd;
    args.os_tid = tid;
    args.os_page = (uintptr_t)ptr;
    return ioctl(OS_FD, OBJSNAP_DIRTYPAGE, &args);
}

int objsnap_stat(index_t fd, osinode_t *inode)
{
    int error = 0;
    struct objsnap_stat_args args;

    if ((error = objsnap_fd_check()) != 0) {
        return error;
    }

    args.os_index = fd;

    error = ioctl(OS_FD, OBJSNAP_STAT, &args);
    *inode = args.os_inode;
    
    return (error);
}

int objsnap_systemstats(statblock *stats, int *cnt)
{
    int error = 0;
    struct objsnap_systemstats_args args;

    if ((error = objsnap_fd_check()) != 0) {
        return error;
    }

    error = ioctl(OS_FD, OBJSNAP_SYSTEMSTATS, &args);
    memcpy(stats, args.os_stats, sizeof(statblock));
    *cnt = args.os_cnt;

    return (error);
}

int 
objsnap_checkpoint(int tid)
{
    struct objsnap_checkpoint_args args;
    int error = 0;
    if ((error = objsnap_fd_check()) != 0) {
        return error;
    }

    args.tid = tid;
    return ioctl(OS_FD, OBJSNAP_CHECKPOINT, &args);
}


#ifdef __cplusplus
}
#endif
