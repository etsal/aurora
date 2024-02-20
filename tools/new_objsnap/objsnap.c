#include <sys/types.h>
#include <sys/disk.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/vnode.h>
#include <sys/mman.h>

#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>
#include <unistd.h>
#include <uuid.h>

#include <objsnap.h>
#include <objsnap_ioctl.h>

int newfs(const char *path)
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
		bsize = 64 * 1024;
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
	sb->super_bsize = bsize;
	sb->super_size = size;
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


int main()
{
    
    int error = newfs("/dev/nvd0");
    if (error) {
        printf("Problem creating new objsnap device");
        return -1;
    }

    error = objsnap_init("/dev/nvd0");
    if (error) {
        printf("Error with objsnap init\n");
        return -1;
    }

    char *addr = mmap(NULL, BLOCKSIZE * 100, PROT_READ | PROT_WRITE, 
        MAP_ANON, -1, 0);
    if (addr == MAP_FAILED) {
        printf("MMAP FAILED\n");
        return -1;
    }

    printf("Init good!\n");

    index_t inode1 = objsnap_create();

    printf("Object created! %lu\n", inode1);

  
    index_t inode2 = objsnap_create();

    printf("Object created! %lu\n", inode2);

    error = objsnap_dirty(inode1, addr);
    if (error) {
        printf("Could not dirty page");
    }

    error = objsnap_dirty(inode1, (char *)addr + BLOCKSIZE);
    if (error) {
        printf("Could not dirty page");
    }

    error = objsnap_dirty(inode1, (char *)addr + (2 * BLOCKSIZE));
    if (error) {
        printf("Could not dirty page");
    }

    index_t checkpointed[2];
    checkpointed[0] = inode1;
    checkpointed[1] = inode2;

    error = objsnap_checkpoint(checkpointed, 2);
    if (error) {
        printf("Problem Checkpointing");
    }
}