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
#include <stdlib.h>

#include <assert.h>
#include <objsnap.h>
#include <objsnap_ioctl.h>
#include <rdtsc.h>

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

int 
setup()
{
 	int error = newfs("/dev/nvd0");
	if (error) {
		printf("Problem creating new objsnap device");
		return error;
	}

	error = objsnap_init("/dev/nvd0");
	if (error) {
		printf("Error with objsnap init\n");
		return error;
	}

	return (0);
}


struct mapping {
	index_t inode;
	char *map;
};

int
setup_map(struct mapping *map, size_t size_in_blocks)
{
	map->inode = objsnap_create();
	map->map = mmap(NULL, BLOCKSIZE * size_in_blocks, PROT_READ | PROT_WRITE, 
        MAP_ANON, -1, 0);
    if (map->map == MAP_FAILED) {
        printf("MMAP FAILED\n");
        return -1;
    }

	return (0);
}

int dirty_map(struct mapping *map, off_t offset) 
{
	return objsnap_dirty(map->inode, map->map + (offset * BLOCKSIZE));
}

int checkpoint_maps(struct mapping *maps, size_t cnt)
{
	index_t indexes[1024];
	for (int i = 0; i < cnt; i++) {
		indexes[i] = maps[i].inode;
	}
    return objsnap_checkpoint(indexes, cnt);
}

void
basicTest()
{
	struct mapping map;
	int error = 0;
	if ((error = setup())) {
        printf("Problem in Setup!");
		return;
    }

	if ((error = setup_map(&map, 1024))) {
		printf("Error in setting up mapping\n");
	}

	if ((error = dirty_map(&map, 0))) {
		printf("Problem dirtying mapping\n");
	}

	if ((error = checkpoint_maps(&map, 1))) {
		printf("Problem Checkpointing mappings\n");
	}
}

void
random_write_load(int num_objs, int size_of_obj_in_blocks, 
	int writes_per_iteration, int times)
{
	int error = 0;
	if ((error = setup())) {
        printf("Problem in Setup!");
		return;
    }

	srand(time(NULL));

	struct mapping *maps = (struct mapping *)malloc(sizeof(struct mapping) * num_objs);
	for (int i = 0; i < num_objs; i++) {
		setup_map(&maps[i], size_of_obj_in_blocks);
	}


	// Do writes
	for (int times_i = 0; times_i < times; times_i++) {
		for (int obj_i = 0; obj_i < num_objs; obj_i++ ) {
			for (int writes_i = 0; writes_i < writes_per_iteration; writes_i++) {
				int rand_offset = rand() % size_of_obj_in_blocks;
				dirty_map(&maps[obj_i], rand_offset);
			}
		}
		checkpoint_maps(maps, num_objs);
	}


	for (int i = 0; i < num_objs; i++) {
		munmap(maps[i].map, BLOCKSIZE * size_of_obj_in_blocks);
	}

	free(maps);
}

void 
printstats(uint64_t clock) {
	statblock stats;
	int cnt = 0;
	printf("Stat Blocks\n");
	objsnap_systemstats(&stats, &cnt);
	for (int i = 0; i < cnt; i++) {
		struct timerstat *t = &stats[i];
		uint64_t sum = cycles_to_us(t->sum, clock);
		uint64_t avg = cycles_to_us(t->avg, clock);
		printf("[%s] Sum: %lu us, Count: %lu, Avg: %lu us\n",
			t->name, sum, t->cnt, avg);
	}
}


int main()
{
	//basicTest();
	uint64_t clock = get_clock_speed_sleep();
	int numCheckpoints = 5000;
	uint64_t before = rdtscp();	
	random_write_load(1, 1024 * 1024 * 1024, 16, numCheckpoints);	
	uint64_t after = rdtscp();
	uint64_t change = after - before;
	change = cycles_to_ms(change, clock);
	printf("Checkpoints[%d]: %lu\n", numCheckpoints, change);

	printstats(clock);
}