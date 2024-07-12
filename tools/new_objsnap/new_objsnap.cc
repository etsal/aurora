#include <iostream>
#include <thread>
#include <vector>

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
#include <sysexits.h>
#include <time.h>
#include <unistd.h>
#include <uuid.h>

#include <assert.h>
#include <objsnap.h>
#include <objsnap_ioctl.h>
#include <rdtsc.h>

const char *disk;
uint64_t clock_cycles;

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
setup(const char *disk)
{
 	int error = newfs(disk);
	if (error) {
		printf("Problem creating new objsnap device");
		return error;
	}

	error = objsnap_init(disk);
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
	map->map = (char *)mmap(NULL, BLOCKSIZE * size_in_blocks, PROT_READ | PROT_WRITE, 
        MAP_ANON, -1, 0);
    if (map->map == MAP_FAILED) {
        printf("MMAP FAILED\n");
        return -1;
    }

	return (0);
}

int dirty_map(struct mapping *map, int tid, off_t offset) 
{
	return objsnap_dirty(map->inode, tid, map->map + (offset * BLOCKSIZE));
}

void
basicTest()
{
	struct mapping map;
	int error = 0;
	if ((error = setup(disk))) {
        printf("Problem in Setup!");
		return;
    }

	if ((error = setup_map(&map, 1024))) {
		printf("Error in setting up mapping\n");
	}

	if ((error = dirty_map(&map, 0, 0))) {
		printf("Problem dirtying mapping\n");
	}

	if ((error = objsnap_checkpoint(0))) {
		printf("Problem Checkpointing mappings\n");
	}
}

uint64_t
random_write_task(struct mapping *maps, 
	int num_objs, int size_of_obj_in_blocks, 
	int writes_per_iteration, int times, int tid) {
	uint64_t cnt = 0;
	uint64_t sum = 0;
	for (int times_i = 0; times_i < times; times_i++) {
		for (int obj_i = 0; obj_i < num_objs; obj_i++ ) {
			for (int writes_i = 0; writes_i < writes_per_iteration; writes_i++) {
				int rand_offset = rand() % size_of_obj_in_blocks;
				dirty_map(&maps[obj_i], tid, rand_offset);
			}
		}
		uint64_t before = rdtscp();	
		objsnap_checkpoint(tid);
		uint64_t after = rdtscp();
		sum += (after - before);
		cnt += 1;
	}
	return sum / cnt;
}

void
random_write_load(int num_objs, int size_of_obj_in_blocks, 
	int writes_per_iteration, int times, int tid)
{
	int error = 0;
	if ((error = setup(disk))) {
        printf("Problem in Setup!");
		return;
    }

	srand(time(NULL));

	struct mapping *maps = (struct mapping *)malloc(sizeof(struct mapping) * num_objs);
	for (int i = 0; i < num_objs; i++) {
		setup_map(&maps[i], size_of_obj_in_blocks);
	}


	random_write_task(maps, num_objs, 
		size_of_obj_in_blocks,
	 	writes_per_iteration, 
		times, tid);

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

uint64_t
threadedTest(int numthreads, int num_objs, 
	int size_of_obj_in_blocks,
	int writes_per_iteration, int times) {

    // Vector to hold thread objects
    std::vector<std::thread> threads;
	uint64_t *avgs = (uint64_t *)malloc(sizeof(uint64_t)* numthreads);
	int error = 0;

	srand(time(NULL));

	struct mapping *maps = (struct mapping *)malloc(sizeof(struct mapping) * num_objs);
	for (int i = 0; i < num_objs; i++) {
		setup_map(&maps[i], size_of_obj_in_blocks);
	}


    for (int i = 0; i < numthreads; ++i) {
        // Using a lambda function for the thread's task
        threads.emplace_back([avgs, maps, num_objs, 
			size_of_obj_in_blocks, writes_per_iteration, times, i](){

			uint64_t avg =  random_write_task(maps, num_objs, 
				size_of_obj_in_blocks,
				writes_per_iteration, 
				times, i);
			avgs[i] = avg;

        });
    }

    // Join all threads to wait for them to finish
    for (auto& thread : threads) {
        thread.join();
    }

	uint64_t sum_avg = 0;
	for (int i = 0; i < numthreads; i++) {
		printf("[Thread %d] %f\n", i, cycles_to_us(avgs[i], clock_cycles));
		sum_avg += cycles_to_us(avgs[i], clock_cycles);
	}

	return sum_avg / numthreads;
}


int
main(int argc, char *argv[])
{
	if (argc != 2) {
		printf("Usage: new_objsnap <disk>");
		return (EX_USAGE);
	}

	disk = argv[1];

	clock_cycles = get_clock_speed_sleep();

	//basicTest();

	int error = setup(disk);
	if (error != 0) {
        	printf("Problem in Setup!");
		return (-1);
    	}

	int totaldirtyset = 16;
	int numCheckpoints = 5000;
	int numthreads = 4;
	int numobjs = 8;
	
	int numblocks_per_obj_per_ckpt = totaldirtyset / numobjs;
	int MiB = (1024 * 1024) / BLOCKSIZE;
	int GiB = (1024 * MiB);
	printf("Threads(%d), Blocksize (%lu), Total Dirty Set in Blocks (%d), Checkpoints per thread(%d), Number of objects(%d)\n",
		numthreads, BLOCKSIZE, totaldirtyset, numCheckpoints, numobjs);
	for (int i = 1; i < numthreads + 1; i++) {
		uint64_t before = rdtscp();	
		uint64_t avglat = threadedTest(i, numobjs, 1 * GiB, 
			numblocks_per_obj_per_ckpt, numCheckpoints);
		uint64_t after = rdtscp();
		double change = after - before;
		change = cycles_to_s(change, clock_cycles);
		printf("[%d] Ckpts/s(%f), latency(%lu), total(%d), seconds(%f)\n", 
			i, (numCheckpoints * i) / change, avglat, (numCheckpoints * i), change);
	}

	printstats(clock_cycles);

	return (EX_OK);
}
