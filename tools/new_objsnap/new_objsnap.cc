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
#include <objsnap_rdtsc.h>

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
	sb->super_next = 3;
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
	size_t size = BLOCKSIZE * size_in_blocks;
	int i;

	map->inode = objsnap_create();
	map->map = (char *)mmap(NULL, size, PROT_READ | PROT_WRITE, 
        MAP_ANON, -1, 0);
	if (map->map == MAP_FAILED) {
		printf("MMAP FAILED\n");
		return -1;
	}

	memset(map->map, 0xa5, size);

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
	int error = setup(disk);
	if (error != 0) {
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


std::pair<std::vector<uint64_t>, uint64_t>
random_write_task(struct mapping *maps, 
	int num_objs, int size_of_obj_in_blocks, 
	int writes_per_iteration, int times, int tid) {
	std::vector<uint64_t> samples;
	auto start_time = std::chrono::high_resolution_clock::now();
	uint64_t txns = 0;
	while(true) {
		for (int w = 0; w < writes_per_iteration; w++) {
			int obj_i = rand() % num_objs;
			int rand_offset = rand() % size_of_obj_in_blocks;
			dirty_map(&maps[obj_i], tid, rand_offset);
		}
		uint64_t before = rdtscp();	
		objsnap_checkpoint(tid);
		uint64_t after = rdtscp();
		if (txns % 1000) {
			samples.push_back(cycles_to_ns(after - before, clock_cycles));
		}
		auto current_time = std::chrono::high_resolution_clock::now();
		if (std::chrono::duration<double>(current_time - start_time).count() >= times) {
			break;
		}
		txns += 1;
	}
	return std::make_pair(samples, txns);
}

void
random_write_load(int num_objs, int size_of_obj_in_blocks, 
	int writes_per_iteration, int times, int tid)
{
	int error = setup(disk);
	if (error != 0) {
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

std::vector<std::pair<std::vector<uint64_t>, uint64_t>>
threadedTest(int numthreads, int num_objs, 
	int size_of_obj_in_blocks,
	int writes_per_iteration, int times) {

	// Vector to hold thread objects
	std::vector<std::thread> threads;
	std::mutex lock;
	std::vector<std::pair<std::vector<uint64_t>, uint64_t>> avgs;
	int error = 0;

	srand(time(NULL));

	struct mapping *maps = (struct mapping *)malloc(sizeof(struct mapping) * num_objs);
	for (int i = 0; i < num_objs; i++) {
		setup_map(&maps[i], size_of_obj_in_blocks);
	}


	for (int i = 0; i < numthreads; ++i) {
		// Using a lambda function for the thread's task
		threads.emplace_back([&lock, &avgs, maps, num_objs, 
				size_of_obj_in_blocks, writes_per_iteration, times, i](){

				auto samples = random_write_task(maps, num_objs, 
					size_of_obj_in_blocks,
					writes_per_iteration, 
					times, i);
				lock.lock();
				avgs.push_back(samples);
				lock.unlock();

		});
	}

	// Join all threads to wait for them to finish
	for (auto& thread : threads) {
		thread.join();
	}

	return avgs;
}

void print_usage() {
	printf("Usage example: new_objsnap disk_path [-o value] [-d value] [-t value] [-r value] [-s]\n");
	printf("	-p 		location of disk (e.g., /dev/nvd0)\n");
	printf("	-o		num of objects to create and randomly select\n");
	printf("	-d		size of dirty set on each checkpoint\n"); 
	printf("	-t		number of threads to concurrently write\n");
	printf("	-r		run benchmark for this long in seconds\n");
	printf("	-g		size of each object in GiB's\n");
	printf("	-s		print internal stats at end of run\n");
	printf("	-h		print usage\n");
}


int
main(int argc, char *argv[])
{
	int opt;
	int totaldirtyset = 1;
	int runFor = 60;
	int numthreads = 1;
	int numobjs = 1;
	int ps = 0;
	int sizeGiB = 10;

	while ((opt = getopt(argc, argv, "p:o:d:t:r:g:sh")) != -1) {
		switch (opt) {
		case 'o':
			numobjs = atoi(optarg);
			break;
		case 'd':
			totaldirtyset = atoi(optarg);
			break;
		case 't':
			numthreads = atoi(optarg);
			break;
		case 'r':
			runFor = atoi(optarg);
			break;
		case 's':
			ps = 1;
			break;
		case 'p':
			disk = optarg;
			break;
		case 'g':
			sizeGiB = atoi(optarg);
			break;
		case 'h':
		default:
			print_usage();
			exit(1);
		}
	}

	clock_cycles = get_clock_speed_sleep();

	//basicTest();

	int error = setup(disk);
	if (error != 0) {
        	printf("Problem in Setup!");
		return (-1);
    	}

	int numblocks_per_obj_per_ckpt = totaldirtyset;
	int MiB = (1024 * 1024) / BLOCKSIZE;
	int GiB = (1024 * MiB);
	auto samples = threadedTest(numthreads, numobjs, sizeGiB * GiB, 
		numblocks_per_obj_per_ckpt, runFor);
	std::vector<uint64_t> totals_lat;
	std::vector<uint64_t> total_txns;
	for (auto &s: samples) {
		totals_lat.insert(totals_lat.begin(), std::get<0>(s).begin(), std::get<0>(s).end());
		total_txns.push_back(std::get<1>(s));
	}
	
	std::sort(totals_lat.begin(), totals_lat.end());
	double sum = 0;
	for (auto &s: totals_lat) {
		sum += s;
	}
	double lat_ns = sum / static_cast<double>(totals_lat.size());
	int n99 = static_cast<int>(static_cast<double>(totals_lat.size()) * 0.99);
	uint64_t lat_99_ns = totals_lat[n99];

	double iops = 0;
	double goodput = 0;
	for (auto &s : total_txns) {
		iops += s / runFor;
		goodput += ((double)s * (double)(totaldirtyset * 4)) / ((double)1024) ;

	}

	if (ps)
		printstats(clock_cycles);

	printf("objsnap, %d, %d, %lu, %f, %f, %lu, %f", numthreads, numobjs, totaldirtyset * BLOCKSIZE, iops, lat_ns, lat_99_ns, goodput);

	return (EX_OK);
}
