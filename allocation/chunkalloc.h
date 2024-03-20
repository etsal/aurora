#ifndef __CHUNKALLOC_H__
#define __CHUNKALLOC_H__

#include <functional>
#include <mutex>
#include <atomic>

#include "binaryalloc.h"

#define CHUNKSIZE (1024UL * KiB)
#define MAXSECTORS (1024 / 4)

#define BLOCKSIZE (4096)

#define CURRENTLY_USED (1)
#define EMPTYING (2)
#define FULL (3)

extern std::function<void(struct transaction *txns, int cnt)> txn_func;

struct transaction {
   uint32_t inode; 
   uint32_t offset;
   diskptr_t ptr;
};

struct objectid {
    uint32_t inode;
    uint32_t offset;
};

struct sector {
    uint64_t block_map; // uint64_t means that the max sector size is 64 blocks. (256 MiB)
    struct objectid objects[64];
};

struct chunk {
    struct sector sector_map[MAXSECTORS];
    int index;
    uint8_t used;
    uint32_t txn_size;
    uint64_t sectors_free;
    uint64_t max_sectors;
    diskptr_t ptr;
    uint64_t freed;

    std::mutex mtx;
};

struct chunkallocator {
    struct chunk *free_chunks;    

    // This is the list of binary allocation candidates
    // [0] = 1 block
    // [1] = 2 blocks
    // [2] = 4 blocks
    struct chunk **chunks_candidates;

    // When a chunk gets used
    struct chunk **old_chunks;
    int old_cnt;

    struct chunk **thread_worklist;
    std::mutex allocator_lock;

    uint64_t candidate_cnt;
    uint64_t num_chunks;
    uint64_t used_chunks;
    uint64_t starting_offset;
    uint32_t txn_size;

    uint64_t allocations_from_chunk;
    uint64_t new_chunk_calls;

    pthread_t tid;
    int terminate_thread;
    int high_pressure;

    int emptys;
};

int ca_init(struct chunkallocator *ca, off_t starting_offset, uint64_t disksize, int txn_size_in_blocks);
int ca_destroy(struct chunkallocator *ca);
int ca_print(struct chunkallocator *ca);

int ca_alloc(struct chunkallocator *ca, struct transaction *txn, uint32_t numblocks, int flag);
int ca_free(struct chunkallocator *ca, diskptr_t ptr);
#endif