#ifndef __CHUNKALLOC_H__
#define __CHUNKALLOC_H__

#include <functional>

#include "binaryalloc.h"

#define BLOCKSIZE (4096)
#define MAXSECTORS (4096)

#define CURRENTLY_USED (1)
#define FULL (2)

extern std::function<void(diskptr_t *, int cnt)> txn_func;

struct sector {
    uint64_t block_map; // uint64_t means that the max sector size is 64 blocks. (256 MiB)
};

// Chunks are 256 MiB in size. The smallest sector size is a single block.
// That would mean ((256 * 1024) / 4) = 65536 total addressable sectors within
// a chunk. Requiring 1024 sectors as a max to address it.
// In this example 
// sector[0] = blocks 0..63
// sector[1] = blocks 64..127
struct chunklist {
    uint8_t used;
    uint32_t txn_size;
    diskptr_t ptr;
    int index;
    struct sector sector_map[MAXSECTORS];
    uint64_t sectors_free;
    uint64_t max_sectors;
};

struct chunkallocator {
    struct chunklist *free_chunks;    

    struct chunklist *current_chunk;
    uint64_t num_chunks;
    uint64_t used_chunks;
    uint64_t starting_offset;
    uint32_t txn_size;

    uint64_t allocations_from_chunk;
    uint64_t new_chunk_calls;

    pthread_t tid;
    int terminate_thread;
    int high_pressure;
};

int ca_init(struct chunkallocator *ca, off_t starting_offset, uint64_t disksize, int txn_size_in_blocks);
int ca_destroy(struct chunkallocator *ca);
int ca_print(struct chunkallocator *ca);

int ca_alloc(struct chunkallocator *ca, int numblocks, diskptr_t *ptr);
int ca_free(struct chunkallocator *ca, diskptr_t ptr);
#endif