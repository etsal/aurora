#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>

#include <cassert>
#include <thread>
#include <mutex>

#include "chunkalloc.h"

#define KiB (1024UL)
#define MiB (1024UL * KiB)
#define HIGHPRESSURE (2)

#define SETALL(size) ((1 << (size)) - 1)
#define UNSET(num, i) ((num) & (~(1 << (i))))

pthread_mutex_t mtx = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
pthread_cond_t hp_cond = PTHREAD_COND_INITIALIZER;

#include <chrono>
using namespace std;
using namespace chrono;

static int 
determine_bucket(int numblocks)
{
    int i = 1;
    int shift;
    for (shift = 0; shift <= 32; shift++) {
        if (numblocks <= (i << shift)) {
            return (shift);
        }
    }

    assert(false);
}


[[maybe_unused]] static void 
printchunk(struct chunk *l) 
{
    printf("Chunk ptr[%d, %d], used[%d], tnx_size[%d], sectors_free[%ld], freed[%ld], max_sectors[%ld]\n", 
        l->ptr.offset, l->ptr.size, l->used, l->txn_size, l->sectors_free, l->freed, l->max_sectors);
}

static int 
getFreeChunk(struct chunkallocator *ca, struct chunk **cl, uint32_t txn_size) {
    ca->new_chunk_calls++;
    for (uint32_t i = 0; i < ca->num_chunks; i++) {
        if (!ca->free_chunks[i].used) {
            ca->free_chunks[i].used = CURRENTLY_USED;
            ca->free_chunks[i].txn_size = txn_size;
            ca->free_chunks[i].max_sectors =  ca->free_chunks[i].ptr.size / txn_size;
            ca->free_chunks[i].sectors_free = ca->free_chunks[i].max_sectors;
            *cl = &ca->free_chunks[i];
            ca->used_chunks++;
            return (0);
        }
    }

    return ENOSPC;
}

static void*
chunk_collect(void *arg) {    
    struct chunkallocator *ca = (struct chunkallocator *)arg;
    // You can extract any other arguments similarly
    
    // Lock the mutex before entering the loop
    pthread_mutex_lock(&mtx);
    
    while (!ca->terminate_thread) {
        // Wait for the condition variable to be signaled
        pthread_cond_wait(&cond, &mtx);

        pthread_mutex_unlock(&mtx);
        
        // Check if termination signal received while waiting
        if (ca->terminate_thread) {
            break;
        }
    }
    
    // Unlock the mutex before exiting
    pthread_mutex_unlock(&mtx);
    
    printf("Thread %ld terminated.\n", ca->tid);
    pthread_exit(NULL);
}

int 
ca_init(struct chunkallocator *ca, off_t starting_offset, size_t disksize, int txn_size_in_blocks)
{
    int chunks = disksize / CHUNKSIZE;
    ca->num_chunks = chunks;
    ca->used_chunks = 0;
    ca->starting_offset = starting_offset;
    ca->txn_size = txn_size_in_blocks;
    ca->high_pressure = 0;

    ca->allocations_from_chunk = 0;
    ca->new_chunk_calls = 0;
    ca->candidate_cnt = determine_bucket(ca->txn_size) + 1;


    // These chunk lists act as a bitmap for chunks on the disk.
    ca->free_chunks = (struct chunk *)malloc(sizeof(struct chunk) * chunks, M_CHUNKALLOC, M_WAITOK);
    ca->chunks_candidates = (struct chunk **)malloc(sizeof(struct chunk *) * ca->candidate_cnt, M_CHUNKALLOC, M_WAITOK);

    ca->old_chunks = (struct chunk **)malloc(sizeof(struct chunk *) * chunks, M_CHUNKALLOC, M_WAITOK);
    ca->thread_worklist = (struct chunk **)malloc(sizeof(struct chunk *) * 128, M_CHUNKALLOC, M_WAITOK);
    ca->old_cnt = 0;

    memset(ca->chunks_candidates, 0, sizeof(struct chunk *) * ca->candidate_cnt);
    memset(ca->thread_worklist, 0, sizeof(struct chunk *) * 128);

    // Init our chunks
    for (uint32_t i = 0; i < ca->num_chunks; i++) {
        diskptr_t ptr;
        ptr.offset = starting_offset + (i * (CHUNKSIZE / BLOCKSIZE));
        ptr.size = CHUNKSIZE / BLOCKSIZE;
        struct chunk *l = &ca->free_chunks[i];
        l->ptr = ptr;
        memset(l->sector_map, 0, MAXSECTORS * sizeof(struct sector));
        l->index = i;
        l->used = 0;
        l->freed = 0;
    }

    ca->terminate_thread = 0;

    if (pthread_create(&ca->tid, NULL, chunk_collect, ca) != 0) {
        fprintf(stderr, "Error creating thread.\n");
        return 1;
    }

    for (uint32_t i = 0; i < ca->candidate_cnt; i++) {
        getFreeChunk(ca, &ca->chunks_candidates[i], 1 << i);
    }


    ca->emptys = 0;
    return 0;
}

void teardown_thread(struct chunkallocator *ca) {
    pthread_mutex_lock(&mtx);
    ca->terminate_thread = 1;
    pthread_cond_signal(&cond);
    pthread_mutex_unlock(&mtx);
    pthread_join(ca->tid, NULL);
}


int 
ca_destroy(struct chunkallocator *ca)
{
    teardown_thread(ca);
    free(ca->free_chunks, M_CHUNKALLOC);
    return 0;
}

static void
append_old_chunk(struct chunkallocator *ca, struct chunk *entry)
{
    assert(entry->used == FULL);
    ca->allocator_lock.lock();
    ca->old_chunks[ca->old_cnt] = entry;
    ca->old_cnt++;
    ca->allocator_lock.unlock();
}

int ca_print(struct chunkallocator *ca)
{
    printf("Chunk Allocator State: %ld\n", ca->num_chunks);
    int free = 0;
    uint64_t used_blocks = 0;
    for (uint32_t i = 0; i < ca->num_chunks; i++) {
        struct chunk *cl = &ca->free_chunks[i];
        uint64_t blocks = 0;
    
        for (uint32_t t = 0; t < cl->max_sectors; t++) {
            blocks += __builtin_popcount(cl->sector_map[t].block_map);
        }

        if (!ca->free_chunks[i].used) {
            free += 1;
            if (blocks != 0) {
                for (uint32_t t = 0; t < cl->max_sectors; t++) {
                    int blockey =  __builtin_popcount(cl->sector_map[t].block_map);
                    printf("[%d] %d\n", t, blockey);
                }
                printchunk(cl);
                assert(false);
            }
        }

        used_blocks += blocks;
    }
    printf("Free: %d\n", free);
    printf("Allocations from Chunk %lu\n", ca->allocations_from_chunk);
    printf("New Chunk calls %lu\n", ca->new_chunk_calls);
    printf("Emptys done %d\n", ca->emptys);
    printf("Used Blocks %ldMiB\n", (used_blocks * BLOCKSIZE) / (1024 * 1024));
    return 0;
}

static int
allocate_from_chunk(struct chunk *chunk, struct transaction *txns, uint32_t numblocks)
{
    diskptr_t ptr;
    if (!chunk->sectors_free) {
        return ENOSPC;
    }

    for (uint64_t i = 0; i < chunk->max_sectors; i++) {
        // Look for a free sector
        if (chunk->sector_map[i].block_map == 0) {
            // Create the pointer from the blockmap
            ptr.offset = chunk->ptr.offset + (i * chunk->txn_size);
            if (numblocks < chunk->txn_size) {
                chunk->sector_map[i].block_map = SETALL(numblocks);
                ptr.size = numblocks;
            } else {
                chunk->sector_map[i].block_map = SETALL(chunk->txn_size);
                ptr.size = chunk->txn_size;
            }
            assert(__builtin_popcount(chunk->sector_map[i].block_map) == ptr.size);
            chunk->sectors_free--;
            // Update our transaction list
            for (uint32_t t = 0; t < ptr.size; t++) {
                txns[t].ptr.offset = ptr.offset + t;
                txns[t].ptr.size = 1;
                txns[t].ptr.obj_offset = txns[t].offset;
                txns[t].ptr.obj_id = txns[t].inode;
                // We need to update our sectors objectid list this is to enable
                // our ability to move data.
                chunk->sector_map[i].objects[t].inode = txns[t].inode;
                chunk->sector_map[i].objects[t].offset = txns[t].offset;
            }
            return (0);
        }
    }

    return (ENOSPC);
}

#define BREAKPOINT (2000)

static void
move_data(struct chunkallocator *ca, int thread_id, uint32_t numblocks) {
    struct chunk *curempty = NULL;

tryagain:

    // First we check to see if we have a worklist already!
    if (ca->thread_worklist[thread_id] == NULL) {
        // We need to get some work to do but we also need to ensure
        // That enough logical time has passed. Meaning Some amount of blocks
        // Have been appended to the old queue
        ca->allocator_lock.lock();
        if (ca->old_cnt < BREAKPOINT) {
            ca->allocator_lock.unlock();
            return;
        }

        // There is some work that we should pop it off the list
        ca->thread_worklist[thread_id] = ca->old_chunks[0];
        assert(ca->thread_worklist[thread_id]);
        for (int i = 1; i < ca->old_cnt; i++) {
            ca->old_chunks[i - 1] = ca->old_chunks[i];
        }
        ca->old_cnt--;
        ca->allocator_lock.unlock();
    }


    curempty = ca->thread_worklist[thread_id];
    curempty->mtx.lock();
    curempty->used = EMPTYING;
    int sector_threshold = curempty->sectors_free < (curempty->max_sectors >> 1);
    int chunk_threshold = (ca->num_chunks - ca->used_chunks) > 512;
    if (sector_threshold && chunk_threshold) {
        curempty->mtx.unlock();
        return;
    }

    if (curempty->sectors_free == curempty->max_sectors) {
        curempty->used = 0;
        curempty->mtx.unlock();
        curempty = NULL;
        ca->thread_worklist[thread_id] = NULL;
        ca->used_chunks--;
        goto tryagain;
    }

    // We now have a candidate to move
    // We must create a transaction that will free a given sector
    struct transaction writeset[64];
    uint32_t writeset_cnt = 0;
    uint32_t max_write_set = numblocks;
    for (uint32_t i = 0; i < curempty->max_sectors; i++) {
        struct sector *map = &curempty->sector_map[i];
        if (map->block_map == 0)
            continue;

        for (uint32_t s = 0; s < curempty->txn_size; s++) {
            // We found a write that can be added to the writeset
            if (map->block_map & (1 << s)) {
                // Get our pointer out
                writeset[writeset_cnt].ptr.offset = 
                    curempty->ptr.offset + (i * curempty->txn_size) + s;
                writeset[writeset_cnt].ptr.size = 1;
                writeset[writeset_cnt].inode = map->objects[s].inode;
                writeset[writeset_cnt].offset = map->objects[s].offset;
                writeset_cnt++;
                map->block_map = UNSET(map->block_map, s);
                if (map->block_map == 0) {
                    curempty->sectors_free++;
                    break;
                }
            }


            if (writeset_cnt == max_write_set) {
                break;
            }
        }

        if (writeset_cnt == max_write_set) {
            break;
        }
    }

    if (writeset_cnt > 0) {
        assert(txn_func);
        curempty->mtx.unlock();
        txn_func(writeset, writeset_cnt);
        writeset_cnt = 0;
    } else {
        curempty->mtx.unlock();
    }
}

int 
ca_alloc(struct chunkallocator *ca, struct transaction *txns, uint32_t numblocks, int flag)
{
    int error;
    int bucket;
    struct chunk *cl;
ca_alloc_start:
    bucket = determine_bucket(numblocks);
    cl = ca->chunks_candidates[bucket];

    cl->mtx.lock();
    if (cl->used == FULL) {
        cl->mtx.unlock();
        goto ca_alloc_start;
    }

    error = allocate_from_chunk(cl, txns, numblocks);
    if (!error) {
        ca->allocations_from_chunk++;
        cl->mtx.unlock();

        // Before they leave - attempt to move data!
        // WE ARE CURRENTLY PASSING IN 0, THIS WILL BE THE TID
        if (!flag)
            move_data(ca, 0, 1 << bucket);

        return (0);
    }

    struct chunk *newbucket;
    cl->used = FULL;
    // We could not allocate from a current chunk. So we need to acquire a new one
    error = getFreeChunk(ca, &newbucket, 1 << bucket);
    if (!error) {
        ca->chunks_candidates[bucket] = newbucket;
        cl->mtx.unlock();
        append_old_chunk(ca, cl);
        // We now unlock. Any thread waiting in the allocation will fail. 
        // Require the lock to try and refill it, see it marked as FULL and exit.
        goto ca_alloc_start;
    }
    cl->mtx.unlock();

    assert(false);
    // ca->high_pressure = 1;
    // pthread_cond_signal(&cond);

    // // Our current candidate was a dud. We are running out of space path,
    // // start giving out any free space we can find. 
    // for (uint32_t i = 0; i < ca->num_chunks; i++) {
    //     cl = &ca->free_chunks[i];
    //     cl->mtx.lock();
    //     if (cl->sectors_free > 0 && (cl->used != EMPTYING)) {
    //         numblocks = numblocks > cl->txn_size ? cl->txn_size : numblocks;
    //         error = allocate_from_chunk(cl, numblocks, ptr);
    //         if (!error && !cl->used) {
    //             cl->used = CURRENTLY_USED;
    //         }

    //         cl->mtx.unlock();
    //         if (error) {
    //             continue;
    //         }
    //         return (0);
    //     }
    //     cl->mtx.unlock();
    // }

    goto ca_alloc_start;

    uint64_t total_space_available = 0;
    for (uint32_t i = 0; i < ca->num_chunks; i++) {
        cl = &ca->free_chunks[i];
        printchunk(cl);
        total_space_available += ((cl->txn_size) * BLOCKSIZE) * cl->sectors_free;
    }


    printf("Could not find chunk for %d\n", numblocks);
    printf("Total Space in free sectors: %ld MiB\n", total_space_available / (1024UL * 1024UL));
    assert(false);

    goto ca_alloc_start;
}

int 
ca_free(struct chunkallocator *ca, diskptr_t ptr)
{
    assert(ptr.size != (uint32_t)(-1));
    int chunk = ptr.offset / (CHUNKSIZE / BLOCKSIZE);

    struct chunk *l = &ca->free_chunks[chunk];
    l->mtx.lock();
    uint32_t sector_i = (ptr.offset - l->ptr.offset) / l->txn_size;
    uint32_t bitmap_i = (ptr.offset - l->ptr.offset) % l->txn_size;
    assert((sector_i * l->txn_size) < (l->ptr.offset + l->ptr.size));
    assert((bitmap_i + ptr.size) <= l->txn_size);
    // We freed this with a move, this wont actually happen with pre-allocated
    // wals for now lets just return
    if (l->sector_map[sector_i].block_map == 0) {
        l->mtx.unlock();
        return (0);
    }

    for (uint32_t i = 0; i < ptr.size; i++) {
        l->sector_map[sector_i].block_map = 
            UNSET(l->sector_map[sector_i].block_map, bitmap_i + i);
        assert(l->sector_map[sector_i].objects[bitmap_i + i].offset == ptr.obj_offset);
    }

    // Recheck if its zero
    if (l->sector_map[sector_i].block_map == 0) {
        l->sectors_free++;
    }

    l->mtx.unlock();
    return 0;
}
