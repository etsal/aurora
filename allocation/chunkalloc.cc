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

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
pthread_cond_t hp_cond = PTHREAD_COND_INITIALIZER;

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
printchunk(struct chunklist *l) 
{
    printf("Chunk ptr[%d, %d], used[%d], tnx_size[%d], sectors_free[%ld], freed[%ld], max_sectors[%ld]\n", 
        l->ptr.offset, l->ptr.size, l->used, l->txn_size, l->sectors_free, l->freed, l->max_sectors);
}

static int getFreeChunk(struct chunkallocator *ca, struct chunklist **cl, uint32_t txn_size) {
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
    pthread_mutex_lock(&mutex);
    struct chunklist *curempty = NULL;
    diskptr_t *writeset = (diskptr_t *)malloc(sizeof(diskptr_t) * ca->txn_size, 
        M_WAITOK, M_CHUNKALLOC);
    uint32_t writeset_cnt = 0;

    
    while (!ca->terminate_thread) {
        // Wait for the condition variable to be signaled
        pthread_cond_wait(&cond, &mutex);

        pthread_mutex_unlock(&mutex);
        
        // Check if termination signal received while waiting
        if (ca->terminate_thread) {
            break;
        }
        // Find a candidate to empty
        if (curempty == NULL) {
            uint32_t max = 0;
            for (uint32_t i = 0; i < ca->num_chunks; i++) {
                struct chunklist *l = &ca->free_chunks[i];
                if ((l->used == FULL) && (l->sectors_free > max)) {
                    max = l->sectors_free;
                    curempty = l;
                }
            }
        }

        if (curempty == NULL) {
            continue;
        }
        

        // nothing to do with this
        if ((curempty->sectors_free <= (curempty->max_sectors / 4)) 
                && (ca->high_pressure == 0)) {
            curempty = NULL;
            pthread_mutex_lock(&mutex);
            continue;
        }
another_round:
        curempty->used = EMPTYING;

        if (ca->terminate_thread) {
            break;
        }

        // Check to see if all our sectors are free now!
        if (curempty->sectors_free == curempty->max_sectors) {
            assert(curempty->sectors_free != 0);
            for (uint32_t t = 0; t < curempty->max_sectors; t++) {
                assert(__builtin_popcount(curempty->sector_map[t].block_map) == 0);
            }

            curempty->freed += 1;
            curempty->used = 0;
            curempty = NULL;
            ca->high_pressure = 0;
            ca->used_chunks--;
            pthread_mutex_lock(&mutex);
            pthread_cond_signal(&hp_cond);
            continue;
        }

        // We now have a candidate to move
        // We must create a transaction that will free a given sector
        for (uint32_t i = 0; i < curempty->max_sectors; i++) {
            struct sector *map = &curempty->sector_map[i];
            if (map->block_map == 0)
                continue;

            for (uint32_t s = 0; s < curempty->txn_size; s++) {
                // We found a write that can be added to the writeset
                if (map->block_map & (1 << s)) {
                    // Get our pointer out
                    assert(writeset_cnt < curempty->txn_size);
                    writeset[writeset_cnt].offset = 
                        curempty->ptr.offset + (i * curempty->txn_size) + s;
                    writeset[writeset_cnt].size = 1;
                    writeset_cnt++;
                }
                if (writeset_cnt == curempty->txn_size) {
                    break;
                }
            }

            if (writeset_cnt == curempty->txn_size) {
                break;
            }
        }

        if (writeset_cnt > 0) {
            assert(txn_func);
            txn_func(writeset, writeset_cnt);
            writeset_cnt = 0;
        }

        goto another_round;
    }
    
    // Unlock the mutex before exiting
    pthread_mutex_unlock(&mutex);
    
    printf("Thread %ld terminated.\n", ca->tid);
    free(writeset, M_CHUNKALLOC);
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
    ca->free_chunks = (struct chunklist *)malloc(sizeof(struct chunklist) * chunks, M_CHUNKALLOC, M_WAITOK);
    ca->chunks_candidates = (struct chunklist **)malloc(sizeof(struct chunklist *) * ca->candidate_cnt, M_CHUNKALLOC, M_WAITOK);
    memset(ca->chunks_candidates, 0, sizeof(struct chunklist *) * ca->candidate_cnt);

    // Init our chunks
    for (uint32_t i = 0; i < ca->num_chunks; i++) {
        diskptr_t ptr;
        ptr.offset = starting_offset + (i * (CHUNKSIZE / BLOCKSIZE));
        ptr.size = CHUNKSIZE / BLOCKSIZE;
        struct chunklist *l = &ca->free_chunks[i];
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
    pthread_mutex_lock(&mutex);
    ca->terminate_thread = 1;
    pthread_cond_signal(&cond);
    pthread_mutex_unlock(&mutex);
    pthread_join(ca->tid, NULL);
}


int 
ca_destroy(struct chunkallocator *ca)
{
    teardown_thread(ca);
    free(ca->free_chunks, M_CHUNKALLOC);
    return 0;
}

int ca_print(struct chunkallocator *ca)
{
    printf("Chunk Allocator State: %ld\n", ca->num_chunks);
    int free = 0;
    uint64_t used_blocks = 0;
    for (uint32_t i = 0; i < ca->num_chunks; i++) {
        struct chunklist *cl = &ca->free_chunks[i];
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
allocate_from_chunk(struct chunklist *chunk, uint32_t numblocks, diskptr_t *ptr)
{
    if (!chunk->sectors_free) {
        return ENOSPC;
    }

    for (uint64_t i = 0; i < chunk->max_sectors; i++) {
        // Look for a free sector
        if (chunk->sector_map[i].block_map == 0) {
            // Create the pointer from the blockmap
            ptr->offset = chunk->ptr.offset + (i * chunk->txn_size);
            if (numblocks < chunk->txn_size) {
                chunk->sector_map[i].block_map = SETALL(numblocks);
                ptr->size = numblocks;
            } else {
                chunk->sector_map[i].block_map = SETALL(chunk->txn_size);
                ptr->size = chunk->txn_size;
            }
            assert(__builtin_popcount(chunk->sector_map[i].block_map) == ptr->size);
            chunk->sectors_free--;
            return (0);
        }
    }

    return (ENOSPC);
}

static void refup(struct chunklist *cl) {
    assert(cl);
    cl->refcnt++;
}

static void refdown(struct chunklist *cl) {
    assert(cl);
    cl->refcnt--;
}


int 
ca_alloc(struct chunkallocator *ca, uint32_t numblocks, diskptr_t *ptr)
{
    int error;
    int retries = 0;
ca_alloc_start:
    int bucket = determine_bucket(numblocks);
    struct chunklist *cl = ca->chunks_candidates[bucket];

    cl->mtx.lock();
    if (cl->used == FULL && (retries == 0)) {
        retries += 1;
        cl->mtx.unlock();
        goto ca_alloc_start;
    }

    if (!retries) {
        error = allocate_from_chunk(cl, numblocks, ptr);
        if (!error) {
            ca->allocations_from_chunk++;
            cl->mtx.unlock();
            return (0);
        }

        struct chunklist *newbucket;
        cl->used = FULL;
        // We could not allocate from a current chunk. So we need to acquire a new one
        error = getFreeChunk(ca, &newbucket, 1 << bucket);
        if (!error) {
            ca->chunks_candidates[bucket] = newbucket;
            cl->mtx.unlock();
            // We now unlock. Any thread waiting in the allocation will fail. 
            // Require the lock to try and refill it, see it marked as FULL and exit.
            goto ca_alloc_start;
        }
        cl->mtx.unlock();
    } else {
       cl->mtx.unlock(); 
    }

    ca->high_pressure = 1;
    pthread_cond_signal(&cond);

    // THIS THREAD NEEDS TO HELP EMPTY CHUNKS!
    // Our current candidate was a dud. Fallback path, find the first chunk that can fit out bucket
    for (uint32_t i = 0; i < ca->num_chunks; i++) {
        cl = &ca->free_chunks[i];
        cl->mtx.lock();
        if (cl->sectors_free > 0 && (cl->used != EMPTYING)) {
            numblocks = numblocks > cl->txn_size ? cl->txn_size : numblocks;
            error = allocate_from_chunk(cl, numblocks, ptr);
            if (!error && !cl->used) {
                cl->used = CURRENTLY_USED;
            }

            cl->mtx.unlock();
            if (error) {
                continue;
            }
            return (0);
        }
        cl->mtx.unlock();
    }

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

    struct chunklist *l = &ca->free_chunks[chunk];
    l->mtx.lock();
    uint32_t sector_i = (ptr.offset - l->ptr.offset) / l->txn_size;
    uint32_t bitmap_i = (ptr.offset - l->ptr.offset) % l->txn_size;
    assert((sector_i * l->txn_size) < (l->ptr.offset + l->ptr.size));
    assert((bitmap_i + ptr.size) <= l->txn_size);
    for (uint32_t i = 0; i < ptr.size; i++) {
        assert(l->sector_map[sector_i].block_map & (1 << (bitmap_i + i)));
        l->sector_map[sector_i].block_map = 
            UNSET(l->sector_map[sector_i].block_map, bitmap_i + i);
    }

    if (l->sector_map[sector_i].block_map == 0) {
        l->sectors_free++;
        pthread_cond_signal(&cond);
    }

    l->mtx.unlock();
    return 0;
}
