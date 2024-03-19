#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <cassert>
#include <thread>

#include "chunkalloc.h"

#define KiB (1024UL)
#define MiB (1024UL * KiB)
#define CHUNKSIZE (512UL * KiB)
#define HIGHPRESSURE (2)

#define SETALL(size) ((1 << (size)) - 1)
#define UNSET(num, i) ((num) & (~(1 << (i))))

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
pthread_cond_t hp_cond = PTHREAD_COND_INITIALIZER;

static void 
printchunk(struct chunklist *l) 
{
    printf("Chunk ptr[%d, %d], used[%d], sectors_free[%ld]\n", 
        l->ptr.offset, l->ptr.size, l->used, l->sectors_free);
}

static int getFreeChunk(struct chunkallocator *ca, struct chunklist **cl) {
    ca->new_chunk_calls++;
    for (uint32_t i = 0; i < ca->num_chunks; i++) {
        if (!ca->free_chunks[i].used) {
            ca->free_chunks[i].used = CURRENTLY_USED;
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
        if ((curempty->sectors_free <= (curempty->max_sectors / 2)) 
                && (ca->high_pressure == 0)) {
            curempty = NULL;
            pthread_mutex_lock(&mutex);
            continue;
        }
another_round:
        // Check to see if all our sectors are free now!
        if (curempty->sectors_free == curempty->max_sectors) {
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

    // These chunk lists act as a bitmap for chunks on the disk.
    ca->free_chunks = (struct chunklist *)malloc(sizeof(struct chunklist) * chunks, M_CHUNKALLOC, M_WAITOK);

    // Init our chunks
    for (uint32_t i = 0; i < ca->num_chunks; i++) {
        diskptr_t ptr;
        ptr.offset = starting_offset + (i * (CHUNKSIZE / BLOCKSIZE));
        ptr.size = CHUNKSIZE / BLOCKSIZE;
        struct chunklist *l = &ca->free_chunks[i];
        l->ptr = ptr;
        l->txn_size = ca->txn_size;
        l->max_sectors = ptr.size / l->txn_size;
        memset(l->sector_map, 0, l->max_sectors * sizeof(struct sector));
        l->index = i;
        l->used = 0;
        l->sectors_free = l->max_sectors;
    }

    ca->terminate_thread = 0;

    if (pthread_create(&ca->tid, NULL, chunk_collect, ca) != 0) {
        fprintf(stderr, "Error creating thread.\n");
        return 1;
    }

    // Set our current chunk
    getFreeChunk(ca, &ca->current_chunk);
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
    for (uint32_t i = 0; i < ca->num_chunks; i++) {
        if (!ca->free_chunks[i].used) {
            free += 1;
        }
    }
    printf("Free: %d\n", free);
    printf("Allocations from Chunk %lu\n", ca->allocations_from_chunk);
    printf("New Chunk calls %lu\n", ca->new_chunk_calls);
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
            chunk->sectors_free--;
            return (0);
        }
    }

    return (ENOSPC);
}

int 
ca_alloc(struct chunkallocator *ca, int numblocks, diskptr_t *ptr)
{
    int error;
ca_alloc_start:
    error = allocate_from_chunk(ca->current_chunk, numblocks, ptr);
    if (!error) {
        ca->allocations_from_chunk++;
        return (0);
    }

    ca->current_chunk->used = FULL;
    // We could not allocate from a current chunk. So we need to acquire a new one
    error = getFreeChunk(ca, &ca->current_chunk);
    if (error == 0 && ((ca->num_chunks - ca->used_chunks) > HIGHPRESSURE))
        goto ca_alloc_start;

    // THIS THREAD NEEDS TO HELP EMPTY CHUNKS!
    pthread_mutex_lock(&mutex);
    ca->high_pressure = 1;
    while (ca->high_pressure) {
        pthread_cond_signal(&cond);
        pthread_cond_wait(&hp_cond, &mutex);
    }

    pthread_mutex_unlock(&mutex);
    goto ca_alloc_start;
}

int 
ca_free(struct chunkallocator *ca, diskptr_t ptr)
{
    assert(ptr.size != (uint32_t)(-1));
    int chunk = ptr.offset / (CHUNKSIZE / BLOCKSIZE);

    struct chunklist *l = &ca->free_chunks[chunk];
    uint32_t sector_i = (ptr.offset - l->ptr.offset) / l->txn_size;
    uint32_t bitmap_i = ptr.offset % l->txn_size;
    assert((sector_i * l->txn_size) < (l->ptr.offset + l->ptr.size));
    assert((bitmap_i + ptr.size) <= l->txn_size);
    for (uint32_t i = 0; i < ptr.size; i++) {
        l->sector_map[sector_i].block_map = 
            UNSET(l->sector_map[sector_i].block_map, bitmap_i + i);
    }

    if (l->sector_map[sector_i].block_map == 0) {
        l->sectors_free++;
        pthread_cond_signal(&cond);
    }

    return 0;
}
