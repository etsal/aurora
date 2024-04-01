#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>

#include <cassert>
#include <thread>
#include <mutex>

#include "chunkalloc.h"

#define HIGHPRESSURE (2)

#define SETALL(size) ((size) == 64 ? UINT64_MAX : ((1ULL << (size)) - 1))
#define UNSETMASK(i) (~(1ULL << (i)))

pthread_mutex_t mtx = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
pthread_cond_t hp_cond = PTHREAD_COND_INITIALIZER;

#include <chrono>
using namespace std;
using namespace chrono;

// Enabling old chunks only really matters if real transactions can use the 64 page
// bucket. Otherwise old data is naturally aggregated together as regular transactions
// never allocate out of the 64 page bucket.
int enable_old_chunks = 0;

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
    ca->allocator_lock.lock();
    ca->new_chunk_calls++;
    if (ca->next_cnt == 0) {
        ca->allocator_lock.unlock();
        return ENOSPC;
    }

    *cl = ca->next_chunk[0];

    ca->next_cnt--;
    for (int i = 0; i < ca->next_cnt; i++) {
        ca->next_chunk[i] = ca->next_chunk[i + 1];
    }

    assert((*cl)->used == 0);
    (*cl)->used = CURRENTLY_USED;
    (*cl)->txn_size = txn_size;
    (*cl)->max_sectors = (*cl)->ptr.size / txn_size;
    (*cl)->sectors_free = (*cl)->max_sectors;
    ca->used_chunks++;

    ca->allocator_lock.unlock();
    return (0);
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
    ca->moved = 0;


    // These chunk lists act as a bitmap for chunks on the disk.
    ca->chunks = (struct chunk *)malloc(sizeof(struct chunk) * chunks, M_CHUNKALLOC, M_WAITOK);
    ca->chunks_candidates = (struct chunk **)malloc(sizeof(struct chunk *) * ca->candidate_cnt, M_CHUNKALLOC, M_WAITOK);
    ca->chunks_candidates_old = (struct chunk **)malloc(sizeof(struct chunk *) * ca->candidate_cnt, M_CHUNKALLOC, M_WAITOK);

    ca->old_chunks = (struct chunk **)malloc(sizeof(struct chunk *) * chunks, M_CHUNKALLOC, M_WAITOK);
    ca->next_chunk = (struct chunk **)malloc(sizeof(struct chunk *) * chunks, M_CHUNKALLOC, M_WAITOK);
    ca->thread_worklist = (struct chunk **)malloc(sizeof(struct chunk *) * MAXTHREADS, M_CHUNKALLOC, M_WAITOK);
    ca->old_cnt = 0;

    memset(ca->chunks_candidates, 0, sizeof(struct chunk *) * ca->candidate_cnt);
    memset(ca->thread_worklist, 0, sizeof(struct chunk *) * MAXTHREADS);

    // Init our chunks
    for (uint32_t i = 0; i < ca->num_chunks; i++) {
        diskptr_t ptr;
        ptr.offset = starting_offset + (i * (CHUNKSIZE / BLOCKSIZE));
        ptr.size = CHUNKSIZE / BLOCKSIZE;
        struct chunk *l = &ca->chunks[i];
        l->ptr = ptr;
        memset(l->sector_map, 0, MAXSECTORS * sizeof(struct sector));
        l->index = i;
        l->used = 0;
        l->freed = 0;
        ca->next_chunk[i] = &ca->chunks[i];
    }

    ca->next_cnt = ca->num_chunks;
    ca->terminate_thread = 0;

    if (pthread_create(&ca->tid, NULL, chunk_collect, ca) != 0) {
        fprintf(stderr, "Error creating thread.\n");
        return 1;
    }

    for (uint32_t i = 0; i < ca->candidate_cnt; i++) {
        getFreeChunk(ca, &ca->chunks_candidates[i], 1ULL << i);
    }

    for (uint32_t i = 0; i < ca->candidate_cnt; i++) {
        getFreeChunk(ca, &ca->chunks_candidates_old[i], 1ULL << i);
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
    free(ca->chunks, M_CHUNKALLOC);
    free(ca->next_chunk, M_CHUNKALLOC);
    free(ca->chunks_candidates, M_CHUNKALLOC);
    free(ca->chunks_candidates_old, M_CHUNKALLOC);
    return 0;
}

static void
append_old_chunk(struct chunkallocator *ca, struct chunk *entry)
{
    assert(entry->used == FULL);
    ca->allocator_lock.lock();
    ca->old_chunks[ca->old_cnt] = entry;
    assert(entry->used == FULL);
    ca->old_cnt++;
    ca->allocator_lock.unlock();
}

struct allocatorstats ca_stat(struct chunkallocator *ca)
{
    struct allocatorstats stats;
    int free = 0;
    uint64_t used_blocks = 0;
    for (uint32_t i = 0; i < ca->num_chunks; i++) {
        struct chunk *cl = &ca->chunks[i];
        uint64_t blocks = 0;
        for (uint32_t t = 0; t < cl->max_sectors; t++) {
            blocks += __builtin_popcountll(cl->sector_map[t].block_map);
        }

        if (!cl->used) {
            free += 1;
            if (blocks != 0) {
                for (uint32_t t = 0; t < cl->max_sectors; t++) {
                    int blockey =  __builtin_popcountll(cl->sector_map[t].block_map);
                    printf("[%d] %d\n", t, blockey);
                }
                printchunk(cl);
                assert(false);
            }
        }

        used_blocks += blocks;
    }

    stats.list[0] = free;
    stats.list[1] = ca->num_chunks;
    stats.list[2] = ca->allocations_from_chunk;
    stats.list[3] = ca->new_chunk_calls;
    stats.list[4] = ca->emptys;
    stats.list[5] = used_blocks;
    stats.list[6] = ca->moved;
    stats.numStats = 7;
    return stats;
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

            if (__builtin_popcountll(chunk->sector_map[i].block_map) != ptr.size) {
                printf("%d\n", __builtin_popcountll(chunk->sector_map[i].block_map));
                printf("%d %d\n", numblocks, chunk->txn_size);
                printf("%u %u\n", ptr.offset, ptr.size);
                printchunk(chunk);
                assert(false);
            }

            chunk->blocks_used += ptr.size;
            chunk->sectors_free--;
            // Update our transaction list
            for (uint32_t t = 0; t < ptr.size; t++) {
                txns[t].ptr.offset = ptr.offset + t;
                txns[t].ptr.size = 1;
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

#define BREAKPOINT (MAXTHREADS)
static void
ensure_workset(struct chunkallocator *ca, int thread_id)
{
    // First we check to see if we have a worklist already!
    if (ca->thread_worklist[thread_id] == NULL) {
        // We need to get some work to do but we also need to ensure
        // That enough logical time has passed. Meaning Some amount of blocks
        // Have been appended to the old queue
        ca->allocator_lock.lock();
        if (ca->old_cnt < BREAKPOINT && ca->next_cnt > BREAKPOINT) {
            ca->allocator_lock.unlock();
            return;
        }

        int i = 0;
        uint64_t min = UINT64_MAX;
        int cur = 0;
        // This is terrible code need to clean it up
        for (i = 0; i < ca->old_cnt; i++) {
            // This is completely free put it on the next queue
            if (ca->old_chunks[i]->used == 0) {
                assert(ca->old_chunks[i]->used == 0);
                ca->next_chunk[ca->next_cnt] = ca->old_chunks[i];
                ca->next_cnt++;
                for (int t = i; t < ca->old_cnt; t++) {
                    ca->old_chunks[t] = ca->old_chunks[t + 1];
                }
                // Need to reset out end point
                ca->old_cnt--;
                // And start the for loop over from current element
                i--;
                continue;
            }
            if (ca->old_chunks[i]->used != FULL) {
                printchunk(ca->old_chunks[i]);
                assert(false);
            }
            if (ca->old_chunks[i]->blocks_used < min) {
                min = ca->old_chunks[i]->blocks_used;
                cur = i;
            }
        }

        if (ca->old_cnt == 0) {
            return;
        }
        // There is some work that we should pop it off the list
        ca->thread_worklist[thread_id] = ca->old_chunks[cur];
        assert(ca->thread_worklist[thread_id]);
        assert(ca->thread_worklist[thread_id]->used == FULL);
        for (int i = cur; i < ca->old_cnt; i++) {
            ca->old_chunks[i] = ca->old_chunks[i + 1];
        }
        ca->old_cnt--;
        ca->allocator_lock.unlock();
    }
}

static void
move_data(struct chunkallocator *ca, int thread_id, uint32_t numblocks) {
    struct chunk *curempty = NULL;

    ensure_workset(ca, thread_id);
    curempty = ca->thread_worklist[thread_id];
    if (!curempty) {
        assert(ca->next_cnt > BREAKPOINT);
        return;
    }

    curempty->mtx.lock();
    curempty->used = EMPTYING;

    // We now have a candidate to move
    // We must create a transaction that will free a given sector
    struct transaction writeset[64];
    uint32_t writeset_cnt = 0;
#ifdef MAX_BLOCKS_TO_MOVE 
    uint32_t max_write_set = MAX_BLOCKS_TO_MOVE;
#else
    uint32_t max_write_set = curempty->txn_size;
#endif
    if (ca->next_cnt > (2 * MAXTHREADS)) {
        curempty->mtx.unlock();
        return;
    }

    int sector_freed = 0;
    for (uint32_t i = 0; i < curempty->max_sectors; i++) {
        struct sector *map = &curempty->sector_map[i];
        if (map->block_map == 0)
            continue;

        if (writeset_cnt == max_write_set) {
            break;
        }

        for (uint32_t s = 0; s < curempty->txn_size; s++) {
            // We found a write that can be added to the writeset
            if (map->block_map & (1ULL << s)) {
                // Get our pointer out
                writeset[writeset_cnt].ptr.offset = 
                    curempty->ptr.offset + (i * curempty->txn_size) + s;
                writeset[writeset_cnt].ptr.size = 1;
                writeset[writeset_cnt].inode = map->objects[s].inode;
                writeset[writeset_cnt].offset = map->objects[s].offset;
                writeset_cnt++;
                auto before = __builtin_popcountll(map->block_map);
                map->block_map &= UNSETMASK(s);
                curempty->blocks_used -= 1;
                assert((before - 1) == __builtin_popcountll(map->block_map));
                if (map->block_map == 0) {
                    sector_freed = 1;
                    curempty->sectors_free++;
                    break;
                }
            }

            if (writeset_cnt == max_write_set) {
                break;
            }

            if (sector_freed) {
                break;
            }
        }

        if (writeset_cnt == max_write_set) {
            break;
        }

        if (sector_freed) {
            break;
        }
        
        if (map->block_map != 0) {
            assert(false);
        }

    }

    if (curempty->sectors_free == curempty->max_sectors) {
        curempty->used = 0;
        ca->next_chunk[ca->next_cnt] = curempty;
        ca->next_cnt++;
        ca->used_chunks--;
        ca->emptys++;
        curempty->mtx.unlock();
        curempty = NULL;
        ca->thread_worklist[thread_id] = NULL;
        ensure_workset(ca, thread_id);
        curempty = ca->thread_worklist[thread_id];
    } else {
        curempty->mtx.unlock();
    }

    if (writeset_cnt > 0) {
        assert(txn_func);
        if (writeset_cnt > max_write_set) {
            printf("%d %d\n", writeset_cnt, max_write_set);
            assert(false);
        }
        txn_func(writeset, writeset_cnt);
        ca->moved += writeset_cnt;
        writeset_cnt = 0;
    }
}

int 
ca_alloc(struct chunkallocator *ca, struct transaction *txns, uint32_t numblocks, int flag)
{
    int error;
    int bucket;
    int allocated = false;
ca_alloc_start:
    struct chunk *cl = NULL;
    while ((ca->next_cnt < MAXTHREADS) && !flag) {
        move_data(ca, 0, 1ULL << bucket);
    }

    bucket = determine_bucket(numblocks);
    ca->allocator_lock.lock();
    if (flag && enable_old_chunks) {
        cl = ca->chunks_candidates_old[bucket];
    } else {
        cl = ca->chunks_candidates[bucket];
    }
    cl->mtx.lock();
    ca->allocator_lock.unlock();


    if (cl->used != FULL) {
        error = allocate_from_chunk(cl, txns, numblocks);
        if (!error) {
            allocated = true;
            ca->allocations_from_chunk++;
            cl->mtx.unlock();

            assert(cl->used != FULL);
            // Before they leave - attempt to move data!
            // WE ARE CURRENTLY PASSING IN 0, THIS WILL BE THE TID
            if (!flag) {
                move_data(ca, 0, 1ULL << bucket);
            }

            return (0);
        }
    }

    struct chunk *newbucket;
    if (cl->used != CURRENTLY_USED && cl->used != FULL) {
        printchunk(cl);
        assert(false);
    }
    cl->used = FULL;
    // We could not allocate from a current chunk. So we need to acquire a new one
    error = getFreeChunk(ca, &newbucket, 1ULL << bucket);
    if (!error) {
        append_old_chunk(ca, cl);
        if (flag && enable_old_chunks) {
            ca->chunks_candidates_old[bucket] = newbucket;
            assert(ca->chunks_candidates_old[bucket]->used == CURRENTLY_USED);
            assert(ca->chunks_candidates_old[bucket]->used != FULL);
        } else {
            ca->chunks_candidates[bucket] = newbucket;
            assert(ca->chunks_candidates[bucket]->used == CURRENTLY_USED);
            assert(ca->chunks_candidates[bucket]->used != FULL);
        }
        cl->mtx.unlock();
        // We now unlock. Any thread waiting in the allocation will fail. 
        // Require the lock to try and refill it, see it marked as FULL and exit.
        assert(newbucket->used != FULL);
        if (allocated)
            return (0);
        goto ca_alloc_start;
    }

    cl->mtx.unlock();
    printf("RESTARTING!\n");
    goto ca_alloc_start;

    uint64_t total_space_available = 0;
    uint64_t total_space_blocks = 0;
    for (uint32_t i = 0; i < ca->num_chunks; i++) {
        cl = &ca->chunks[i];
        if (cl->used) {
            total_space_available += ((cl->txn_size) * BLOCKSIZE) * cl->sectors_free;
            for (int i = 0; i < cl->max_sectors; i++) {
                total_space_blocks += __builtin_popcountll(cl->sector_map[i].block_map);
            }
        }
    }


    printf("Could not find chunk for %d\n", numblocks);
    printf("Total Space in free sectors: %ld MiB\n", total_space_available / (1024UL * 1024UL));
    printf("Total Space in free blocks: %ld MiB\n", total_space_blocks / (1024UL * 1024UL));
    assert(false);
    goto ca_alloc_start;
}

int 
ca_free(struct chunkallocator *ca, diskptr_t ptr)
{
    assert(ptr.size != (uint32_t)(-1));
    int chunk = ptr.offset / (CHUNKSIZE / BLOCKSIZE);
    if (chunk > ca->num_chunks) {
        printf("%u %u\n", ptr.offset, ptr.size);
        assert(false);
    }

    struct chunk *l = &ca->chunks[chunk];
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
        // Need to check if its set to not double count
        // It may not be set if data was moved, this is likely to not happen
        // In the kernel version as any free will be away of the data moving
        // we just cant do it yet cause we are not pre allocationg things with a WAL
        if (l->sector_map[sector_i].block_map & (1ULL << (bitmap_i + i))) {
            l->sector_map[sector_i].block_map &= UNSETMASK(bitmap_i + i);
            l->blocks_used -= 1;
        }
    }



    // Recheck if its zero
    if (l->sector_map[sector_i].block_map == 0) {
        l->sectors_free++;
    }
    auto blocksused = 0;
    for (int i = 0; i < l->max_sectors; i++) {
        blocksused += __builtin_popcountll(l->sector_map[i].block_map);
    }

    assert(blocksused == l->blocks_used);

    l->mtx.unlock();
    return 0;
}
