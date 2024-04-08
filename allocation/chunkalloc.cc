#include <errno.h>
#include <stdio.h>
#include <string.h>

#include <cassert>
#include <mutex>
#include <thread>

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

// Enabling old chunks only really matters if real transactions can use the 64
// page bucket. Otherwise old data is naturally aggregated together as regular
// transactions never allocate out of the 64 page bucket.
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
printchunk(struct chunk* l, bool verbose = false)
{
  printf("Chunk ptr[%d, %d], used[%d], tnx_size[%d], sectors_free[%ld], "
         "freed[%ld], max_sectors[%ld], blocks_used[%lu]\n",
         l->ptr.offset,
         l->ptr.size,
         l->used,
         l->txn_size,
         l->sectors_free,
         l->freed,
         l->max_sectors,
         l->blocks_used);
  if (verbose) {
    for (int i = 0; i < l->max_sectors; i++) {
      printf("[%d] %llx\n", i, l->sector_map[i].block_map);
    }
  }
  fflush(stdout);
}

void
assertchunk(struct chunk* curempty, int line, int flag = 0)
{
  uint64_t free_sectors = 0;
  uint64_t blocks = 0;
  for (int i = 0; i < curempty->max_sectors; i++) {
    struct sector* map = &curempty->sector_map[i];
    if (map->block_map == 0) {
      free_sectors += 1;
    }

    blocks += __builtin_popcountll(map->block_map);
  }
  auto a = free_sectors == curempty->sectors_free;
  auto b = blocks == curempty->blocks_used;
  if (!a || !b) {
    printchunk(curempty, true);
    printf("%d\n", line);
    printf("%d\n", flag);
    assert(false);
  }
}

static int
getFreeChunk(struct chunkallocator* ca, struct chunk** cl, uint32_t txn_size)
{
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
    assert(ca->next_chunk[i]->used == 0);
  }

  if ((*cl)->used != 0) {
    printchunk(*cl);
    assert(false);
  }

  (*cl)->used = CURRENTLY_USED;
  (*cl)->txn_size = txn_size;
  (*cl)->max_sectors = (*cl)->ptr.size / txn_size;
  (*cl)->sectors_free = (*cl)->max_sectors;
  ca->used_chunks++;

  ca->allocator_lock.unlock();
  return (0);
}

static void*
chunk_collect(void* arg)
{
  struct chunkallocator* ca = (struct chunkallocator*)arg;
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
ca_init(struct chunkallocator* ca,
        off_t starting_offset,
        size_t disksize,
        int txn_size_in_blocks)
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
  ca->emergency_move = 0;
  ca->free = (chunks * CHUNKSIZE) / BLOCKSIZE;

  // These chunk lists act as a bitmap for chunks on the disk.
  ca->chunks = (struct chunk*)malloc(
    sizeof(struct chunk) * chunks, M_CHUNKALLOC, M_WAITOK);
  ca->chunks_candidates = (struct chunk**)malloc(
    sizeof(struct chunk*) * ca->candidate_cnt, M_CHUNKALLOC, M_WAITOK);
  ca->chunks_candidates_old = (struct chunk**)malloc(
    sizeof(struct chunk*) * ca->candidate_cnt, M_CHUNKALLOC, M_WAITOK);

  ca->old_chunks = (struct chunk**)malloc(
    sizeof(struct chunk*) * chunks, M_CHUNKALLOC, M_WAITOK);
  ca->next_chunk = (struct chunk**)malloc(
    sizeof(struct chunk*) * chunks, M_CHUNKALLOC, M_WAITOK);
  memset(ca->thread_worklist, 0, sizeof(struct workset) * 64);
  ca->old_cnt = 0;

  memset(ca->chunks_candidates, 0, sizeof(struct chunk*) * ca->candidate_cnt);
  memset(ca->thread_worklist, 0, sizeof(struct chunk*) * MAXTHREADS);

  // Init our chunks
  for (uint32_t i = 0; i < ca->num_chunks; i++) {
    diskptr_t ptr;
    ptr.offset = starting_offset + (i * (CHUNKSIZE / BLOCKSIZE));
    ptr.size = CHUNKSIZE / BLOCKSIZE;
    struct chunk* l = &ca->chunks[i];
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

void
teardown_thread(struct chunkallocator* ca)
{
  pthread_mutex_lock(&mtx);
  ca->terminate_thread = 1;
  pthread_cond_signal(&cond);
  pthread_mutex_unlock(&mtx);
  pthread_join(ca->tid, NULL);
}

int
ca_destroy(struct chunkallocator* ca)
{
  teardown_thread(ca);
  free(ca->chunks, M_CHUNKALLOC);
  free(ca->next_chunk, M_CHUNKALLOC);
  free(ca->chunks_candidates, M_CHUNKALLOC);
  free(ca->chunks_candidates_old, M_CHUNKALLOC);
  return 0;
}

static void
append_old_chunk(struct chunkallocator* ca, struct chunk* entry)
{
  assert(entry->used == FULL);
  ca->allocator_lock.lock();
  ca->old_chunks[ca->old_cnt] = entry;
  assert(entry->used == FULL);
  ca->old_cnt++;
  ca->allocator_lock.unlock();
}

struct allocatorstats
ca_stat(struct chunkallocator* ca)
{
  struct allocatorstats stats;
  int free = 0;
  uint64_t used_blocks = 0;
  for (uint32_t i = 0; i < ca->num_chunks; i++) {
    struct chunk* cl = &ca->chunks[i];
    uint64_t blocks = 0;
    uint64_t sector_check = 0;
    for (uint32_t t = 0; t < cl->max_sectors; t++) {
      blocks += __builtin_popcountll(cl->sector_map[t].block_map);
      if (cl->sector_map[t].block_map == 0) {
        sector_check++;
      }
    }

    assert(sector_check == cl->sectors_free);

    if (!cl->used) {
      free += 1;
      if (blocks != 0) {
        for (uint32_t t = 0; t < cl->max_sectors; t++) {
          int blockey = __builtin_popcountll(cl->sector_map[t].block_map);
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
  stats.list[7] = ca->emergency_move;
  stats.list[8] = ca->free;
  stats.numStats = 9;
  if (used_blocks !=
      ((((ca->num_chunks * CHUNKSIZE) / BLOCKSIZE)) - ca->free)) {
    for (uint32_t i = 0; i < ca->num_chunks; i++) {
      struct chunk* cl = &ca->chunks[i];
      if (cl->used != FULL)
        printchunk(cl);
    }
    printf("CANDIDATES\n");
    for (uint32_t i = 0; i < ca->candidate_cnt; i++) {
      printchunk(ca->chunks_candidates[i]);
    }

    printf("OLD\n");
    for (uint32_t i = 0; i < ca->candidate_cnt; i++) {
      printchunk(ca->chunks_candidates_old[i]);
    }
    printf("%lu %lu %lu %lu %lu\n",
           free,
           stats.list[1],
           stats.list[7],
           stats.list[8],
           used_blocks);
    assert(false);
  }
  return stats;
}

static int
allocate_from_chunk(struct chunkallocator* ca,
                    chunk* chunk,
                    struct transaction* txns,
                    uint32_t numblocks)
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
      assert(numblocks <= chunk->txn_size);
      chunk->sector_map[i].block_map = SETALL(numblocks);
      ptr.size = numblocks;

      if (__builtin_popcountll(chunk->sector_map[i].block_map) != numblocks) {
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

static void
free_chunk(struct chunkallocator* ca, struct chunk* cl)
{
  ca->allocator_lock.lock();
  for (int i = 0; i < cl->max_sectors; i++) {
    assert(cl->sector_map[i].block_map == 0ULL);
  }
  assert(cl->sectors_free == cl->max_sectors);
  assert(cl->blocks_used == 0);
  cl->used = 0;
  cl->freed += 1;
  ca->next_chunk[ca->next_cnt] = cl;
  ca->next_cnt++;
  cl->mtx.unlock();
  ca->allocator_lock.unlock();
}

#define BREAKPOINT (MAXTHREADS)
// Every thread has access to their own "binary_allocator" buckets of work to
// do. This ensures every thread does an equivalent amount of work on a given
// allocation. For example if you do an allocation of 6 pages, you will do work
// on the 3rd bucket (2^3), and move old data in those buckets. If no data for
// that bucket type is found then it just grabs the oldest data on the list
// (index 0).
static struct chunklist*
ensure_workset(struct chunkallocator* ca,
               int thread_id,
               int numblocks,
               bool high_pressure = false)
{
  // First we check to see if we have a worklist already!
  int bucket = determine_bucket(numblocks);
  struct chunklist* cl = &ca->thread_worklist[thread_id].buckets[bucket];
  if (cl->chunk != NULL &&
      (cl->chunk->sectors_free == cl->chunk->max_sectors)) {
    cl->chunk->mtx.lock();
    free_chunk(ca, cl->chunk);
    cl->chunk = NULL;
  }

  if (cl->chunk == NULL) {
    // We need to get some work to do but we also need to ensure
    // That enough logical time has passed. Meaning Some amount of blocks
    // Have been appended to the old queue
    ca->allocator_lock.lock();
    if (ca->next_cnt > (ca->num_chunks >> 2)) {
      ca->allocator_lock.unlock();
      return NULL;
    }
    int i = 0;
    uint64_t min = UINT64_MAX;
    int cur = 0;
    // This is terrible code need to clean it up
    for (i = 0; i < ca->old_cnt; i++) {
      // This is completely free put it on the next queue
      if (ca->old_chunks[i]->used == 0) {
        assert(ca->old_chunks[i]->used == 0);
        ca->old_chunks[i]->freed += 1;
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

      bool rightTxn =
        ca->old_chunks[i]->txn_size == (1 << bucket) || high_pressure;
      if ((ca->old_chunks[i]->blocks_used) < min && rightTxn) {
        min = ca->old_chunks[i]->blocks_used;
        cur = i;
      }
    }

    if (ca->old_cnt == 0) {
      return NULL;
    }
    // There is some work that we should pop it off the list
    cl->chunk = ca->old_chunks[cur];
    for (int i = cur; i < ca->old_cnt; i++) {
      ca->old_chunks[i] = ca->old_chunks[i + 1];
    }
    ca->old_cnt--;
    ca->allocator_lock.unlock();
  }
  return cl;
}

static void
move_data(struct chunkallocator* ca,
          int thread_id,
          uint32_t numblocks,
          bool high_pressure = false)
{
  struct chunklist* curempty = NULL;
  struct transaction writeset[64];
  uint32_t writeset_cnt = 0;
try_again:
  curempty = ensure_workset(ca, thread_id, numblocks, high_pressure);
  if (!curempty) {
    assert(ca->next_cnt > 0);
    return;
  }
  struct chunk* chunk = curempty->chunk;

  chunk->mtx.lock();
  chunk->used = EMPTYING;

  // We now have a candidate to move
  // We must create a transaction that will free a given sector
#ifdef MAX_BLOCKS_TO_MOVE
  uint32_t max_write_set = MAX_BLOCKS_TO_MOVE;
#else
  uint32_t max_write_set = numblocks;
  // We are getting low, starting ramping up the moves
#endif
  for (int s = 0; s < chunk->max_sectors; s++) {
    struct sector* map = &chunk->sector_map[s];
    if (map->block_map == 0) {
      continue;
    }

    if (writeset_cnt == max_write_set) {
      break;
    }

    for (int i = 0; i < chunk->txn_size; i++) {
      // We found a write that can be added to the writeset
      if (map->block_map & (1ULL << i)) {
        // Get our pointer out
        writeset[writeset_cnt].ptr.offset =
          chunk->ptr.offset + (s * chunk->txn_size) + i;
        writeset[writeset_cnt].ptr.size = 1;
        writeset[writeset_cnt].inode = map->objects[i].inode;
        writeset[writeset_cnt].offset = map->objects[i].offset;
        writeset_cnt++;
      }

      if (writeset_cnt == max_write_set) {
        break;
      }
    }
  }

done:
  if (chunk != NULL)
    chunk->mtx.unlock();

  assert(txn_func);
  if (writeset_cnt > max_write_set) {
    printf("%d %d\n", writeset_cnt, max_write_set);
    assert(false);
  }
  txn_func(writeset, writeset_cnt);
  ca->moved += writeset_cnt;
  writeset_cnt = 0;

  for (int i = 0; i < writeset_cnt; i++) {
    ca_free(ca, writeset[i].ptr);
  }
}

static int
high_pressure_alloc(struct chunkallocator* ca,
                    struct transaction* txns,
                    uint32_t numblocks,
                    int flag)
{
  int error;
start_again:
  if (!flag) {
    move_data(ca, 0, 32);
    while (ca->next_cnt < 32) {
      move_data(ca, 0, 32);
      ca->emergency_move += 32;
    }
    return 0;
  }
  uint32_t blocks = 0;
  for (int i = 0; i < ca->num_chunks; i++) {
    struct chunk* cl = &ca->chunks[i];
    cl->mtx.lock();
    if ((cl->txn_size >= numblocks) && (cl->sectors_free > 0) &&
        cl->used != EMPTYING) {

      error = allocate_from_chunk(ca, cl, txns, numblocks);
      ca->allocations_from_chunk++;
      ca->free -= numblocks;
      cl->mtx.unlock();
      if (error != 0) {
        printchunk(cl, true);
        fflush(stdout);
        assert(false);
      }
      return (0);
    }
    cl->mtx.unlock();
  }
}

int
ca_alloc(struct chunkallocator* ca,
         struct transaction* txns,
         uint32_t numblocks,
         int flag)
{
  int error;
  int bucket;
  int allocated = false;
  int high_pressure = false;
  bucket = determine_bucket(numblocks);
  struct chunk* cl = NULL;
ca_alloc_start:
  if (!flag) {
    move_data(ca, 0, 1 << bucket);
    while ((ca->next_cnt < 4)) {
      move_data(ca, 0, 32, true);
      ca->emergency_move += 32;
    }
  }
  ca->allocator_lock.lock();
  if (flag && enable_old_chunks) {
    cl = ca->chunks_candidates_old[bucket];
  } else {
    cl = ca->chunks_candidates[bucket];
  }
  cl->mtx.lock();
  ca->allocator_lock.unlock();

  if (cl->used != FULL) {
    error = allocate_from_chunk(ca, cl, txns, numblocks);
    if (!error) {
      ca->allocations_from_chunk++;
      ca->free -= numblocks;
      cl->mtx.unlock();

      assert(cl->used != FULL);
      return (0);
    }
  }

  struct chunk* newbucket;
  cl->used = FULL;
  // We could not allocate from a current chunk. So we need to acquire a new one
  error = getFreeChunk(ca, &newbucket, 1ULL << bucket);
  if (!error) {
    assertchunk(cl, 636);
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
    assertchunk(cl, 661);
    // We now unlock. Any thread waiting in the allocation will fail.
    // Require the lock to try and refill it, see it marked as FULL and exit.
    assert(newbucket->used != FULL);
    if (allocated)
      return (0);

    goto ca_alloc_start;
  }

  cl->mtx.unlock();
  high_pressure = true;
  goto ca_alloc_start;
}

int
ca_free(struct chunkallocator* ca, diskptr_t ptr)
{
  assert(ptr.size != (uint32_t)(-1));
  int chunk = ptr.offset / (CHUNKSIZE / BLOCKSIZE);
  if (chunk > ca->num_chunks) {
    printf("%u %u\n", ptr.offset, ptr.size);
    assert(false);
  }

  struct chunk* l = &ca->chunks[chunk];
  l->mtx.lock();
  int sector_i = (ptr.offset - l->ptr.offset) / l->txn_size;
  int bitmap_i = (ptr.offset - l->ptr.offset) % l->txn_size;
  assert((sector_i * l->txn_size) < (l->ptr.offset + l->ptr.size));
  assert((bitmap_i + ptr.size) <= l->txn_size);
  // We freed this with a move, this wont actually happen with pre-allocated
  // wals for now lets just return
  assert(l->sector_map[sector_i].block_map != 0);

  assert(ptr.size == 1);
  if (l->sector_map[sector_i].block_map & (1ULL << bitmap_i)) {
    l->sector_map[sector_i].block_map &= UNSETMASK(bitmap_i);
    l->blocks_used -= 1;
    ca->free += 1;
  }

  // Recheck if its zero
  if (l->sector_map[sector_i].block_map == 0) {
    l->sectors_free++;
    assert(l->sectors_free <= l->max_sectors);
  }

  assertchunk(l, 712);
  l->mtx.unlock();
  return 0;
}
