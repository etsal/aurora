#include "buf.h"
#include "options.h"

#include "pthread.h"

#include <cassert>
#include <chrono>
#include <list>
#include <set>
#include <thread>
#include <unordered_map>

std::mutex buffer_cache_lk;
std::unordered_map<uint64_t, struct buf*> buffer_cache;

std::mutex dirty_lk;
std::set<struct buf*> dirty_set;

std::atomic<uint64_t> pblkno;

static int acquires = 0;
static int releases = 0;

void
reset_lock_nums()
{
  acquires = 0;
  releases = 0;
}

void
free_buffer(struct buf* bp)
{
  free(bp->bp_data);
  delete bp;
}

void
reset_buf_cache()
{
  for (auto it : buffer_cache) {
    free_buffer(it.second);
  }

  buffer_cache.erase(buffer_cache.begin(), buffer_cache.end());
  pblkno = 0;
}

void
locks_print()
{
  printf("A (%d), R(%d)\n", acquires, releases);
}

bool
check_locks()
{
  return acquires == releases;
}

void
sleep_ns(unsigned long ns)
{
  auto start_time = std::chrono::high_resolution_clock::now();
  std::this_thread::sleep_for(std::chrono::nanoseconds(ns));
  while (std::chrono::duration_cast<std::chrono::nanoseconds>(
           std::chrono::high_resolution_clock::now() - start_time)
           .count() < ns) {
  }
}

/*
 * LRUCache
 *
 * This cache serves to induce disk latency when required for buffers
 * that are not longer in its hot set. To reduce this time, increase
 * throughput of the simulated SSD (THROUHPUT)
 */
class LRUCache
{
public:
  LRUCache()
    : m_capacity(LRU_CAPACITY)
  {
  }
  int access(int key)
  {
    auto it = m_cache.find(key);
    if (it != m_cache.end()) {
      m_list.splice(m_list.begin(), m_list, it->second);
      hits.fetch_add(1);
      return 0;
    }

    if (m_cache.size() == m_capacity) {
      int key_to_remove = m_list.back();
      m_list.pop_back();
      m_cache.erase(key_to_remove);
    }

    m_list.emplace_front(key);
    m_cache[key] = m_list.begin();
    misses.fetch_add(1);
    return 1;
  }

  std::atomic<uint64_t> hits;
  std::atomic<uint64_t> misses;

private:
  int m_capacity;
  std::list<uint64_t> m_list;
  std::unordered_map<int, std::list<uint64_t>::iterator> m_cache;
};

static LRUCache lru;

void
print_buf_stats()
{
  auto h = lru.hits.load();
  auto m = lru.misses.load();
  printf("LRU Stats\n");
  printf("=========\n");
  printf("Hits: %lu\n", h);
  printf("Misses: %lu\n", m);
  auto percentage = (int)(((double)h / (double)(h + m)) * 100);
  printf("Percentage: %d\n", percentage);
}

struct buf**
get_dirty_set(size_t* size)
{
  *size = dirty_set.size();
  struct buf** ds =
    (struct buf**)malloc(sizeof(struct buf*) * dirty_set.size());
  int i = 0;
  for (const auto bp : dirty_set) {
    ds[i] = bp;
    i += 1;
  }

  return ds;
}

static struct buf*
create_buf(uint64_t lblkno, size_t size)
{
  struct buf* bp = new buf{};
  bp->bp_data = malloc(size);
  bzero(bp->bp_data, size);
  bp->bp_lblkno = lblkno;

  return bp;
}

struct buf*
getblk(uint64_t lblkno, size_t size, int lk_flags)
{
  std::lock_guard<std::mutex> guard(buffer_cache_lk);
  /* Check if we miss on the cache */
#ifdef DISK_LATENCY
  if (lru.access(lblkno)) {
    double sleeptime = ((double)size) / THROUGHPUT;
    sleeptime = sleeptime * NS;
    sleep_ns(sleeptime);
  }
#endif

  auto iter = buffer_cache.find(lblkno);
  if (iter == buffer_cache.end()) {
    struct buf* bp = create_buf(lblkno, size);
    buffer_cache.insert({ lblkno, bp });
    buf_lock(bp, lk_flags);

    return bp;
  }

  buf_lock(iter->second, lk_flags);
  return iter->second;
}

void
buf_lock(struct buf* bp, int flags)
{
  if (flags == LK_EXCLUSIVE) {
    acquires += 1;
    bp->bp_lk.lock();
    return;
  }

  if (flags == LK_SHARED) {
    acquires += 1;
    bp->bp_lk.lock_shared();
    return;
  }
}

void
buf_unlock(struct buf* bp, int flags)
{
  if (flags == LK_EXCLUSIVE) {
    releases += 1;
    bp->bp_lk.unlock();
  }

  if (flags == LK_SHARED) {
    releases += 1;
    bp->bp_lk.unlock_shared();
  }
}

void
bdirty(struct buf* bp)
{
  std::lock_guard<std::mutex> guard(dirty_lk);
  dirty_set.insert(bp);
}

void
bawrite(struct buf* bp)
{
  std::lock_guard<std::mutex> guard(dirty_lk);
  auto it = dirty_set.find(bp);
  if (it != dirty_set.end())
    dirty_set.erase(it);
}

void
bclean(struct buf* bp)
{
  bawrite(bp);
}

diskptr_t
allocate_blk(size_t size)
{
  diskptr_t ptr;
  size = ((size + (PBLKSZ - 1)) / PBLKSZ);
  ptr.size = size;
  auto off = pblkno.fetch_add(size);
  ptr.offset = off;
  ptr.epoch = 0;
  return ptr;
}
