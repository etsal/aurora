#ifndef _BUF_H
#define _BUF_H
/*
 * Buf.h
 *
 * Main purpose of this is to simulate the buffer cache API within FreeBSD
 * as to be able to test tree data structures that use its API in the kernel
 * but in userspace
 */
#include <shared_mutex>
#include <sys/types.h>

#define DPTR_COW (0x1)
#define DPTR_RDX_LEAF (0x2)
#define DPTR_RDX_INNER (0x4)
#define DPTR_DATA (0x8)

#define LK_EXCLUSIVE (1)
#define LK_SHARED (2)

const uint64_t PBLKSZ = 4 * 1024;

/* On disk pointer */
typedef struct diskptr
{
  uint64_t offset;
  uint64_t size;
  uint64_t epoch;
  uint16_t flags;
} diskptr_t;

/* Main buf struct */
struct buf
{
  void* bp_data;
  size_t bp_lblkno;
  std::shared_mutex bp_lk;
};

struct buf*
getblk(uint64_t blkno, size_t size, int lk_flags);
void
buf_lock(struct buf* bp, int flags);
void
buf_unlock(struct buf* bp, int flags);
void
bdirty(struct buf* bp);
void
bawrite(struct buf* bp);
void
bclean(struct buf* bp);
struct buf**
get_dirty_set(size_t* size);

void
reset_buf_cache();
void
reset_lock_nums();
bool
check_locks();
void
locks_print();

diskptr_t
allocate_blk(size_t size);

void
print_buf_stats();

#endif
