#ifndef __OBJSNAP_ALLOC_H_
#define __OBJSNAP_ALLOC_H_

#include <sys/param.h>
#include <sys/bitstring.h>
#include <sys/condvar.h>
#include <sys/fcntl.h>
#include <sys/file.h>
#include <sys/filedesc.h>
#include <sys/lock.h>
#include <sys/mutex.h>


#include "binaryalloc.h"
#include "chunkalloc.h"
#include "objsnap_common.h"
#include "objsnap_internal.h"
#include "objsnap_ioctl.h"

#define ENTRIES_PER_GIB ((1024UL * 1024UL * 1024UL) / 4096UL)
#define MAX_WAL_ENTRIES (10UL * ENTRIES_PER_GIB)

enum obj_alloctype {
	OBJALLOC_BINARY,
	OBJALLOC_CHUNK,
};
extern const enum obj_alloctype obj_alloctype;

union objallocator {
	struct binaryallocator ba;
	struct chunkallocator ca;
};

struct allocator {
  size_t alloc_size_total_blocks;
  size_t alloc_starting_offset;
  size_t alloc_bsize;
  struct lock alloc_lk;
  union objallocator alloc_impl;

  volatile size_t alloc_walptr_head;
  volatile size_t alloc_walptr_tail;
  volatile size_t alloc_base;
};

static inline void
oa_init(union objallocator *oa, uint32_t startoff, size_t numblocks)
{
	switch (obj_alloctype) {
	case OBJALLOC_BINARY:
		ba_init(&oa->ba, startoff, numblocks);
		return;
	case OBJALLOC_CHUNK:
		ca_init(&oa->ca, startoff, numblocks);
		return;
	default:
		panic("invalid allocator type %d\n", obj_alloctype);
	}
}

static inline void
oa_destroy(union objallocator *oa)
{
	switch (obj_alloctype) {
	case OBJALLOC_BINARY:
		ba_destroy(&oa->ba);
		return;
	case OBJALLOC_CHUNK:
		ca_destroy(&oa->ca);
		return;
	default:
		panic("invalid allocator type %d\n", obj_alloctype);
	}
}

static inline int
oa_alloc_txn(union objallocator *oa, int tid, struct objsnap_txn *txn)
{
	switch (obj_alloctype) {
	case OBJALLOC_BINARY:
		return (ba_alloc(&oa->ba, txn->d_cnt, &txn->d_ptr));
	case OBJALLOC_CHUNK:
		return (ca_alloc_txn(&oa->ca, tid, txn));
	default:
		panic("invalid allocator type %d\n", obj_alloctype);
	}
}

static inline int
oa_alloc_system(union objallocator *oa, obj_diskptr_t *ptrp)
{
	switch (obj_alloctype) {
	case OBJALLOC_BINARY:
		/* 
		 * The binary allocator does not distinguish between
		 * data and system block allocations.
		 */
		return (ba_alloc(&oa->ba, 1, ptrp));
	case OBJALLOC_CHUNK:
		return (ca_alloc_system(&oa->ca, ptrp));
	default:
		panic("invalid allocator type %d\n", obj_alloctype);
	}
}


static inline void
oa_free(union objallocator *oa, obj_diskptr_t ptr)
{
	switch (obj_alloctype) {
	case OBJALLOC_BINARY:
		ba_free(&oa->ba, ptr);
		return;
	case OBJALLOC_CHUNK:
		ca_free(&oa->ca, ptr);
		return;
	default:
		panic("invalid allocator type %d\n", obj_alloctype);
	}
}

static inline void
oa_print(union objallocator *oa)
{
	switch (obj_alloctype) {
	case OBJALLOC_BINARY:
		ba_print(&oa->ba);
		return;
	case OBJALLOC_CHUNK:
		ca_print(&oa->ca);
		return;
	default:
		panic("invalid allocator type %d\n", obj_alloctype);
	}
}

static inline void
oa_gc(union objallocator *oa, size_t numblocks)
{
	switch (obj_alloctype) {
	case OBJALLOC_BINARY:
		return;
	case OBJALLOC_CHUNK:
		ca_gc(&oa->ca, numblocks);
		return;
	default:
		panic("invalid allocator type %d\n", obj_alloctype);
	}
}

void allocator_init(void);
void allocator_destroy(void);
    
int write_ondisk_inode(osinode_t *inode);
int allocate_txn_block(struct objsnap_txn *txn, int tid);
int allocate_system_block(obj_diskptr_t *ptr);
void free_block(obj_diskptr_t ptr);
int objsnap_blkalloc_wal(obj_diskptr_t *ptr);
void garbage_collect(size_t numblocks);
osinode_t *allocate_inode(void);

#endif
