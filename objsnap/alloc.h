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
#include "objsnap_internal.h"
#include "objsnap_ioctl.h"

#define ENTRIES_PER_GIB ((1024UL * 1024UL * 1024UL) / 4096UL)
#define MAX_WAL_ENTRIES (10UL * ENTRIES_PER_GIB)

struct allocator {
  size_t alloc_size_total_blocks;
  size_t alloc_bsize;
  struct lock alloc_lk;
  struct binaryallocator alloc_impl;

  volatile size_t alloc_walptr_head;
  volatile size_t alloc_walptr_tail;
  volatile size_t alloc_base;
};

void allocator_init(void);
void allocator_destroy(void);
    
int write_ondisk_inode(osinode_t *inode);
diskptr_t allocate_block(int num);
void free_block(diskptr_t ptr);
diskptr_t objsnap_blkalloc_wal(void);
osinode_t *allocate_inode(void);

#endif
