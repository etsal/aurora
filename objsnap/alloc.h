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

#include "chunkalloc.h"
#include "objsnap_internal.h"
#include "objsnap_ioctl.h"

#define MAX_WAL_ENTRIES (1024)

struct allocator {
  size_t alloc_size_total_blocks;
  size_t alloc_bsize;
  struct mtx alloc_lk;
  struct chunkallocator alloc_impl;

  volatile size_t alloc_walptr_head;
  volatile size_t alloc_walptr_tail;
  volatile size_t alloc_base;
};

void allocator_init(void);
void allocator_destroy(void);
    
int write_ondisk_inode(osinode_t *inode);
diskptr_t allocate_block(int num);
void free_block(diskptr_t ptr);
diskptr_t allocate_threadwal(void);
osinode_t *allocate_inode(void);

#endif
