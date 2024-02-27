#ifndef __OBJSNAP_ALLOC_H_
#define __OBJSNAP_ALLOC_H_

#include "objsnap_internal.h"
#include "objsnap_ioctl.h"

struct allocator {
  size_t alloc_size_total_blocks;
  size_t alloc_bsize;
  volatile size_t alloc_next_block;
};

void allocator_init(void);
int write_ondisk_inode(osinode_t *inode);
int flush(void);
diskptr_t allocate_block(int num);
osinode_t *allocate_inode(void);

#endif