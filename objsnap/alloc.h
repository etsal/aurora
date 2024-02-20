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

diskptr_t allocate_block(void);

osinode_t *allocate_inode(void);

#endif