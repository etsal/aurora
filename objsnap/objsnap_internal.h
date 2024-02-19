#ifndef _OBJSNAP_INTERNAL_H_
#define _OBJSNAP_INTERNAL_H_

#include <sys/param.h>
#include <sys/bitstring.h>
#include <sys/condvar.h>
#include <sys/fcntl.h>
#include <sys/file.h>
#include <sys/filedesc.h>
#include <sys/lock.h>
#include <sys/mutex.h>
#include <sys/proc.h>
#include <sys/queue.h>
#include <sys/sdt.h>
#include <sys/stat.h>
#include <sys/syscallsubr.h>
#include <sys/sysctl.h>
#include <sys/vnode.h>

#include <vm/vm.h>
#include <vm/uma.h>
#include <vm/vm_object.h>

#include "objsnap_ioctl.h"

struct objsnap_metadata {
	struct cdev *os_cdev;	/* The cdev that exposes the SLS ops */
	struct vnode *os_vp;
	struct g_consumer *os_consumer;
};

struct allocator {
  size_t alloc_size_total_blocks;
  size_t alloc_block_size;
  size_t alloc_next_block;
};

extern struct allocator alloc;
extern super_t superblock;

MALLOC_DECLARE(M_OBJSNAP);

#endif
