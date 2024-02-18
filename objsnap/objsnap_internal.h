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
	struct cdev *slsm_cdev;		/* The cdev that exposes the SLS ops */
};

typedef struct {
	uint64_t d_offset;
} diskptr_t;

typedef struct {
	diskptr_t i_ptr;
	epoch_t i_epoch;
	index_t i_index;
} osinode_t;

typedef struct {
	size_t super_num_inodes;
	index_t super_freelist;
} super_t;

MALLOC_DECLARE(M_OBJSNAP);

#endif
