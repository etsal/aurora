#ifndef __BINARYALLOC_H__
#define __BINARYALLOC_H__
#include <sys/param.h>
#include <sys/bitstring.h>
#include <sys/condvar.h>
#include <sys/fcntl.h>
#include <sys/lock.h>
#include <sys/mutex.h>
#include <sys/proc.h>
#include <sys/queue.h>
#include <sys/sdt.h>
#include <sys/stat.h>
#include <sys/syscallsubr.h>
#include <sys/sysctl.h>
#include <sys/vnode.h>
#include <sys/taskqueue.h>

#include "objsnap_ioctl.h"

// Remember its the 2^(x) * PAGE_SIZE
// Or rather its how many continguous page blocks are there.
#define INITLISTSIZE (2048)

struct binaryallocator {
    struct mtx  ba_lock;
    struct arraylist ba_flists[MAXPOWEROFTWO + 1];
};

void ba_init(struct binaryallocator *ba, uint32_t offset, uint32_t left);
int ba_alloc(struct binaryallocator *ba, int numblocks, obj_diskptr_t *ptr);
void ba_free(struct binaryallocator *ba, obj_diskptr_t tofree);
void ba_destroy(struct binaryallocator *ba);
void ba_print(struct binaryallocator *ba);
#endif
