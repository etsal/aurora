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

#include "arraylist.h"
#include "objsnap_ioctl.h"


struct binaryallocator {
    struct mtx  ba_lock;
    struct arraylist ba_flists[MAXPOWEROFTWO + 1];
};

void ba_init(struct binaryallocator *ba, uint32_t offset, uint32_t left);
int ba_alloc(struct binaryallocator *ba, int numblocks, diskptr_t *ptr);
void ba_free(struct binaryallocator *ba, diskptr_t tofree);
void ba_destroy(struct binaryallocator *ba);
void ba_print(struct binaryallocator *ba);
#endif
