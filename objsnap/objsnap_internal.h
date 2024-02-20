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
#include "vtree.h"

#define OBJMAGIC (0xdeadbeef)
#define MAXDRTYCNT (64)

struct objsnap_metadata {
	struct cdev *os_cdev;	/* The cdev that exposes the SLS ops */
	struct vnode *os_vp;
	struct g_consumer *os_consumer;
	struct lock os_lock;
};

struct pageset {
	vm_page_t page;
};

struct objsnap_vnode {
	osinode_t *v_inode;
	uint64_t v_magic;
	struct virtualtree v_tree;
	struct lock v_lock;
	int v_dirtycnt;
	struct pageset v_dirty_pageset[MAXDRTYCNT];
};


extern struct objsnap_metadata osdata;
extern struct allocator alloc;
extern super_t superblock;
extern struct objsnap_vnode *vnode_cache;
#define LOCK(lock, type) (lockmgr(lock, type, NULL))
#define UNLOCK(lock) (lockmgr(lock, LK_RELEASE, NULL))

#define LOCK_SUPER() (LOCK(&osdata.os_lock, LK_EXCLUSIVE))
#define UNLOCK_SUPER() (UNLOCK(&osdata.os_lock))

#define INDEX_TO_VNODE(i) (&vnode_cache[(i) / 2])
#define DEVICE_BLOCK_NUM(blki) ((blki) * (BLOCKSIZE / superblock.super_bsize))


#endif
