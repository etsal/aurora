#ifndef _OBJSNAP_INTERNAL_H_
#define _OBJSNAP_INTERNAL_H_
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
#include <sys/sysctl.h>

#include <vm/vm.h>
#include <vm/uma.h>
#include <vm/vm_object.h>

#include "objsnap_common.h"
#include "objsnap_ioctl.h"
#include "arraylist.h"
#include "vtree.h"

#define OBJMAGIC (0xdeadbeef)

#define LOCK(lock, type) (lockmgr(lock, type, NULL))
#define UNLOCK(lock) (lockmgr(lock, LK_RELEASE, NULL))
#define LOCK_SUPER() (LOCK(&osdata.os_lock, LK_EXCLUSIVE))
#define UNLOCK_SUPER() (UNLOCK(&osdata.os_lock))
#define INDEX_TO_VNODE(i) (&vnode_cache[(i) / 2])
#define DEVICE_BLOCK_NUM(blki) ((uint64_t)(((uint64_t)blki) * 8UL))

#define OS_STAT_DEFINE(name, num) \
	static int OS_STAT_##name = num; \
	static char *OS_STAT_NAME_##name = #name; \
	static inline struct cycletimer *OS_STAT_GET_##name(void) { \
		return &osdata.os_stats[OS_STAT_##name]; \
	} \
	static inline __attribute__((always_inline)) struct timerstat OS_TOSTAT_##name() { \
		return ctstat(#name, OS_STAT_GET_##name()); \
	}

#define OS_START(name, before) do { ctstart(OS_STAT_GET_##name(), before); } while(0)
#define OS_STOP(name, before) do {ctstop(OS_STAT_GET_##name(), before); } while(0)
#define OS_STOP_SAMPLE(name, before, sample) do { if ((*before % sample) == 0) ctstop(OS_STAT_GET_##name(), before); } while(0)

SYSCTL_DECL(_objsnap);

enum objsync_state {
	OBJSYNC_UNINIT = 0,
	OBJSYNC_RUNNING,
	OBJSYNC_EXITING,
	OBJSYNC_EXITED,
};

struct objsnap_metadata {
	struct cdev *os_cdev;	/* The cdev that exposes the SLS ops */
	struct vnode *os_vp;
	struct taskqueue *os_tq;
	struct g_consumer *os_consumer;
	struct g_provider *os_provider;
	struct lock os_lock;
	struct cycletimer os_stats[32];

	struct mtx os_syncer_lk;
	struct thread *os_syncertd;
	int os_syncer_wakeup;
	volatile enum objsync_state os_syncer_exit;
};

struct __attribute__((packed)) walptr {
	index_t w_inode; // Object being modified
	index_t	w_index;  // Index into the object the modification occurs
	index_t w_offset; // Ptr to the data holding the modified page
};

enum VSTATE {
	VNULL = 0,
	VALID = 1,
};

struct objsnap_vnode {
	osinode_t *v_inode;
	uint64_t v_magic;
	struct virtualtree v_tree;
	struct lock v_lock;
	struct lock v_commit_lock;
	enum VSTATE v_state;
};

#define MAXPOWEROFTWO (31)
static inline int 
determine_bucket(int numblocks)
{
    int i = 1;
    int shift;
    for (shift = 0; shift <= MAXPOWEROFTWO; shift++) {
        if (numblocks <= (i << shift)) {
            return (shift);
        }
    }

    panic("Bucket could not be determined %d", numblocks);
}

extern struct objsnap_metadata osdata;
extern struct allocator alloc;
extern super_t superblock;
extern struct objsnap_vnode *vnode_cache;

OS_STAT_DEFINE(LOCKANDCOPY, 0);
OS_STAT_DEFINE(DATAWRITE, 1);
OS_STAT_DEFINE(UNLOCK, 2);
OS_STAT_DEFINE(VNFAULTMOVE, 3);
OS_STAT_DEFINE(INODE, 4);
OS_STAT_DEFINE(BTFIND, 5);
OS_STAT_DEFINE(BTCOW, 6);
OS_STAT_DEFINE(BTINSERT, 7);
OS_STAT_DEFINE(CHECKPOINT, 8);
OS_STAT_DEFINE(ALLOCATE, 9);
OS_STAT_DEFINE(GETBLK, 10);
OS_STAT_DEFINE(WAITERS, 11);
OS_STAT_DEFINE(WRITERS, 12);
OS_STAT_DEFINE(DIRTY, 13);
#define OS_STAT_LAST (14)

#define STAT_TO_ARGS(args, name) ((args)->os_stats[OS_STAT_##name]) = OS_TOSTAT_##name()

extern vm_page_t hackpage;
void objsnap_printstats(void);
bool objsnap_wal_search(uint32_t offset);

extern uint8_t *objbit;

SDT_PROVIDER_DECLARE(objsnap);

#endif
