#ifndef _SLS_IOCTL_H_
#define _SLS_IOCTL_H_

#include <sys/ioccom.h>
#include <sys/sbuf.h>

#include "objsnap_rdtsc.h"

#ifdef __cplusplus
extern "C" {
#endif

#define BLOCKSIZE (4096UL)
#define MAXINODES (1024)
#define CKPT_MAXINODES (64)
#define OS_STAT_MAX	(16)

#define NULLDISKPTR ((obj_diskptr_t) -1)
#define OBJINO_BADINDEX ((index_t)(-1))
#define MAXPOWEROFTWO (31)


typedef uint64_t epoch_t;
typedef int index_t;

typedef struct obj_diskptr {
  uint32_t offset;
  uint32_t size;
} obj_diskptr_t;

typedef struct timerstat statblock[OS_STAT_MAX];

typedef struct {
  size_t super_max_inodes;
  size_t super_bsize;
  size_t super_ssize;
  size_t super_size;
  size_t super_asize;
  size_t super_version;
  index_t super_next;
  index_t super_blk;

  index_t super_freelist[];
} super_t;

#define INODE_MAX_BOUND (64)

typedef struct {
	index_t i_index;
  obj_diskptr_t i_treeptr;

  uint64_t i_version;
  
  int16_t i_cnt;
  index_t i_checkpointed_with[INODE_MAX_BOUND];
} osinode_t;

struct objsnap_init_args {
  char path[256];
};

struct objsnap_checkpoint_args {
  int tid;
};

struct objsnap_create_args {
  index_t os_index;
  int error;
};

struct objsnap_dirty_page_args {
  int os_tid;
  index_t os_index;
  uintptr_t os_page;
};

struct objsnap_stat_args {
  index_t os_index; /* IN: Inode to get*/
  osinode_t os_inode; /* OUT: Inode structure */
};

struct objsnap_systemstats_args {
  statblock os_stats;
  int os_cnt;
};

#define OBJSNAP_CHECKPOINT _IOWR('d', 1, struct objsnap_checkpoint_args)
#define OBJSNAP_CREATEOBJ _IOR('d', 2, struct objsnap_create_args)
#define OBJSNAP_DIRTYPAGE _IOW('d', 3, struct objsnap_dirty_page_args)
#define OBJSNAP_INIT _IOW('d', 4, struct objsnap_init_args)


#define OBJSNAP_STAT _IOWR('d', 5, struct objsnap_stat_args)
#define OBJSNAP_SYSTEMSTATS _IOR('d', 6, struct objsnap_systemstats_args)

#ifdef __cplusplus
}
#endif

#endif /* _SLS_IOCTL_H_ */
