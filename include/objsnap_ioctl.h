#ifndef _SLS_IOCTL_H_
#define _SLS_IOCTL_H_

#include <sys/ioccom.h>
#include <sys/sbuf.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef uint64_t epoch_t;
typedef uint64_t index_t;

typedef struct {
	size_t super_num_inodes;
	index_t super_freelist;
} super_t;

typedef struct {
	uint64_t d_offset;
} diskptr_t;

typedef struct {
	diskptr_t i_ptr;
	epoch_t i_epoch;
	index_t i_index;
} osinode_t;

struct objsnap_init_args {
  char path[256];
};

struct objsnap_checkpoint_args {
  epoch_t os_epoch;
};

struct objsnap_create_args {
  int *os_fd;
  int error;
};

struct objsnap_dirty_page_args {
  int os_fd;
  int os_dirty_i;
};

#define OBJSNAP_CHECKPOINT _IOWR('d', 1, struct objsnap_checkpoint_args)
#define OBJSNAP_CREATEOBJ _IOWR('d', 2, struct objsnap_create_args)
#define OBJSNAP_DIRTYPAGE _IOWR('d', 3, struct objsnap_dirty_page_args)
#define OBJSNAP_INIT _IOWR('d', 4, struct objsnap_init_args)

#ifdef __cplusplus
}
#endif

#endif /* _SLS_IOCTL_H_ */
