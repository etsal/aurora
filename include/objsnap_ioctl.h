#ifndef _SLS_IOCTL_H_
#define _SLS_IOCTL_H_

#include <sys/ioccom.h>
#include <sys/sbuf.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef uint64_t epoch_t;
typedef uint64_t index_t;

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

#ifdef __cplusplus
}
#endif

#endif /* _SLS_IOCTL_H_ */
