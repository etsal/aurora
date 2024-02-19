#ifndef _OBJSNAP_H_
#define _OBJSNAP_H_

#include <sys/types.h>
#include "objsnap_ioctl.h"

#ifdef __cplusplus
extern "C" {
#endif

int objsnap_init(char *path);
index_t objsnap_create();
int objsnap_dirty(int fd, off_t index);
int objsnap_checkpoint(int *fds);

#ifdef __cplusplus
}
#endif

#endif
