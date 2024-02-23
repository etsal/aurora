#ifndef _OBJSNAP_H_
#define _OBJSNAP_H_

#include <sys/types.h>
#include "objsnap_ioctl.h"

#ifdef __cplusplus
extern "C" {
#endif

int objsnap_init(char *path);
index_t objsnap_create();
int objsnap_dirty(index_t fd, void *ptr);
int objsnap_stat(index_t fd, osinode_t *inode);
int objsnap_checkpoint(index_t *fds, int size);

#ifdef __cplusplus
}
#endif

#endif
