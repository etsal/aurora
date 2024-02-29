#ifndef _OBJSNAP_H_
#define _OBJSNAP_H_

#include <sys/types.h>
#include "objsnap_ioctl.h"

#ifdef __cplusplus
extern "C" {
#endif

int objsnap_init(char *path);
index_t objsnap_create();
int objsnap_dirty(index_t fd, int tid, void *ptr);
int objsnap_stat(index_t fd, osinode_t *inode);
int objsnap_checkpoint(index_t *fds, int size);
int objsnap_systemstats(statblock *stats, int *cnt);

#ifdef __cplusplus
}
#endif

#endif
