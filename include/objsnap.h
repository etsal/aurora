#ifndef _OBJSNAP_H_
#define _OBJSNAP_H_

#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

int objsnap_init(char *path);
int objsnap_create();
int objsnap_dirty(int fd, off_t index);
int objsnap_checkpoint(int *fds);

#ifdef __cplusplus
}
#endif

#endif
