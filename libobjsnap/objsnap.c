#include "objsnap.h"

#ifdef __cplusplus
extern "C" {
#endif

int 
objsnap_init(char *path)
{
    return (0);
}
int 
objsnap_create()
{
    return (0);
}

int 
objsnap_dirty(int fd, off_t index)
{
    return (0);
}

int 
objsnap_checkpoint(int *fds)
{
    return (0);
}


#ifdef __cplusplus
}
#endif
