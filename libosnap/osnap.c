#include <string.h>
#include <fcntl.h>
#include <errno.h>

#include "objsnap.h"
#include "objsnap_ioctl.h"

#ifdef __cplusplus
extern "C" {
#endif

static char * OS_DEV = "/dev/objsnap";
static int OS_FD = -1;

static int 
objsnap_fd_check()
{
     if (OS_FD == -1) {
        int fd = open(OS_DEV, O_RDWR);
        if (fd < 0) {
            return errno;
        }

        OS_FD = fd;
    }

    return 0;
}

int 
objsnap_init(char *path)
{
    int error = 0;
    struct objsnap_init_args args;

    if ((error = objsnap_fd_check()) != 0) {
        return error;
    }

    strcpy(args.path, path);
    return ioctl(OS_FD, OBJSNAP_INIT, &args);
}

index_t
objsnap_create()
{
    int error = 0;
    struct objsnap_create_args args;

    if ((error = objsnap_fd_check()) != 0) {
        return error;
    }

    error = ioctl(OS_FD, OBJSNAP_CREATEOBJ, &args);
    if (error) {
        return BADINDEX;
    }

    return (args.os_index);
}

int 
objsnap_dirty(int fd, off_t index)
{
    int error = 0;
    if ((error = objsnap_fd_check()) != 0) {
        return error;
    }

    return (0);
}

int 
objsnap_checkpoint(int *fds)
{
    int error = 0;
    if ((error = objsnap_fd_check()) != 0) {
        return error;
    }

    return (0);
}


#ifdef __cplusplus
}
#endif
