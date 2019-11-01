#ifndef _SLOS_IO_H_
#define _SLOS_IO_H_

#include <sys/param.h>

#include <sys/file.h>
#include <sys/uio.h>

#include <vm/vm_object.h>

#include "../include/slos.h"
#include "slos_internal.h"

int slos_read(struct vnode *vp, struct slos_diskptr *diskptr, struct uio *uio);
int slos_write(struct vnode *vp, struct slos_diskptr *diskptr, struct uio *uio);

void slos_uioinit(struct uio *auio, uint64_t off, enum uio_rw rwflag, 
	struct iovec *aiovs, size_t iovcnt);
int slos_uiozero(struct uio *auio, size_t len);

int slos_sbread(void);
int slos_sbwrite(struct slos *slos);

int slos_readblk(struct slos *slos, uint64_t blkno, void *buf);
int slos_writeblk(struct slos *slos, uint64_t blkno, void *buf);


#ifdef SLOS_TESTS

int slos_testio_random(void);
int slos_testio_intraseg(void);

#endif /* SLOS_TESTS */

#endif /* _SLOS_IO_H_ */
