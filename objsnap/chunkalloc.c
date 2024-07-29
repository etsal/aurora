#include <sys/param.h>
#include <sys/bitstring.h>
#include <sys/condvar.h>
#include <sys/fcntl.h>
#include <sys/lock.h>
#include <sys/mutex.h>
#include <sys/proc.h>
#include <sys/queue.h>
#include <sys/sdt.h>
#include <sys/stat.h>
#include <sys/syscallsubr.h>
#include <sys/sysctl.h>
#include <sys/vnode.h>
#include <sys/taskqueue.h>

#include "arraylist.h"
#include "chunkalloc.h"
#include "objsnap_ioctl.h"

void
ca_init(struct chunkallocator *ba)
{
}

int
ca_alloc(struct chunkallocator *ba, int numblocks, diskptr_t *ptr)
{
	return (EOPNOTSUPP);
}

void
ca_free(struct chunkallocator *ba, diskptr_t tofree)
{
}

void
ca_destroy(struct chunkallocator *ba)
{
}

void
ca_print(struct chunkallocator *ba)
{
}
