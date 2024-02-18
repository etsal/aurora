#include <sys/param.h>
#include <sys/systm.h>
#include <sys/bitstring.h>
#include <sys/capsicum.h>
#include <sys/conf.h>
#include <sys/file.h>
#include <sys/kernel.h>
#include <sys/kthread.h>
#include <sys/limits.h>
#include <sys/lock.h>
#include <sys/malloc.h>
#include <sys/md5.h>
#include <sys/module.h>
#include <sys/mutex.h>
#include <sys/protosw.h>
#include <sys/queue.h>
#include <sys/rwlock.h>
#include <sys/sbuf.h>
#include <sys/stat.h>
#include <sys/sx.h>
#include <sys/sysctl.h>
#include <sys/time.h>
#include <sys/uio.h>
#include <sys/vnode.h>
#include <sys/wait.h>

#include <vm/vm_page.h>
#include <vm/vm_param.h>

#include "objsnap_internal.h"
#include "objsnap_ioctl.h"

/* XXX Rename to M_SLS. */
MALLOC_DEFINE(M_OBJSNAP, "objsnap", "objsnap");

struct objsnap_metadata osdata;

static int
objsnap_sysctl_init(void)
{
    //struct sysctl_oid *root;
	return (0);
}

static void
sls_sysctl_fini(void)
{
}

static void
objsnap_checkpoint(struct objsnap_checkpoint_args *args)
{
	return;
}

static void
objsnap_create(struct objsnap_create_args *args)
{
	return;
}

static void
objsnap_dirty_page(struct objsnap_dirty_page_args *args)
{
	return;
}

static int
objsnap_ioctl(struct cdev *dev, u_long cmd, caddr_t data, int flag __unused,
    struct thread *td)
{
	switch (cmd) {

	case OBJSNAP_CHECKPOINT:
		objsnap_checkpoint((struct objsnap_checkpoint_args *)data);
		break;

	case OBJSNAP_CREATEOBJ:
		objsnap_create((struct objsnap_create_args *)data);
		break;

	case OBJSNAP_DIRTYPAGE:
		objsnap_dirty_page((struct objsnap_dirty_page_args *)data);
		break;
	}
	return (0);
}

static struct cdevsw objsnap_cdevsw = {
	.d_version = D_VERSION,
	.d_ioctl = objsnap_ioctl,
};

static int
objsnapHandler(struct module *inModule, int inEvent, void *inArg)
{
	int error = 0;

	switch (inEvent) {
	case MOD_LOAD:
		/* Make the SLS available to userspace. */
		osdata.slsm_cdev = make_dev(
		    &objsnap_cdevsw, 0, UID_ROOT, GID_WHEEL, 0666, "sls");

		break;

	case MOD_UNLOAD:
    break;
	default:
		error = EOPNOTSUPP;
		break;
	}

	return (error);
}

static moduledata_t moduleData = { "objsnap", objsnapHandler, NULL };

DECLARE_MODULE(objsnap, moduleData, SI_SUB_DRIVERS, SI_ORDER_MIDDLE);
MODULE_VERSION(objsnap, 0);
