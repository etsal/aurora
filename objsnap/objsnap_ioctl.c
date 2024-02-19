#include <sys/param.h>
#include <sys/systm.h>
#include <sys/bio.h>
#include <sys/buf.h>
#include <sys/conf.h>
#include <sys/dirent.h>
#include <sys/extattr.h>
#include <sys/fcntl.h>
#include <sys/kernel.h>
#include <sys/kthread.h>
#include <sys/lockf.h>
#include <sys/module.h>
#include <sys/mount.h>
#include <sys/mutex.h>
#include <sys/namei.h>
#include <sys/pctrie.h>
#include <sys/priv.h>
#include <sys/proc.h>
#include <sys/rwlock.h>
#include <sys/stat.h>
#include <sys/syscallsubr.h>
#include <sys/sysctl.h>
#include <sys/taskqueue.h>
#include <sys/uio.h>
#include <sys/unistd.h>
#include <sys/vnode.h>

#include <vm/vm.h>
#include <vm/vm_extern.h>
#include <vm/vm_page.h>

#include <machine/param.h>
#include <machine/vmparam.h>

#include <geom/geom.h>
#include <geom/geom_vfs.h>


#include "objsnap_internal.h"
#include "objsnap_ioctl.h"
#include "alloc.h"

/* XXX Rename to M_SLS. */
MALLOC_DEFINE(M_OBJSNAP, "objsnap", "objsnap");

struct objsnap_metadata osdata;
super_t superblock;
struct objsnap_vnode *vnode_cache = NULL;

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
	int error;
	osinode_t inode;

	if ((error = allocate_inode(&inode)) == BADINDEX) {
		printf("Issue creating inode\n");
	}

	args->os_index = inode.i_index;

	return;
}

static void
objsnap_dirty_page(struct objsnap_dirty_page_args *args)
{
	return;
}

static int
superblock_init(struct vnode *vp)
{
	struct buf *bp = NULL;
	int error;

	if ((error = bread(vp, 0, BLOCKSIZE, NOCRED, &bp)) != 0) {
		return error;
	}

	memcpy(&superblock, bp->b_data, sizeof(super_t));
	brelse(bp);
	printf("Size of disk %zu\n", superblock.super_size);	
	return (0);
};

static void
objsnap_init(struct objsnap_init_args *args)
{
	struct nameidata nd;
	struct vnode *vp;

	int error = 0;
	char *path = args->path;


	// Take path and convert to a device vnode.
	NDINIT(&nd, LOOKUP, FOLLOW | LOCKLEAF, UIO_SYSSPACE, path, curthread);
	error = namei(&nd);
	if (error != 0) {
		printf("Error looking up path: %d\n", error);
		return;
	}
	NDFREE(&nd, NDF_ONLY_PNBUF);

	vp = nd.ni_vp;

	if (!vn_isdisk_error(vp, &error)) {
		/* XXX Can we make it so we can use a file? */
		printf("Is not a disk! %d\n", error);
		vput(vp);
		return;
	}
	
	dev_ref(vp->v_rdev);

	g_topology_lock();

	// Geom layer vfs consumer
	error = g_vfs_open(vp, &osdata.os_consumer, "objsnap", 1);
	if (error != 0) {
		printf("Error opening geom devvp: %p %d\n", vp->v_rdev, error);
		vput(vp);

		g_topology_unlock();

		return;
	}

	g_topology_unlock();

	vref(vp);

	osdata.os_vp = vp;
	lockinit(&osdata.os_lock, PVFS, "objsnap_big_lock", 
		VLKTIMEOUT, LK_NOSHARE);

	superblock_init(vp);

	allocator_init();

	vput(vp);

	return;
}

static int
objsnap_ioctl(struct cdev *dev, u_long cmd, caddr_t data, int flag __unused,
    struct thread *td)
{
	switch (cmd) {

	case OBJSNAP_INIT:
		objsnap_init((struct objsnap_init_args *)data);
		break;

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
	.d_name = "objsnap_dev"
};

static int
objsnapHandler(struct module *inModule, int inEvent, void *inArg)
{
	int error = 0;

	switch (inEvent) {
	case MOD_LOAD:

		bzero(&osdata, sizeof(osdata));

		/* Make the SLS available to userspace. */
		error = make_dev_p(MAKEDEV_WAITOK | MAKEDEV_CHECKNAME, 
			&osdata.os_cdev, &objsnap_cdevsw, 0, UID_ROOT, GID_WHEEL, 
			0666, "objsnap");

		if (error) {
			return (error);
		}

		osdata.os_vp = NULL;

		vnode_cache = malloc(sizeof(struct objsnap_vnode) * MAXINODES,
			M_OBJSNAP, M_WAITOK);
	
		break;
	case MOD_UNLOAD:
		if (osdata.os_consumer != NULL) {
			g_topology_lock();

			g_vfs_close(osdata.os_consumer);

			g_topology_unlock();

			osdata.os_consumer = NULL;
			printf("Destroying consumer\n");
		}

		if (osdata.os_vp != NULL) {

			vrele(osdata.os_vp);

			osdata.os_vp = NULL;
			printf("Destroying device vnode\n");
		}

		if (osdata.os_cdev != NULL) {
			destroy_dev(osdata.os_cdev);

			osdata.os_cdev = NULL;
			printf("Destroying device\n");
		}

		for (int i = 0; i < MAXINODES; i ++) {
			struct objsnap_vnode *vnode = &vnode_cache[i];
			if (vnode->v_tree.v_tree != NULL) {
				free(vnode->v_tree.v_tree, M_OBJSNAP);
				vnode->v_tree.v_tree = NULL;
			}
		}
		free(vnode_cache, M_OBJSNAP);

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
