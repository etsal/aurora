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

static MALLOC_DEFINE(M_SLSFS, "slsfs_mount", "SLSFS mount structures");

static vfs_root_t slsfs_root;
static vfs_statfs_t slsfs_statfs;
static vfs_vget_t slsfs_vget;
static vfs_sync_t slsfs_sync;

struct slos_meta {
	struct mtx sb_mtx;
	uint64_t sb_sas_addr;
	/* 
	 * XXX Do we need a state variable? We never have dirty data
	 * because we just provide the MemSnap interface to ObjSnap.
	 */
	/*
	 * XXX We need some indexing structure for the SAS
	 * objects, since we are addressing them by name.
	 */
};

struct slos_node {
	vm_object_t sn_obj;
	vaddr_t sn_addr;
	size_t sn_size;
};

/*
 * Register the Aurora filesystem type with the kernel.
 */
static void
slsfs_cb_register(struct vfsconf *vfsp)
{
	sls_writefault_hook = slsfs_sas_trace_update;
	sas_cow_hook = sas_test_cow;

}

/*
 * Unregister the Aurora filesystem type from the kernel.
 */
static void
slsfs_cb_unregister(struct vfsconf *vfsp)
{
	sls_writefault_hook = NULL;
	sas_cow_hook = NULL;
}

static int
slsfs_mount(struct mount *mp)
{
	struct slos_meta *sb;
	int error;

	/* We do nothing on updates. */
	if (mp->mnt_flag & MNT_UPDATE)
		return (0);

	/* Get an ID for the new filesystem. */
	vfs_getnewfsid(mp);
	
	sb = malloc(sizeof(*sb), M_SLSFS, M_WAITOK | M_ZERO);
	mtx_init(&sb->sb_mtx, "sbmtx", NULL, MTX_DEF);
	sb->sb_sas_addr = SLS_SAS_INITADDR;

	/* 
	 * XXX Create a root that acts as a directory. This node
	 * is used just for lookups and is the only directory in
	 * the file system.
	 */

	MNT_ILOCK(mp);
	mp->mnt_data = sb;
	mp->mnt_flag &= ~MNT_LOCAL;
	mp->mnt_kern_flag |= MNTK_USES_BCACHE;
	MNT_IUNLOCK(mp);

	return (0);
}

static int
slsfs_statfs(struct mount *mp, struct statfs *sbp)
{
	return (EOPNOTSUPP);
}

static int
slsfs_unmount(struct mount *mp, int mntflags)
{
	struct slos_meta *sb;

	if (mntflags & MNT_FORCE) {
		flags |= FORCECLOSE;
	}


	/* 
	 * XXX Can there be in-progress operations 
	 * while we are unmounting? The vnodes are
	 * stateless so as long as we can tear them
	 * out from any in-progress users we're good.
	 */

	MNT_ILOCK(mp);
	sb = mp->mnt_data;
	mp->mnt_data = NULL;
	mp->mnt_flag &= ~MNT_LOCAL;
	MNT_IUNLOCK(mp);

	free(sb, M_SLSFS);


	return (0);
}

/*
 * Get a new vnode for the specified SLOS inode.
 */
static int
slsfs_vget(struct mount *mp, uint64_t ino, int flags, struct vnode **vpp)
{
	struct thread *td = curthread;
	struct vnode **vpp = NULL;
	struct vnode *vp = NULL;
	int error;

	/* Truncate the inode number to 32 bits. */
	ino = ino & (INT_MAX - 1);

	/* Make sure the inode does not already have a vnode. */
	error = vfs_hash_get(mp, ino, LK_EXCLUSIVE, td, &vp, NULL, NULL);
	if (error)
		return (error);

	/* If we do have a vnode already, return it. */
	if (vp != NULL) {
		*vpp = vp;
		return (0);
	}

	/* Get a new blank vnode. */
	error = getnewvnode("sas", mp, &slsfs_vnodeops, &vp);
	if (error) {
		*vpp = NULL;
		return (error);
	}

	/*
	 * If the vnode is not the root, which is managed directly
	 * by the SLOS, add it to the mountpoint.
	 */
	vn_lock(vp, LK_EXCLUSIVE);

	error = insmntque(vp, mp);
	if (error)
		goto free;

	vp->v_type = VREG;
	vp->v_data = malloc(sizeof(struct slos_node), M_SLSFS, M_WAITOK | M_ZERO);

	/*
	 * Try to insert the new node into the table. We might have been
	 * beaten to it by another process, in which case we reuse their
	 * fresh vnode for the inode.
	 */
	error = vfs_hash_insert(vp, ino, LK_EXCLUSIVE, td, vpp, NULL, NULL);
	if (error != 0) {
		*vpp = NULL;
		return (error);
	}

	/* If we weren't beaten to it, propagate the new node to the caller. */
	if (*vpp == NULL) {
		*vpp = vp;
	} else {
		error = 0;
		goto free;
	}

	return (0);

free:
	if (vp != NULL)
		vput(vp);

	*vpp = NULL;

	return (error);
}

static int
slsfs_sync(struct mount *mp, int waitfor)
{
	return (0);
}

static struct vfsops slsfs_vfsops = { 
	.vfs_init = slsfs_cb_register,
	.vfs_uninit = slsfs_cb_unregister,
	.vfs_root = slsfs_root,
	.vfs_statfs = slsfs_statfs,
	.vfs_mount = slsfs_mount,
	.vfs_unmount = slsfs_unmount,
	.vfs_vget = slsfs_vget,
	.vfs_sync = slsfs_sync,
};

int
slsfs_loader(struct module *m, int what, void *arg)
{
	int error = 0;

	switch (what) {
	case MOD_LOAD:
		error = vfs_modevent(NULL, what, &virtiofs_vfsconf);
		if (error != 0)
			break;

		slsfs_cb_register();

	case MOD_UNLOAD:
		error = vfs_modevent(NULL, what, &virtiofs_vfsconf);
		if (error != 0)
			break;

		slsfs_cb_unregister();

	default:
		return (EINVAL);
	}

	return (error);
}

static moduledata_t slsfs_moddata {
	"slsfs",
	&slsfs_loader,
	&slsfs_vfsconf,
};

MODULE_DEPEND(slsfs, objsnap, 0, 0, 0);
MODULE_VERSION(slsfs, 0);
