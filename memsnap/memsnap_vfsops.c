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
#include <sys/limits.h>
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

#include "memsnap.h"

MALLOC_DEFINE(M_SLSFS, "slsfs_mount", "SLSFS mount structures");

/* Setup/Teardown for the VM subsystem hooks. */

static int
slsfs_cb_register(struct vfsconf *vfsp)
{
	sls_writefault_hook = slsfs_sas_trace_update;
	sas_cow_hook = sas_test_cow;

	return (0);
}

static int
slsfs_cb_unregister(struct vfsconf *vfsp)
{
	sls_writefault_hook = NULL;
	sas_cow_hook = NULL;

	return (0);
}

static int
slsfs_mount(struct mount *mp)
{
	struct slos_meta *sb;

	/* We do nothing on updates. */
	if (mp->mnt_flag & MNT_UPDATE)
		return (0);

	/* Get an ID for the new filesystem. */
	vfs_getnewfsid(mp);
	
	sb = malloc(sizeof(*sb), M_SLSFS, M_WAITOK | M_ZERO);
	mtx_init(&sb->sb_mtx, "sbmtx", NULL, MTX_DEF);
	sb->sb_sas_addr = SLS_SAS_INITADDR;

	/* Initialize the directory structure for name-inode equivalence. */
	sb->sb_sd.sd_cnt = SDI_MAXENTRIES;
	sb->sb_sd.sd_nextfree = 0;
	sb->sb_sd.sd_entries = malloc(sizeof(*sb->sb_sd.sd_entries) * SDI_MAXENTRIES, M_SLSFS, M_WAITOK);

	MNT_ILOCK(mp);
	mp->mnt_data = sb;
	mp->mnt_flag &= ~MNT_LOCAL;
	mp->mnt_kern_flag |= MNTK_USES_BCACHE;
	MNT_IUNLOCK(mp);

	return (0);
}

static int
slsfs_unmount(struct mount *mp, int mntflags)
{
	struct thread *td = curthread;
	struct slos_meta *sb;
	int flags = 0;
	int error;

	if (mntflags & MNT_FORCE)
		flags |= FORCECLOSE;

	/* Flush and destroy all mount vnodes. */
	error = vflush(mp, 0, flags, td);
	if (error != 0)
		return (error);

	/* 
	 * XXX Free the root vnode. We will use the 
	 * root vnode for lookups.
	 */

	MNT_ILOCK(mp);
	sb = mp->mnt_data;
	mp->mnt_data = NULL;
	mp->mnt_flag &= ~MNT_LOCAL;
	MNT_IUNLOCK(mp);

	free(sb->sb_sd.sd_entries, M_SLSFS);
	free(sb, M_SLSFS);

	return (0);
}

/*
 *  Return the vnode for the root of the filesystem.
 */
static int
slsfs_root(struct mount *mp, int flags, struct vnode **vpp)
{
	/* XXX Decide on the root mount operation. */
	panic("unimplemented");
}


static int
slsfs_statfs(struct mount *mp, struct statfs *sbp)
{
	return (EOPNOTSUPP);
}

/*
 * Get a new vnode for the specified SLOS inode.
 */
static int
slsfs_vget(struct mount *mp, uint64_t ino, int flags, struct vnode **vpp)
{
	struct thread *td = curthread;
	struct vnode *vp = NULL;
	int error;

	/* Truncate the inode number to 32 bits. */
	ino = OIDTOSLSID(ino);

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

	/* XXX This could potentially be the dummy root. */
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

VFS_SET(slsfs_vfsops, slsfs, 0);
MODULE_DEPEND(slsfs, objsnap, 0, 0, 0);
MODULE_VERSION(slsfs, 0);
