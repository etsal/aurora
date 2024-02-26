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
#include <vm/vm_map.h>
#include <vm/vm_param.h>

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
int pbufcnt = -1;

struct checkpoint_data {
	struct dirtyset cp_d;
	index_t cp_inode;
};

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
objsnap_done(struct bio *bip)
{
	g_destroy_bio(bip);
}

static void
write_page(
    struct pageset *pg, diskptr_t ptr)
{
	struct uio uio;
	struct iovec aiov;
	
	struct buf *bp = getblk(osdata.os_vp, DEVICE_BLOCK_NUM(ptr), BLOCKSIZE, 
		0, 0, GB_UNMAPPED);

	aiov.iov_base = (void *)(uintptr_t)(pg->offset);
	aiov.iov_len = BLOCKSIZE;
	uio.uio_iov = &aiov;
	uio.uio_iovcnt = 1;
	uio.uio_resid = BLOCKSIZE;
	uio.uio_segflg = UIO_USERSPACE;
	uio.uio_rw = UIO_WRITE;
	uio.uio_td = curthread;
	uio.uio_offset = 0;
	vn_io_fault_pgmove(bp->b_pages, 
		0, (int)BLOCKSIZE, 
		&uio);
	bawrite(bp);

	return;
}

static int
objsnap_systemstats(struct objsnap_systemstats_args *args) {
	STAT_TO_ARGS(args, LOCKANDCOPY);
	STAT_TO_ARGS(args, SERIALIZE);
	STAT_TO_ARGS(args, UNLOCK);
	STAT_TO_ARGS(args, INSERTPLUSPAGE);
	STAT_TO_ARGS(args, INODE);
	STAT_TO_ARGS(args, BTFIND);
	STAT_TO_ARGS(args, BTCOW);
	STAT_TO_ARGS(args, BTINSERT);
	STAT_TO_ARGS(args, CHECKPOINT);
	args->os_cnt = OS_STAT_LAST;
	return (0);
}

static void
objsnap_checkpoint(struct objsnap_checkpoint_args *args)
{
	int cnt = args->ckpt_cnt;
	index_t *inodes = args->ckpt_inodes;
	int i;
	struct objsnap_vnode *vnode;
	osinode_t *inode;
	struct checkpoint_data *sets;
	int error = 0;

	// Acquire Locks to copy over dirty lists, we need to worry about holding
	// onto the commit lock for too long. so well need to let go of our locks
	// and retry.
	OS_START(CHECKPOINT);

	OS_START(LOCKANDCOPY);

	for (i = 0; i < cnt; i++) {
		vnode = &vnode_cache[i];
		LOCK(&vnode->v_lock, LK_EXCLUSIVE);
		LOCK(&vnode->v_commit_lock, LK_EXCLUSIVE);
	}

	// Copy out the dirty sets
	sets = malloc(sizeof(struct checkpoint_data) * cnt, M_OBJSNAP, M_WAITOK);
	for (i = 0; i < cnt; i++) {
		vnode = &vnode_cache[i];
		memcpy(&sets[i].cp_d, &vnode->v_dirty, sizeof(struct dirtyset));
		sets[i].cp_inode = inodes[i];

		// Set the dirty cnt to zero!
		vnode->v_dirty.d_cnt = 0;
	}

	// Unlock Node locks!
	for (i = 0; i < cnt; i++) {
		vnode = &vnode_cache[i];
		UNLOCK(&vnode->v_lock);
	}

	OS_STOP(LOCKANDCOPY);
	
	OS_START(SERIALIZE);
	// Update data and trees
	for (i = 0; i < cnt; i++) {
		struct checkpoint_data *set = &sets[i];
		
		for (int t = 0; t < set->cp_d.d_cnt; t++) {
			// Update the tree
			
			struct pageset *pinfo = &set->cp_d.d_pg[t];
			diskptr_t ptr = allocate_block();
			OS_START(INSERTPLUSPAGE);
			VTREE_INSERT(&vnode->v_tree, 
				IDX_TO_OFF(pinfo->pindex) / BLOCKSIZE, &ptr);
			write_page(pinfo, ptr);
			OS_STOP(INSERTPLUSPAGE);
		}

		OS_START(INODE);
		vnode = &vnode_cache[i]; 
		inode = vnode->v_inode;

		// During inserting we likely COW faulted which means we need to update our treeptr;
		inode->i_treeptr = VTREE_GETROOT(&vnode->v_tree);
		VTREE_CHECKPOINT(&vnode->v_tree);

		// Update inodes to include checkpoint lists

		for (int t = 0; t < cnt; t++) {
			inode->i_checkpointed_with[t] = sets[t].cp_inode;
		}
		
		inode->i_cnt = cnt;
		inode->i_version += 1;

		// Get the sibling inode and write to that instead.
		inode->i_index = (inode->i_index % 2) == 1 ? inode->i_index + 1 : inode->i_index - 1;
		
		if ((error = write_ondisk_inode(inode))) {
			printf("Issue writing inode!\n");
		}
		OS_STOP(INODE);
	}

	OS_STOP(SERIALIZE);

	OS_START(UNLOCK);
	// Unlock commit locks
	for (i = 0; i < cnt; i++) {
		vnode = &vnode_cache[i];
		UNLOCK(&vnode->v_commit_lock);
	}

	free(sets, M_OBJSNAP);

	flush();

	//VOP_FSYNC(osdata.os_vp, MNT_WAIT, curthread);
	OS_STOP(UNLOCK);
	OS_STOP(CHECKPOINT);

	return;
}


static void
objsnap_create(struct objsnap_create_args *args)
{
	osinode_t *inode;
	if ((inode = allocate_inode()) == NULL) {
		printf("Issue creating inode\n");
		args->os_index = BADINDEX;
		return;
	}

	args->os_index = inode->i_index / 2;

	return;
}

static int
usrptr_to_page(vm_offset_t ptr, struct pageset *pinfo) {
	vm_map_entry_t entry;
	vm_object_t obj;
	vm_pindex_t pindex;
	vm_prot_t out_prot;
	boolean_t wired;

	struct proc *p = curthread->td_proc;
	struct vmspace *vms = p->p_vmspace;
    vm_map_t map = &vms->vm_map;


	// Check if page is valid range
	if (!vm_map_range_valid(&vms->vm_map, ptr, ptr + BLOCKSIZE))
		return (-1);

	if (vm_map_lookup(&map, ptr, VM_PROT_READ | VM_PROT_WRITE, 
		&entry, &obj, &pindex, &out_prot, &wired) != KERN_SUCCESS) {
		// Error handling
		return (-1);
	}	

	// Convert the KVA to a physical address (PA)
	vm_paddr_t pa = vtophys(entry);

	// Obtain the vm_page structure corresponding to the physical address
	struct vm_page *page = PHYS_TO_VM_PAGE(pa);

	vm_map_unlock_read(map);
	pinfo->page = page;
	pinfo->pindex = pindex;
	pinfo->offset = ptr;
	return (0);
}


static int
objsnap_dirty_page(struct objsnap_dirty_page_args *args)
{
	vm_offset_t addr = args->os_page;
	index_t inode_i = args->os_index;
	struct pageset pageinfo;
	int error = 0;

	struct objsnap_vnode *vnode = &vnode_cache[inode_i];
	if (vnode->v_magic != OBJMAGIC) {
		printf("Invalid vnode\n");
		return (error);
	}

	LOCK(&vnode->v_lock, LK_EXCLUSIVE);

	error = usrptr_to_page(addr, &pageinfo);
	if (error) {
		UNLOCK(&vnode->v_lock);
		return EINVAL;
	}

	// SLOW LOOKUP
	for (int i = 0; i < vnode->v_dirty.d_cnt; i++) {
		vm_pindex_t p = vnode->v_dirty.d_pg[i].pindex;
		if (pageinfo.pindex == p) {
			UNLOCK(&vnode->v_lock);
			return (0);
		}
	}

	vnode->v_dirty.d_pg[vnode->v_dirty.d_cnt] = pageinfo;
	vnode->v_dirty.d_cnt += 1;

	UNLOCK(&vnode->v_lock);

	return (0);
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
	lockinit(&osdata.os_lock, 0, "objsnap_big_lock", 
		0, LK_NOSHARE);

	superblock_init(vp);

	allocator_init();

	vput(vp);

	return;
}

static int
objsnap_stat(struct objsnap_stat_args *args)
{
	struct objsnap_vnode *vnode = &vnode_cache[args->os_index];
	// TODO: Do a get for an inode
	if (vnode->v_state == VNULL) {
		return -1;
	}

	args->os_inode = *vnode->v_inode;
	return (0);
}


static int
objsnap_ioctl(struct cdev *dev, u_long cmd, caddr_t data, int flag __unused,
    struct thread *td)
{
	int error = 0;

	switch (cmd) {

	case OBJSNAP_INIT:
		objsnap_init((struct objsnap_init_args *)data);

		// We did not create the FS!
		if (superblock.super_bsize == 0) {
			error = -1;
		}
		break;

	case OBJSNAP_CHECKPOINT:
		objsnap_checkpoint((struct objsnap_checkpoint_args *)data);
		break;

	case OBJSNAP_CREATEOBJ:
		objsnap_create((struct objsnap_create_args *)data);
		break;

	case OBJSNAP_DIRTYPAGE:
		error = objsnap_dirty_page((struct objsnap_dirty_page_args *)data);
		break;

	case OBJSNAP_STAT:
		error = objsnap_stat((struct objsnap_stat_args *)data);
		break;
	case OBJSNAP_SYSTEMSTATS:
		error = objsnap_systemstats((struct objsnap_systemstats_args *)data);
		break;
	}

	return (error);
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

		bzero(vnode_cache, sizeof(struct objsnap_vnode) * MAXINODES);

		bzero(osdata.os_stats, sizeof(struct cycletimer) * OS_STAT_MAX);
		// Initialize Locks
		for (int i = 0; i < MAXINODES; i++) {
			lockinit(&vnode_cache[i].v_lock, 0, "objsnap node lock", 
				0, 0);
			lockinit(&vnode_cache[i].v_commit_lock, 0, "objsnap commit lock", 
				0, 0);
			
		}
	
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

			if (vnode->v_inode != NULL) {
				free(vnode->v_inode, M_OBJSNAP);
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
