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
#include "btree.h"

MALLOC_DEFINE(M_OBJSNAP, "objsnap", "objsnap");

struct objsnap_metadata osdata;
static uint64_t global_txnid;

super_t superblock;
struct objsnap_vnode *vnode_cache = NULL;
int pbufcnt = -1;
static struct dirtyset threadsets[MAXTHREADS];

struct checkpoint_data {
	struct dirtyset *cp_d;
	diskptr_t ptr;
};

static uint64_t get_txn_id() {
	return atomic_fetchadd_64(&global_txnid, 1);
}

static void
objsnap_wakeup_syncer()
{
	wakeup(&osdata.os_syncer_wakeup);
}

static int
objsnap_sysctl_init(void)
{
    //struct sysctl_oid *root;
	return (0);
}

static void
objsnap_sysctl_fini(void)
{
}

static void
objsnap_threadwal_flush(struct checkpoint_data *set)
{
	uint64_t before;
	struct uio uio;
	int pagecnt = set->cp_d->d_cnt;
	diskptr_t ptr = set->ptr;

	struct iovec *aiov = malloc(sizeof(struct iovec) * pagecnt , M_OBJSNAP, M_WAITOK);

	struct buf *bp = getblk(osdata.os_vp, 
		DEVICE_BLOCK_NUM(ptr.offset), BLOCKSIZE * pagecnt, 
		0, 0, GB_UNMAPPED);

	for (int t = 0; t < pagecnt; t++) {
		struct pageset *pinfo = &set->cp_d->d_pg[t];
		aiov[t].iov_base = (void *)(uintptr_t)(pinfo->offset);
		aiov[t].iov_len = BLOCKSIZE;
	}

	uio.uio_iov = aiov;
	uio.uio_iovcnt = pagecnt;
	uio.uio_resid = BLOCKSIZE * pagecnt;
	uio.uio_segflg = UIO_USERSPACE;
	uio.uio_rw = UIO_WRITE;
	uio.uio_td = curthread;
	uio.uio_offset = 0;
	OS_START(VNFAULTMOVE, &before);

	vn_io_fault_pgmove(bp->b_pages, 
		0, (int)BLOCKSIZE * pagecnt, 
		&uio);
	OS_STOP(VNFAULTMOVE, &before);

	OS_START(DATAWRITE, &before);
	bwrite(bp);
	OS_STOP(DATAWRITE, &before);

	free(aiov, M_OBJSNAP);
}

static void
objsnap_done(struct bio *bip)
{
	g_destroy_bio(bip);
}

static int
objsnap_systemstats(struct objsnap_systemstats_args *args) {
	STAT_TO_ARGS(args, LOCKANDCOPY);
	STAT_TO_ARGS(args, DATAWRITE);
	STAT_TO_ARGS(args, UNLOCK);
	STAT_TO_ARGS(args, VNFAULTMOVE);
	STAT_TO_ARGS(args, INODE);
	STAT_TO_ARGS(args, BTFIND);
	STAT_TO_ARGS(args, BTCOW);
	STAT_TO_ARGS(args, BTINSERT);
	STAT_TO_ARGS(args, CHECKPOINT);
	STAT_TO_ARGS(args, ALLOCATE);
	args->os_cnt = OS_STAT_LAST;
	return (0);
}


static void
objsnap_checkpoint(struct objsnap_checkpoint_args *args)
{
	uint64_t before;
	OS_START(CHECKPOINT, &before);
	struct dirtyset *set = &threadsets[args->tid];
	struct threadcheckpoint tckpt;
	struct checkpoint_data data;

	tckpt.tckpt_cnt = set->d_cnt;
	tckpt.tckpt_txnid = get_txn_id();

	diskptr_t ptr = allocate_block(set->d_cnt);
	diskptr_t threadblock = allocate_threadwal();
	for (int i = 0; i < set->d_cnt; i++) {
		tckpt.tckpt_ptrs[i].w_inode = set->d_pg[i].inode;
		tckpt.tckpt_ptrs[i].w_index = IDX_TO_OFF(set->d_pg[i].offset);
		tckpt.tckpt_ptrs[i].ptr.offset = ptr.offset + i;
		tckpt.tckpt_ptrs[i].ptr.size = 1;
	}
	struct buf *bp = getblk(osdata.os_vp, DEVICE_BLOCK_NUM(threadblock.offset), 
		BLOCKSIZE, 0, 0, 0);

	memcpy(bp->b_data, &tckpt, sizeof(struct threadcheckpoint));
	bawrite(bp);


	data.cp_d = set;
	data.ptr = ptr;

	objsnap_threadwal_flush(&data);

	set->d_cnt = 0;

	OS_STOP(CHECKPOINT, &before);

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

	if (vm_map_lookup(&map, ptr, VM_PROT_READ, 
		&entry, &obj, &pindex, &out_prot, &wired) != KERN_SUCCESS) {
		// Error handling
		return (-1);
	}	

	vm_map_unlock_read(map);
	pinfo->obj = obj;
	pinfo->pindex = pindex;
	pinfo->offset = ptr;
	return (0);
}


static int
objsnap_dirty_page(struct objsnap_dirty_page_args *args)
{
	vm_offset_t addr = args->os_page;
	index_t inode_i = args->os_index;
	int tid = args->os_tid;

	struct pageset pageinfo;
	pageinfo.inode = inode_i;
	struct dirtyset *set = &threadsets[tid];
	int error = 0;
	
	error = usrptr_to_page(addr, &pageinfo);
	if (error) {
		return EINVAL;
	}

	// SLOW LOOKUP
	for (int i = 0; i < set->d_cnt; i++) {
		vm_pindex_t p = set->d_pg[i].pindex;
		if (pageinfo.pindex == p) {
			return (0);
		}
	}

	if (set->d_cnt > MAXDRTYCNT) {
		printf("TRYING TO OVERLOAD THE THREAD %d\n", set->d_cnt);
		return (0);
	}

	set->d_pg[set->d_cnt] = pageinfo;
	set->d_cnt += 1;

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

	if (!vn_isdisk(vp, &error)) {
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

	osdata.os_provider = osdata.os_consumer->provider;

	g_topology_unlock();

	vref(vp);

	osdata.os_vp = vp;

	lockinit(&osdata.os_lock, 0, "objsnap_big_lock", 
		0, LK_NOSHARE);

	superblock_init(vp);

	allocator_init();

	// Init per thread dirty lists
	for (int x = alloc.alloc_walptr_head;
			x < alloc.alloc_walptr_head + MAXTHREADS;
			x++) {
    	struct buf *bp = getblk(osdata.os_vp, DEVICE_BLOCK_NUM(x), 
        	BLOCKSIZE, 0, 0, 0);
		bzero(bp->b_data, BLOCKSIZE);
		bwrite(bp);
	}

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
objsnap_sync_dirtylist(int threadlist_at) 
{
	struct buf *bp;
	struct threadcheckpoint set;
	index_t inode_i[64];
	int inode_cnt = 0;

	int error;

	// These won't be actual reads, if system is under load, these will
	// almost always be in the cache.
	if ((error = bread(osdata.os_vp, 
			DEVICE_BLOCK_NUM(threadlist_at), 
			BLOCKSIZE, NOCRED, &bp)) != 0) {
		return error;
	}

	memcpy(&set, bp->b_data, sizeof(struct threadcheckpoint));
	brelse(bp);

	// Create out list of inode objects
	for (int i = 0; i < set.tckpt_cnt; i++) {
		struct walptr *ptr = &set.tckpt_ptrs[i];
		int found = false;
		for (int t = 0; t < inode_cnt; t++) {
			if (inode_i[t] == ptr->w_inode) {
				found = true;
			}
		}

		if (!found) {
			inode_i[inode_cnt] = ptr->w_inode;
			inode_cnt += 1;
		}
	}

	// We now go through every write it owns and update the tree
	for (int i = 0; i < inode_cnt; i++) {
		// Grab our vnode
		struct objsnap_vnode *vnode = &vnode_cache[inode_i[i]];
		osinode_t *inode = vnode->v_inode;

		for (int t = 0; t < set.tckpt_cnt; t++) {
			struct walptr *ptr = &set.tckpt_ptrs[t];
			if (ptr->w_inode == inode_i[i]) {
				VTREE_INSERT(&vnode->v_tree, 
					IDX_TO_OFF(ptr->w_index) / BLOCKSIZE, &ptr->ptr);
			}
		}

		inode = vnode->v_inode;

		// Update inodes to include checkpoint lists
		for (int t = 0; t < inode_cnt; t++) {
			inode->i_checkpointed_with[t] = inode_i[t];
		}
		
		inode->i_cnt = inode_cnt;

		// Set our inode to the correct value
		inode->i_version = set.tckpt_txnid;

		// Get the sibling inode and write to that instead.
		inode->i_index = (inode->i_index % 2) == 1 ? inode->i_index + 1 : inode->i_index - 1;
		
		// During inserting we likely COW faulted which means we need to update our treeptr;
		inode->i_treeptr = VTREE_GETROOT(&vnode->v_tree);
		VTREE_CHECKPOINT(&vnode->v_tree);

		if (write_ondisk_inode(inode)) {
			printf("Issue writing inode!\n");
		}

		// GC Work Section
		btree_t tree = vnode->v_tree.v_tree;

		// We first free all value on the freelist 
		// This is values that the old inode (that has now completely gone)
		// and been rewritten, so we must free it.
		// For example: 
		// Epoch 1 (inode 1): 10 new writes, 0 COWS, 0 freeme, 0 deadlist
		// Epoch 2 (inode 2): 5 new writes, 5 COWS, 0 freeme, 5 deadlist
		// Epoch 3 (inode 1): 2 new writes, 2 COWS, 5 freeme, 2 deadlist
		// Epoch 4 (inode 2): 1 new writes, 1 COWS, 2 freeme, 1 deadlist
		for (int i = 0; i < tree->tr_freeme.cnt; i++) {
			free_block(tree->tr_freeme.list[i]);
		}

		// Move the deadlist to free list
		movelist(&tree->tr_freeme, &tree->tr_deadlist);

	}

	return (0);
}



static void
objsnap_wal_syncer(void *ctx)
{
	mtx_lock(&osdata.os_syncer_lk);
	while (!osdata.os_syncer_exit) {
		mtx_unlock(&osdata.os_syncer_lk);

		// Clear out current tail to head of Wal entrys, no need for a lock
		// If the head ptr outpaces us we just keep staying in the while look clearing
		// stuff out
		while (alloc.alloc_walptr_tail != alloc.alloc_walptr_head) {
			objsnap_sync_dirtylist(alloc.alloc_walptr_tail + alloc.alloc_base);
			// Ring buffer logic
			alloc.alloc_walptr_tail = (alloc.alloc_walptr_tail + 1) % MAXTHREADS;
		}

		mtx_lock(&osdata.os_syncer_lk);
		msleep_sbt(&osdata.os_syncer_wakeup, &osdata.os_syncer_lk,
			PRIBIO, "Sync-wait", SBT_1NS * 100000, 0,
			C_HARDCLOCK);
	}

	osdata.os_syncer_exit = -1;
	mtx_unlock(&osdata.os_syncer_lk);
	kthread_exit();
}

static void
objsnap_vncache_init(void)
{
	struct objsnap_vnode *vn;
	int i;

	vnode_cache = malloc(sizeof(*vnode_cache) * MAXINODES, M_OBJSNAP,
		M_WAITOK | M_ZERO);

	for (i = 0; i < MAXINODES; i++) {
		vn = &vnode_cache[i];

		lockinit(&vn->v_lock, 0, "objsnap node lock", 0, 0);
		lockinit(&vn->v_commit_lock, 0, "objsnap commit lock", 0, 0);
	}
}


static void
objsnap_vncache_fini(void)
{
	struct objsnap_vnode *vn;
	int i;

	if (vnode_cache == NULL)
		return;

	for (i = 0; i < MAXINODES; i++) {
		vn = &vnode_cache[i];

		lockdestroy(&vn->v_lock);
		lockdestroy(&vn->v_commit_lock);
	}

	free(vnode_cache, M_OBJSNAP);
}

static int
objsnap_osdata_init_syncer(void)
{
	int error;

	cv_init(&osdata.os_syncer_cv, "Objsnap Syncer CV");
	mtx_init(&osdata.os_syncer_lk, "Objsnap Syncer Lock", NULL, MTX_DEF);

	error = kthread_add((void (*)(void *))objsnap_wal_syncer, &osdata, NULL,
		&osdata.os_syncertd, 0, 0, "objsnap wal syncer");

	if (error != 0) {
		printf("Syncer could not start");
		cv_destroy(&osdata.os_syncer_cv);
		mtx_destroy(&osdata.os_syncer_lk);
	}

	return (error);
}

static void
objsnap_osdata_fini_syncer(void)
{
	osdata.os_syncer_exit = 1;
	objsnap_wakeup_syncer();

	while(osdata.os_syncer_exit != -1) {
		mtx_lock(&osdata.os_syncer_lk);
		msleep_sbt(&osdata.os_syncer_wakeup, &osdata.os_syncer_lk,
			PRIBIO, "Sync-exit-wait", SBT_1MS, 0,
			C_HARDCLOCK);
		mtx_unlock(&osdata.os_syncer_lk);
	}
}

static int
objsnap_osdata_init(void)
{
	int error;
	bzero(&osdata, sizeof(osdata));

	osdata.os_tq = taskqueue_create("objsnap tasksqueue", M_WAITOK, 
		taskqueue_thread_enqueue, &osdata.os_tq);

	/* XXXETSAL Handle errors during syncer initialization. */
	objsnap_osdata_init_syncer();

	taskqueue_start_threads(&osdata.os_tq, MAXTHREADS, PI_DISK, "objsnap taskqueue");

	/* Make the SLS available to userspace. */
	error = make_dev_p(MAKEDEV_WAITOK | MAKEDEV_CHECKNAME, 
		&osdata.os_cdev, &objsnap_cdevsw, 0, UID_ROOT, GID_WHEEL, 
		0666, "objsnap");

	return (error);
}

static void
objsnap_osdata_fini(void)
{
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
			btree_destroy(vnode->v_tree.v_tree);
			vnode->v_tree.v_tree = NULL;
			
		}

		if (vnode->v_inode != NULL) {
			free(vnode->v_inode, M_OBJSNAP);
		}
	}

	objsnap_osdata_fini_syncer();

	taskqueue_quiesce(osdata.os_tq);
	taskqueue_free(osdata.os_tq);
	osdata.os_tq = NULL;
}

static int
objsnapHandler(struct module *inModule, int inEvent, void *inArg)
{
	int error = 0;

	switch (inEvent) {
	case MOD_LOAD:

		bzero(threadsets, sizeof(struct dirtyset) * MAXTHREADS);

		// TODO: FOR NOW JUST SET TO ZERO, During recovery we
		// have to see the latest txn id
		global_txnid = 0;

		objsnap_vncache_init();

		error = objsnap_osdata_init();
		if (error != 0)
			return (error);

		break;
	case MOD_UNLOAD:
		objsnap_osdata_fini();

		objsnap_vncache_fini();

		allocator_destroy();

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
