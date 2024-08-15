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
#include <sys/types.h>
#include <sys/lock.h>
#include <sys/rwlock.h>
#include <sys/stat.h>
#include <sys/syscallsubr.h>
#include <sys/sysctl.h>
#include <sys/taskqueue.h>
#include <sys/uio.h>
#include <sys/unistd.h>
#include <sys/vnode.h>
#include <sys/sema.h>

#include <vm/vm.h>
#include <vm/vm_extern.h>
#include <vm/vm_page.h>
#include <vm/vm_map.h>
#include <vm/vm_param.h>

#include <machine/param.h>
#include <machine/vmparam.h>

#include <geom/geom.h>
#include <geom/geom_vfs.h>

#include <objsnap_ioctl.h>

#include "objsnap_internal.h"
#include "alloc.h"
#include "btree.h"

#define MSG_NONE (0x0UL)
#define MSG_CHECKPOINT (0x1UL)
#define MSG_CHECKPOINTING (0x2UL)
#define MSG_FORCED (0xFUL)
#define MSG_MASK (0x3UL)
#define BACKOFF() \
       do { \
               for (int i = 0; i < 10; i++) \
                       __asm__ volatile ("pause" ::: ); \
       } while(0)


static int objsnap_osdata_init_syncer(void);

MALLOC_DEFINE(M_OBJSNAP, "objsnap", "objsnap");

struct objsnap_txn tpgs[MAXTHREADS];

struct objsnap_metadata osdata;
static uint64_t global_txnid;

super_t superblock;
struct objsnap_vnode *vnode_cache = NULL;

#define MAX_WRITERS (48)
#define OBJSNAP_MAXUIO (16)

struct sema wr;
uint64_t transaction_size = 0;
uint64_t transaction_size_cnt = 0;

struct objsnap_txn tpgs[MAXTHREADS];

struct __attribute__((packed)) objsnap_wal_entry {
	int we_cnt;	
	uint64_t we_txnid;
	struct walptr we_ptrs[MAXDRTYCNT];
};

struct objsnap_wal_entry *wal_entries;

static uint64_t global_msgs[MAXTHREADS];

static uint64_t get_txn_id() {
	return atomic_fetchadd_64(&global_txnid, 1);
}

static uint64_t 
get_msg(int tid) {
	return atomic_load_64(&global_msgs[tid]);
}

static int
set_msg(int tid, uint64_t msg, uint64_t expected)
{
	uint64_t *dst = &global_msgs[tid];
	return atomic_fcmpset_64(dst, &expected, msg);
}

static void
objsnap_io_uio(void *data, struct pageset *pgset, size_t pgcnt)
{
	size_t resid = BLOCKSIZE * pgcnt;
	struct iovec aiov[16];
	uint64_t before;
	struct uio uio;
	int i;
	int error;

	for (i = 0; i < pgcnt; i++) {
		aiov[i].iov_base = (void *)(uintptr_t)(pgset[i].offset);
		aiov[i].iov_len = BLOCKSIZE;
	}

	uio.uio_iov = (struct iovec *)&aiov;
	uio.uio_iovcnt = pgcnt;
	uio.uio_resid = resid;
	uio.uio_segflg = UIO_USERSPACE;
	uio.uio_rw = UIO_WRITE;
	uio.uio_td = curthread;
	uio.uio_offset = 0;

	OS_START(VNFAULTMOVE, &before);
	error = vn_io_fault_uiomove(data, resid, &uio);
	OS_STOP(VNFAULTMOVE, &before);
	if (error) {
		printf("ERROR %d\n", error);
	}
}

static void
objsnap_msnp_done(struct buf *bp)
{
	vm_page_t m;
	int i;

	for (i = 0; i < bp->b_npages; i++) {
		m = bp->b_pages[i];
		bp->b_pages[i] = NULL;

		m->flags &= ~VPO_SASCOW;
	}

	bp->b_bufsize = bp->b_bcount = 0;
	bp->b_npages = 0;

	bdone(bp);
}

/* 
 * MemSnap transactions take physical pages directly and are zero-copy.
 */
static void
objsnap_io_msnp(struct objsnap_txn *set)
{
	obj_diskptr_t ptr = set->d_ptr;
	uint64_t before;
	struct buf *bp;
	uint64_t ind;
	uint64_t off;
	int cnt, i;
	int error;

	for (ind = 0, cnt = 0; ind < set->d_cnt; ind += cnt) {
		_Static_assert(BLOCKSIZE == PAGE_SIZE, "ObjSnap block size");

		off = DEVICE_BLOCK_NUM(ptr.offset + ind);
		cnt = min(MAXBCACHEBUF / BLOCKSIZE, set->d_cnt - ind);

		bp = getpbuf(NULL);
		KASSERT(bp != NULL, ("could not get pbuf"));

		bp->b_data = unmapped_buf;

		bp->b_lblkno = off;
		bp->b_blkno = DEVICE_BLOCK_NUM(bp->b_lblkno);
		bp->b_iooffset = dbtob(bp->b_lblkno);
		bp->b_iocmd = BIO_WRITE;

		bp->b_npages = cnt;
		bp->b_resid = bp->b_bufsize = bp->b_bcount = bp->b_npages * PAGE_SIZE;
		bp->b_iodone = objsnap_msnp_done;

		for (i = 0; i < cnt; i++)
			bp->b_pages[i] = set->d_msnp[ind + i];

		bp->b_flags &= ~B_INVAL;
		bp->b_rcred = crhold(curthread->td_ucred);
		bp->b_wcred = crhold(curthread->td_ucred);

		OS_START(DATAWRITE, &before);
		BUF_ASSERT_LOCKED(bp);
		g_vfs_strategy(&osdata.os_vp->v_bufobj, bp);
		error = bufwait(bp);
		KASSERT(error == 0, ("bufwait %p returned %d (offset read %ld) (iooffset %ld)", bp, error, bp->b_lblkno, bp->b_iooffset));
		relpbuf(bp, NULL);
		OS_STOP(DATAWRITE, &before);

	}
}

static void
objsnap_io_block(struct objsnap_txn *set)
{
	obj_diskptr_t ptr = set->d_ptr;
	struct buf *src, *dst;
	uint64_t off;
	uint64_t ind;
	int cnt;
	int i;

	KASSERT(set->d_type == OBJTXN_BLOCK, ("not a block transaction"));

	for (ind = 0, cnt = 0; ind < set->d_cnt; ind += cnt) {
		off = DEVICE_BLOCK_NUM(ptr.offset + ind);
		cnt = min(MAXDRTYCNT, set->d_cnt - ind);

		
		dst = getblk(osdata.os_vp, off, cnt * BLOCKSIZE, 
			0, 0, GB_UNMAPPED);

		for (i = 0; i < cnt; i++) {
			src = getblk(osdata.os_vp, set->d_blk[ind + i].blkoff,
				BLOCKSIZE, 0, 0, GB_UNMAPPED);

			memcpy(dst->b_pages[i], src->b_pages[0], PAGE_SIZE);

			brelse(src);
		}

		bwrite(dst);
	}
}

static void
objsnap_io_page(struct objsnap_txn *set)
{
	obj_diskptr_t ptr = set->d_ptr;
	int pagecnt;
	int error;
	int left = set->d_cnt;	
	int pgoff = 0;
	struct g_consumer *cp = osdata.os_consumer;

	KASSERT(set->d_type == OBJTXN_PAGE, ("not a page transaction"));

	while (left) {
		pagecnt = min(OBJSNAP_MAXUIO, left);
		void * data = malloc(BLOCKSIZE * pagecnt, M_OBJSNAP, M_WAITOK | M_ZERO);

		objsnap_io_uio(data, &set->d_pg[pgoff], pagecnt);

		error = g_write_data(cp, DEVICE_BLOCK_NUM(ptr.offset + set->d_cnt - left) * 512, data, BLOCKSIZE * pagecnt);
		if (error) {
			printf("1: Gwrite error %d\n", error);
		}

		pgoff += pagecnt;
		left -= pagecnt;
		free(data, M_OBJSNAP);
	}
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
	STAT_TO_ARGS(args, GETBLK);
	args->os_cnt = OS_STAT_LAST;
	return (0);
}

static void
objsnap_wait_completion(int tid)
{
	const int threshold = 100000;
	int times = 0;

	for (times = 0; times < threshold; times++) {
		if (get_msg(tid) == MSG_NONE)
			return;

		pause_sbt("combiner wait", 1 * SBT_1US, 0 ,0);
	}

	printf("WARNING: excessive waiting (%d iterations)\n", threshold);
}

static bool
objsnap_wait_entry(int tid)
{
	int wait = (MAX_WRITERS - sema_value(&wr)) / 2;

	if (wait > 0)
		pause_sbt("combiner wait", wait * SBT_1US, 0 ,0);

	/* Either become a writer or wait till our write is serviced by one. */
	while (true) {
		/* If someone picked up our write we're done. */
		if (get_msg(tid) != MSG_CHECKPOINT) {
			objsnap_wait_completion(tid);

			return (true);
		}

		/* Else go for a promotion to writer. */
		if (sema_trywait(&wr))
			return (false);

		/* We are neither a writer not done, wait and try again. */
		pause_sbt("combiner wait", 1 * SBT_1US, 0 ,0);
	}
}

static void
objsnap_mktxn(int *mytids, size_t size_tids, enum objsnap_txn_type type, struct objsnap_txn *txn)
{
	int i, j, ind;
	int tmptid;

	txn->d_type = type;

	for (i = 0, ind = 0; i < size_tids; i++) {
		tmptid = mytids[i];

		for (j = 0; j < tpgs[tmptid].d_cnt; j++) { 
			switch (type) {
			case OBJTXN_PAGE:
				txn->d_pg[ind++] = tpgs[tmptid].d_pg[j];
				break;
			case OBJTXN_BLOCK:
				txn->d_blk[ind++] = tpgs[tmptid].d_blk[j];
				break;
			case OBJTXN_MSNP:
				txn->d_msnp[ind++] = tpgs[tmptid].d_msnp[j];
				break;
			}
		}
		tpgs[tmptid].d_cnt = 0;
	}

	txn->d_cnt = ind;
}

static void
objsnap_wal_log(struct objsnap_txn *txn, size_t npages)
{
	struct objsnap_wal_entry we;
	struct objsnap_wal_entry *other;
	uint64_t before;
	obj_diskptr_t walblk;
	int error;
	vm_page_t m;
	int i;

	OS_START(DATAWRITE, &before);

	for (i = 0; i < npages; i++) {
		switch (txn->d_type) {
		case OBJTXN_PAGE:
			we.we_ptrs[i].w_inode = txn->d_pg[i].inode;
			we.we_ptrs[i].w_index = txn->d_pg[i].pindex;
			we.we_ptrs[i].w_offset = txn->d_ptr.offset + i;
			break;
		case OBJTXN_BLOCK:
			we.we_ptrs[i].w_inode = txn->d_blk[i].objino;
			we.we_ptrs[i].w_index = IDX_TO_OFF(txn->d_blk[i].objoff);
			we.we_ptrs[i].w_offset = txn->d_ptr.offset + i;
			break;
		case OBJTXN_MSNP:
			m = txn->d_msnp[i];
			we.we_ptrs[i].w_inode = m->object->objid;
			we.we_ptrs[i].w_index = m->pindex;
			we.we_ptrs[i].w_offset = txn->d_ptr.offset + i;
			break;
		default:
			panic("invalid txn type %d\n", txn->d_type);
		}	
	}

	we.we_cnt = npages;
	we.we_txnid = get_txn_id();
	struct g_consumer *cp = osdata.os_consumer;

	// This will acquire the wal lock
	objsnap_blkalloc_wal(&walblk);
	other = &wal_entries[walblk.offset - alloc.alloc_base];

	memcpy(other, &we, sizeof(struct objsnap_wal_entry));
	
	error = g_write_data(cp, DEVICE_BLOCK_NUM(walblk.offset) * 512, other, BLOCKSIZE);
	if (error) {
		printf("wal: Gwrite error %d\n", error);
	}

	OS_STOP(DATAWRITE, &before);

	return;
}

static void
objsnap_txn_commit(struct objsnap_txn *txn)
{
	uint64_t before;

	atomic_fetchadd_64(&transaction_size, txn->d_cnt);
	atomic_fetchadd_64(&transaction_size_cnt, 1);

	OS_START(ALLOCATE, &before);
	// TODO:CHECK ERROR
	allocate_block(txn->d_cnt, &txn->d_ptr);
	OS_STOP(ALLOCATE, &before);


	/* Write out out the transaction. */
	switch (txn->d_type) {
	case OBJTXN_PAGE:
		objsnap_io_page(txn);
		break;

	case OBJTXN_BLOCK:
		objsnap_io_block(txn);
		break;

	case OBJTXN_MSNP:
		objsnap_io_msnp(txn);
		break;

	default:
		panic("invalid transaction data type %d\n", txn->d_type);
	}

	/* Construct the transaction entry on the WAL and flush it. */
	objsnap_wal_log(txn, txn->d_cnt);
}

static void
print_stat(struct timerstat stat, uint64_t to_unit)
{
	printf("Timer %s: avg(%lu), cnt(%lu), sum(%lu)\n", stat.name, stat.avg / to_unit, stat.cnt, stat.sum / to_unit);
}

static void
objsnap_printstats(void)
{
	uint64_t before = rdtscp();
	pause_sbt("combiner wait", SBT_1US, 0, 0);
	uint64_t per_us = rdtscp() - before;
	per_us = 1;
	print_stat(OS_TOSTAT_LOCKANDCOPY(), per_us);
	print_stat(OS_TOSTAT_DATAWRITE(), per_us);
	print_stat(OS_TOSTAT_UNLOCK(), per_us);
	print_stat(OS_TOSTAT_VNFAULTMOVE(), per_us);
	print_stat(OS_TOSTAT_INODE(), per_us);
	print_stat(OS_TOSTAT_BTFIND(), per_us);
	print_stat(OS_TOSTAT_BTCOW(), per_us);
	print_stat(OS_TOSTAT_BTINSERT(), per_us);
	print_stat(OS_TOSTAT_CHECKPOINT(), per_us);
	print_stat(OS_TOSTAT_ALLOCATE(), per_us);
}

void
objsnap_checkpoint_txn(int tid, enum objsnap_txn_type type)
{
	struct objsnap_txn txn;
	uint64_t checkpoint;
	int mytids[MAXTHREADS];
	size_t size_tids = 0;
	int total_size = 0;
	int i;

	int success = set_msg(tid, MSG_CHECKPOINT, MSG_NONE);
	if (!success) {
		printf("Checkpoint state for tid %d should be zero, but isnt %lu\n", tid, get_msg(tid));
		set_msg(tid, MSG_CHECKPOINT, MSG_FORCED);
	}

	OS_START(CHECKPOINT, &checkpoint);

	if (objsnap_wait_entry(tid)) {
		/* Our write is fully serviced, we're done. */
		OS_STOP(CHECKPOINT, &checkpoint);
		return;
	}

	uint64_t unlock;	
	OS_START(UNLOCK, &unlock);

	/* Try to checkpoint ourselves, even if we fail we're still a writer. */
	if (set_msg(tid, MSG_CHECKPOINTING, MSG_CHECKPOINT)) {
		mytids[size_tids++] = tid;
		total_size = tpgs[tid].d_cnt;
	}

	for (i = 0; i < MAXTHREADS && total_size < MAXDRTYCNT; i++) {
		/* 
		 * XXX There's a TOCTTOU race for d_cnt in this loop. 
		 */

		/* We can't have more the a 64KiB write combined chunk */
		if ((total_size + tpgs[i].d_cnt) > MAXDRTYCNT)
			continue;

		if (set_msg(i, MSG_CHECKPOINTING, MSG_CHECKPOINT)) {
			mytids[size_tids++] = i;
			total_size += tpgs[i].d_cnt;
		}

	}

	if (total_size > MAXDRTYCNT)
		panic("transaction size too large");

	objsnap_mktxn((int *)mytids, size_tids, type, &txn);
	objsnap_txn_commit(&txn);

	for (i = 0; i < size_tids; i++) {
		int local_tid = mytids[i];
		success = set_msg(local_tid, MSG_NONE, MSG_CHECKPOINTING);
		if (!success) {
			printf("Msg should be checkpointing for %u (%lu), i am %d, but isnt\n", 
					local_tid, get_msg(local_tid), tid);
		}
	}

	sema_post(&wr);

	objsnap_wait_completion(tid);
	OS_STOP(CHECKPOINT, &checkpoint);

	OS_STOP(UNLOCK, &unlock);

	return;
}

static void
objsnap_checkpoint(struct objsnap_checkpoint_args *args)
{
	return (objsnap_checkpoint_txn(args->tid, OBJTXN_PAGE));
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

	vm_map_lookup_done(map, entry);

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
	struct objsnap_txn *set = &tpgs[tid];
	int error = 0;
	
	error = usrptr_to_page(addr, &pageinfo);
	if (error) {
		printf("Error: Bad dirty page\n");
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

		/* XXXETSAL Handle errors during syncer initialization. */
		objsnap_osdata_init_syncer();

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
objsnap_sync_dirtylist(uint64_t threadlist_at, index_t inode_i[], int *inode_cnt) 
{
	struct objsnap_wal_entry set = wal_entries[threadlist_at];
	// These won't be actual reads, if system is under load, these will
	// almost always be in the cache.

	if (set.we_cnt > MAXDRTYCNT) {
		printf("ERROR IN SET CNT - WAY TOO LARGE for entry %lu - %d, head is around %lu\n", 
				threadlist_at, set.we_cnt, alloc.alloc_walptr_head);
		return (0);
	}

	// Create out list of inode objects
	for (int i = 0; i < set.we_cnt; i++) {
		struct walptr *ptr = &set.we_ptrs[i];
		int found = false;
		for (int t = 0; t < *inode_cnt; t++) {
			if (inode_i[t] == ptr->w_inode) {
				found = true;
			}
		}

		if (!found) {
			inode_i[*inode_cnt] = ptr->w_inode;
			*inode_cnt += 1;
		}
	}

	// We now go through every write it owns and update the tree
	for (int i = 0; i < *inode_cnt; i++) {
		// Grab our vnode
		struct objsnap_vnode *vnode = &vnode_cache[inode_i[i]];
		for (int t = 0; t < set.we_cnt; t++) {
			struct walptr *ptr = &set.we_ptrs[t];
			if (ptr->w_inode == inode_i[i]) {
				VTREE_INSERT(&vnode->v_tree, 
					ptr->w_index , &ptr->w_offset);
			}
		}

	}
	return (0);
}

static int
check_within(uint64_t s, uint64_t e, int within, int mod) {
	if (e == s) {
		return (1);
	}
	if (e > s) {
		return (s + within) >= e;
	}

	return ((s + within) % mod) >= e;
}
 

#define WAL_SYNCER_SIZE (512)
static void
objsnap_wal_syncer(void *ctx)
{
	index_t inode_i[32];
	memset(inode_i, 0, sizeof(index_t) * 32);
	mtx_lock(&osdata.os_syncer_lk);
	osdata.os_syncer_exit = OBJSYNC_RUNNING;

	printf("Starting checkpoint!\n");
	while (osdata.os_syncer_exit == OBJSYNC_RUNNING) {
		mtx_unlock(&osdata.os_syncer_lk);

		lockmgr(&alloc.alloc_lk, LK_EXCLUSIVE, 0);

		uint64_t head = atomic_load_64(&alloc.alloc_walptr_head);
		uint64_t tail = atomic_load_64(&alloc.alloc_walptr_tail);
		
		lockmgr(&alloc.alloc_lk, LK_RELEASE, 0);

		// Clear out current tail to head of Wal entrys, no need for a lock
		// If the head ptr outpaces us we just keep staying in the while look clearing
		// stuff out
		if (!check_within(tail, head, 2 * WAL_SYNCER_SIZE, MAX_WAL_ENTRIES)) {
			int inode_cnt = 0;
			for (uint64_t i = tail; i < (tail + WAL_SYNCER_SIZE); i++ ) {
				objsnap_sync_dirtylist((i % MAX_WAL_ENTRIES), inode_i, &inode_cnt);
			}
			for (int i = 0; i < inode_cnt; i++) {
				struct objsnap_vnode *vnode = &vnode_cache[inode_i[i]];
				osinode_t *inode = vnode->v_inode;
				// Update inodes to include checkpoint lists
				for (int t = 0; t < inode_cnt; t++) {
					inode->i_checkpointed_with[t] = inode_i[t];
				}
				
				inode->i_cnt = inode_cnt;

				// Set our inode to the correct value
				inode->i_version = get_txn_id();

				// Get the sibling inode and write to that instead.
				inode->i_index = (inode->i_index % 2) == 1 ? inode->i_index + 1 : inode->i_index - 1;

				//vtree_checkpoint(&vnode->v_tree);
				
				// During inserting we likely COW faulted which means we need to update our treeptr;
				inode->i_treeptr = VTREE_GETROOT(&vnode->v_tree);

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

  			VOP_FSYNC(osdata.os_vp, MNT_WAIT, curthread);
			atomic_store_64(&alloc.alloc_walptr_tail, (alloc.alloc_walptr_tail + WAL_SYNCER_SIZE) % MAX_WAL_ENTRIES);
		}

		pause_sbt("waiting to checkpoint", 200 * SBT_1US, 0 ,0);
		mtx_lock(&osdata.os_syncer_lk);
	}

	printf("Ending checkpoint!\n");
	osdata.os_syncer_exit = OBJSYNC_EXITED;
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
	wal_entries = malloc(sizeof(struct objsnap_wal_entry) * MAX_WAL_ENTRIES, M_OBJSNAP, M_WAITOK | M_ZERO);
	
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
	free(wal_entries, M_OBJSNAP);
}

static int
objsnap_osdata_init_syncer(void)
{
	int error;

	error = kthread_add((void (*)(void *))objsnap_wal_syncer, &osdata, NULL,
		&osdata.os_syncertd, 0, 0, "objsnap wal syncer");

	if (error != 0)
		printf("Syncer could not start");

	return (error);
}

static void
objsnap_osdata_fini_syncer(void)
{
	if (osdata.os_syncer_exit != OBJSYNC_UNINIT) {
		printf("Trying to lock!\n");
		osdata.os_syncer_exit = OBJSYNC_EXITING;
		printf("Waiting!\n");

		while(osdata.os_syncer_exit != OBJSYNC_EXITED)
			pause_sbt("waiting for checkpoint", 1 * SBT_1US, 0 ,0);
	}
	pause_sbt("waiting for checkpoint", 100 * SBT_1US, 0 ,0);
	printf("Done!\n");
	mtx_lock(&osdata.os_syncer_lk);
	mtx_unlock(&osdata.os_syncer_lk);

	mtx_destroy(&osdata.os_syncer_lk);
	sema_destroy(&wr);
	bzero(&osdata.os_syncer_lk, sizeof(osdata.os_syncer_lk));
}

static int
objsnap_osdata_init(void)
{
	int error;
	bzero(&osdata, sizeof(osdata));

	mtx_init(&osdata.os_syncer_lk, "Objsnap Syncer Lock", NULL, MTX_DEF);
	sema_init(&wr, MAX_WRITERS, "writers_sema");

	osdata.os_tq = taskqueue_create("objsnap tasksqueue", M_WAITOK, 
		taskqueue_thread_enqueue, &osdata.os_tq);

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
	struct objsnap_vnode *vnode;
	int i;

	objsnap_osdata_fini_syncer();

	if (osdata.os_cdev != NULL) {
		destroy_dev(osdata.os_cdev);

		osdata.os_cdev = NULL;
		printf("Destroying device\n");
	}

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

	for (i = 0; i < MAXINODES; i ++) {
		vnode = &vnode_cache[i];
		if (vnode->v_tree.v_tree != NULL) {
			btree_destroy(vnode->v_tree.v_tree);
			vnode->v_tree.v_tree = NULL;
			
		}

		if (vnode->v_inode != NULL) {
			free(vnode->v_inode, M_OBJSNAP);
		}
	}

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

		bzero(tpgs, sizeof(struct objsnap_txn) * MAXTHREADS);
		bzero(global_msgs, sizeof(uint64_t) * MAXTHREADS);

		// TODO: FOR NOW JUST SET TO ZERO, During recovery we
		// have to see the latest txn id
		global_txnid = 0;

		objsnap_vncache_init();

		error = objsnap_osdata_init();
		if (error != 0)
			return (error);

		break;
	case MOD_UNLOAD:
		printf("Transaction sizes %lu\n", transaction_size);
		printf("Transaction sizes cnt %lu\n", transaction_size_cnt);

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
