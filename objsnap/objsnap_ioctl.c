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

#include "objsnap_common.h"
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

struct sysctl_ctx_list objsnap_ctx;
int wait = 2;
int ckpt_flush = 1;

static uint64_t get_txn_id() {
	return atomic_fetchadd_64(&global_txnid, 1);
}

static uint64_t 
get_msg(int tid) {
	return atomic_load_64(&global_msgs[tid]);
}

static int
set_msg(int tid, uint64_t msg, uint64_t *expected)
{
	uint64_t *dst = &global_msgs[tid];
	return atomic_fcmpset_64(dst, expected, msg);
}

static void
objsnap_io_uio(struct objsnap_txn *txn, size_t pgoff, size_t pgcnt, void *data)
{
	size_t resid = BLOCKSIZE * pgcnt;
	struct iovec aiov[16];
	uint64_t before;
	struct uio uio;
	int error;
	int i;

	for (i = 0; i < pgcnt; i++) {
		aiov[i].iov_base = (void *)PHYS_TO_DMAP(txn->d_page[pgoff + i]->phys_addr);
		aiov[i].iov_len = BLOCKSIZE;
	}

	uio.uio_iov = (struct iovec *)&aiov;
	uio.uio_iovcnt = pgcnt;
	uio.uio_resid = resid;
	uio.uio_segflg = UIO_SYSSPACE;
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

static void __attribute__((noinline))
objsnap_io(struct objsnap_txn *set)
{
	struct g_consumer *cp = osdata.os_consumer;
	obj_diskptr_t ptr = set->d_ptr;
	int left = set->d_cnt;	
	uint64_t offset;
	int pagecnt;
	int error;
	int pgoff = 0;
	void *data;

	while (left) {
		pagecnt = min(OBJSNAP_MAXUIO, left);
		data = malloc(BLOCKSIZE * pagecnt, M_OBJSNAP, M_WAITOK | M_ZERO);
		objsnap_io_uio(set, pgoff, pagecnt, data);

		offset = DEVICE_BLOCK_NUM(ptr.offset + set->d_cnt - left) * 512;
		error = g_write_data(cp, offset, data, BLOCKSIZE * pagecnt);
		if (error != 0) {
			printf("IO: gwrite error %d for offset %ld\n", error, offset);
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
	STAT_TO_ARGS(args, WRITERS);
	STAT_TO_ARGS(args, WAITERS);
	STAT_TO_ARGS(args, DIRTY);
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

static void __attribute__((noinline))
objsnap_mktxn(int *mytids, size_t size_tids, struct objsnap_txn *txn)
{
	int i, j, ind;
	int tmptid;

	for (i = 0, ind = 0; i < size_tids; i++) {
		tmptid = mytids[i];

		for (j = 0; j < tpgs[tmptid].d_cnt; j++) { 
			txn->d_page[ind] = tpgs[tmptid].d_page[j];
			KASSERT(txn->d_page[ind] != NULL, ("transaction includes NULL page"));
			txn->d_index[ind] = tpgs[tmptid].d_index[j];
			txn->d_inode[ind] = tpgs[tmptid].d_inode[j];
			KASSERT(txn->d_index[ind] != 0, ("committing on invalid inode 0"));
			ind += 1;
		}
		tpgs[tmptid].d_cnt = 0;
	}

	txn->d_cnt = ind;
}

static void __attribute__((noinline))
objsnap_wal_log(struct objsnap_txn *txn, size_t npages)
{
	struct objsnap_wal_entry we;
	struct objsnap_wal_entry *other;
	uint64_t before;
	obj_diskptr_t walblk;
	int error;
	int i;

	OS_START(DATAWRITE, &before);

	for (i = 0; i < npages; i++) {
		we.we_ptrs[i].w_inode = txn->d_inode[i];
		we.we_ptrs[i].w_index = txn->d_index[i];
		we.we_ptrs[i].w_offset = txn->d_ptr.offset + i;
		KASSERT(we.we_ptrs[i].w_offset > alloc.alloc_starting_offset,
				("logging invalid pointer (%d, %d)",
				 txn->d_ptr.offset, txn->d_ptr.size));

	}

	we.we_cnt = npages;
	we.we_txnid = get_txn_id();
	struct g_consumer *cp = osdata.os_consumer;

	// This will acquire the wal lock
	objsnap_blkalloc_wal(&walblk);
	other = &wal_entries[walblk.offset - alloc.alloc_base];

	memcpy(other, &we, sizeof(struct objsnap_wal_entry));
	
	uint64_t offset = DEVICE_BLOCK_NUM(walblk.offset) * 512;
	error = g_write_data(cp, offset, other, BLOCKSIZE);
	if (error) {
		printf("wal: g_write_data error %d for offset %ld\n", error, offset);
	}

	OS_STOP(DATAWRITE, &before);

	return;
}

void
objsnap_txn_commit(struct objsnap_txn *txn, int tid, bool alloc)
{
	uint64_t before;

	atomic_fetchadd_64(&transaction_size, txn->d_cnt);
	atomic_fetchadd_64(&transaction_size_cnt, 1);

	OS_START(ALLOCATE, &before);
	// TODO:CHECK ERROR
	if (alloc)
		allocate_txn_block(txn, tid);
	OS_STOP(ALLOCATE, &before);

	objsnap_io(txn);

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
objsnap_checkpoint_txn(int tid)
{
	struct objsnap_txn txn;
	uint64_t waiters_time;
	uint64_t writers_time;
	int mytids[MAXTHREADS];
	size_t size_tids = 0;
	int total_size = 0;
	uint64_t expected;
	int i;
	if (tpgs[tid].d_cnt == 0) {
		printf("NOTHING TO CHECKPOINT?!\n");
		return;
	}

	expected = MSG_NONE;
	int success = set_msg(tid, MSG_CHECKPOINT, &expected);
	if (!success) {
		printf("Checkpoint state for tid %d should be %ld, but isnt %lu\n", tid, MSG_NONE, expected);
		expected = MSG_FORCED;
		if (set_msg(tid, MSG_CHECKPOINT, &expected))
			panic("Succeeded on state transition from MSG_FORCED?\n");
	}

	OS_START(WAITERS, &waiters_time);
	OS_START(WRITERS, &writers_time);

	if (objsnap_wait_entry(tid)) {
		/* Our write is fully serviced, we're done. */
		OS_STOP(WAITERS, &waiters_time);
		return;
	}

	/* Try to checkpoint ourselves, even if we fail we're still a writer. */
	expected = MSG_CHECKPOINT;
	if (set_msg(tid, MSG_CHECKPOINTING, &expected)) {
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

		expected = MSG_CHECKPOINT;
		if (set_msg(i, MSG_CHECKPOINTING, &expected)) {
			if ((total_size + tpgs[i].d_cnt) > MAXDRTYCNT)
				panic("TOCTTOU, transaction will overflow");
			mytids[size_tids++] = i;
			total_size += tpgs[i].d_cnt;
		}

	}

	if (total_size > MAXDRTYCNT)
		panic("transaction size too large");

	/* For the chunk allocator we write out a cold page for every new page. */
	garbage_collect(total_size);

	objsnap_mktxn((int *)mytids, size_tids, &txn);
	objsnap_txn_commit(&txn, tid, true);

	for (i = 0; i < size_tids; i++) {
		int local_tid = mytids[i];
		expected = MSG_CHECKPOINTING;
		success = set_msg(local_tid, MSG_NONE, &expected);
		if (!success) {
			printf("Msg should be checkpointing for %u (%lu), i am %d, but isnt\n", 
					local_tid, get_msg(local_tid), tid);
		}
	}

	sema_post(&wr);

	objsnap_wait_completion(tid);
	if (get_msg(tid) != MSG_NONE)
		panic("Did not actually get checkpointed\n");
	OS_STOP(WRITERS, &writers_time);


	return;
}

static void
objsnap_checkpoint(struct objsnap_checkpoint_args *args)
{
	return (objsnap_checkpoint_txn(args->tid));
}

void
objsnap_create_inode(index_t *indexp)
{
	osinode_t *inode;

	if ((inode = allocate_inode()) == NULL) {
		printf("Issue creating inode\n");
		*indexp = OBJINO_BADINDEX;
		return;
	}

	*indexp = inode->i_index / 2;

	KASSERT(*indexp != 0, ("created inode with index 0"));
}

static void
objsnap_create(struct objsnap_create_args *args)
{
	objsnap_create_inode(&args->os_index);
	return;
}

static vm_page_t
usrptr_to_page(vm_offset_t addr) {
	vm_map_entry_t entry;
	vm_object_t obj;
	vm_pindex_t pindex;
	vm_prot_t out_prot;
	boolean_t wired;
	vm_page_t m;

	struct proc *p = curthread->td_proc;
	struct vmspace *vms = p->p_vmspace;
    	vm_map_t map = &vms->vm_map;

	// Check if page is valid range
	if (!vm_map_range_valid(&vms->vm_map, addr, addr+ BLOCKSIZE))
		return (NULL);

	if (vm_map_lookup(&map, addr, VM_PROT_READ, 
		&entry, &obj, &pindex, &out_prot, &wired) != KERN_SUCCESS) {
		// Error handling
		return (NULL);
	}	

	VM_OBJECT_WLOCK(obj);
	m = vm_page_lookup(obj, pindex);
	if (m == NULL)
		panic("dirty page not resident");
	VM_OBJECT_WUNLOCK(obj);

	vm_map_lookup_done(map, entry);

	return (m);
}


static int
objsnap_dirty_page(struct objsnap_dirty_page_args *args)
{
	int tid = args->os_tid;
	struct objsnap_txn *set = &tpgs[tid];
	vm_offset_t addr = args->os_page;
	index_t inode_i = args->os_index;
	vm_page_t m;

	if (set->d_cnt > MAXDRTYCNT)
		panic("Too many dirty pages in transaction %d\n", set->d_cnt);

	m = usrptr_to_page(addr);
	if (m == NULL)
		panic("Bad dirty page\n");

	// SLOW LOOKUP
	for (int i = 0; i < set->d_cnt; i++) {
		KASSERT(set->d_page[i] != NULL, ("found NULL page during dirtying"));
		if (set->d_page[i] == m)
			return (0);
	}

	KASSERT(inode_i != 0, ("dirtying inode 0"));

	set->d_page[set->d_cnt] = m;
	set->d_index[set->d_cnt] = m->pindex;
	set->d_inode[set->d_cnt] = inode_i;
	set->d_cnt += 1;
	KASSERT(inode_i != 0, ("dirtying invalid inode 0"));

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
			struct walptr *walptr = &set.we_ptrs[t];
			obj_diskptr_t ptr = (obj_diskptr_t) {
				.offset = walptr->w_offset,
				.size = 1,
			};
			if (walptr->w_inode == inode_i[i]) {
				KASSERT(ptr.offset >= alloc.alloc_starting_offset, 
						("[thread %ld, inode %d] inserting invalid pointer (%d,%d)",
						 threadlist_at, inode_i[i], ptr.offset, ptr.size));
				VTREE_INSERT(&vnode->v_tree, 
					walptr->w_index , &ptr);
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
 

#define WAL_SYNCER_SIZE (1024)
static void
objsnap_wal_syncer(void *ctx)
{
	index_t inode_i[128];
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
		if (!check_within(tail, head, WAL_SYNCER_SIZE, MAX_WAL_ENTRIES)) {
			uint64_t inode_before;
			OS_START(INODE,&inode_before);
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
					free_block_system(tree->tr_freeme.list[i]);
				}

				// Move the deadlist to free list
				movelist(&tree->tr_freeme, &tree->tr_deadlist);
			}

			if (ckpt_flush)
  				VOP_FSYNC(osdata.os_vp, MNT_WAIT, curthread);
			atomic_store_64(&alloc.alloc_walptr_tail, (alloc.alloc_walptr_tail + WAL_SYNCER_SIZE) % MAX_WAL_ENTRIES);
			OS_STOP(INODE,&inode_before);
		}

		pause_sbt("waiting to checkpoint", 10 * SBT_1US, 0 ,0);
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

static void
objsnap_sysctl_init(void)
{
	struct sysctl_oid *root;

	sysctl_ctx_init(&objsnap_ctx);

	root = SYSCTL_ADD_ROOT_NODE(&objsnap_ctx, OID_AUTO, "objsnap", CTLFLAG_RW,
		0, "Objsnap Sysctl's");

	(void)SYSCTL_ADD_INT(&objsnap_ctx, SYSCTL_CHILDREN(root), OID_AUTO,
		"wait", CTLFLAG_RW, &wait, 2,
		"Amount of time in microseconds that waiters will always wait");
	(void)SYSCTL_ADD_INT(&objsnap_ctx, SYSCTL_CHILDREN(root), OID_AUTO,
		"ckpt_flush", CTLFLAG_RW, &ckpt_flush, 1,
		"Enable or disable flushing of object trees during checkpointing.");

	return;
}

static void
objsnap_sysctl_fini(void)
{
	if (sysctl_ctx_free(&objsnap_ctx))
		printf("Failed to destroy sysctl\n");
}


static int
objsnapHandler(struct module *inModule, int inEvent, void *inArg)
{
	int error = 0;

	switch (inEvent) {
	case MOD_LOAD:

		bzero(tpgs, sizeof(struct objsnap_txn) * MAXTHREADS);
		bzero(global_msgs, sizeof(uint64_t) * MAXTHREADS);

		objsnap_sysctl_init();
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
		printf("Transaction cnt %lu\n", transaction_size_cnt);

		objsnap_osdata_fini();

		objsnap_vncache_fini();

		allocator_destroy();

		objsnap_sysctl_fini();

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
