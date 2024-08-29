#include <sys/param.h>
#include <sys/lock.h>
#include <sys/queue.h>
#include <sys/systm.h>
#include <sys/bio.h>
#include <sys/libkern.h>
#include <sys/kernel.h>
#include <sys/kthread.h>
#include <sys/malloc.h>
#include <sys/mutex.h>
#include <sys/taskqueue.h>

#include <geom/geom.h>

#include "arraylist.h"
#include "objsnap_common.h"
#include "objsnap_internal.h"
#include "chunkalloc.h"

MALLOC_DEFINE(M_CHUNKALLOC, "Chunk allocator", "chunkalloc");

static void *
ca_arrayalloc(size_t numelems, size_t size)
{
	return (mallocarray(size, numelems, M_CHUNKALLOC, M_WAITOK | M_ZERO));
}

static inline void
ca_checkstate(struct ca_chunk *ch, enum ca_state state)
{
	if (ch->cac_state != state)
		panic("invalid chunk state %d, expected %d\n", ch->cac_state, state);
}

/*
 * Pop off the free list the chunk in index chind.
 */
static void
cac_from_free(struct chunkallocator *ca, size_t chind, struct ca_chunk **chp)
{
	struct ca_chunk *ch;

	mtx_assert(&ca->ca_mtx, MA_OWNED);
	KASSERT(chind < ca->ca_free_cnt, ("removing invalid chind %ld", chind));

	ch = ca->ca_free[chind];

	ca_checkstate(ch, CA_FREE);
	ch->cac_state = CA_NOQUEUE;

	ca->ca_free[chind] = ca->ca_free[ca->ca_free_cnt - 1];
	ca->ca_free_cnt -= 1;

	*chp = ch;
}

static void
cac_free_pop(struct chunkallocator *ca, struct ca_chunk **chp)
{
	if (ca->ca_free_cnt == 0) 
		panic("out of space");

	return (cac_from_free(ca, ca->ca_free_cnt - 1, chp));
}

static void
cac_to_free(struct chunkallocator *ca, struct ca_chunk *ch)
{
	KASSERT(ch->cac_blocks_used == 0, ("freeing non-empty chunk"));
	mtx_assert(&ca->ca_mtx, MA_OWNED);

	ca_checkstate(ch, CA_NOQUEUE);

	ch->cac_alloc_index = 0;
	ch->cac_state = CA_FREE;
	ca->ca_free[ca->ca_free_cnt++] = ch;

}

static void
cac_from_hot(struct chunkallocator *ca, struct ca_chunk **chp)
{
	struct ca_chunk *ch;

	mtx_assert(&ca->ca_mtx, MA_OWNED);

	/* There are no hot blocks available. */
	if (ca->ca_hot_start == ca->ca_hot_end) {
		*chp = NULL;
		return;
	}

	/* Grab the first element in the hot list. */
	ch = ca->ca_hot[ca->ca_hot_start];
	ca->ca_hot_start = (ca->ca_hot_start + 1) % ca->ca_chunk_cnt;

	ca_checkstate(ch, CA_HOT);
	ch->cac_state = CA_NOQUEUE;

	*chp = ch;
}

static void
cac_to_hot(struct chunkallocator *ca, struct ca_chunk *ch)
{
	mtx_assert(&ca->ca_mtx, MA_OWNED);

	ca_checkstate(ch, CA_NOQUEUE);
	ch->cac_state = CA_HOT;

	/* The hot list is a ring buffer. */
	ca->ca_hot[ca->ca_hot_end] = ch;
	ca->ca_hot_end = (ca->ca_hot_end + 1) % ca->ca_chunk_cnt;
	if (ca->ca_hot_start == ca->ca_hot_end)
		panic("hot list full");

}

static void
cac_to_cold(struct chunkallocator *ca, struct ca_chunk *ch)
{
	int bucket;

	mtx_assert(&ca->ca_mtx, MA_OWNED);

	ca_checkstate(ch, CA_NOQUEUE);
	ch->cac_state = CA_COLD;

	bucket = determine_bucket(ch->cac_blocks_used);
	KASSERT(bucket >= CA_COLD_BUCKETS, ("bucket offset too large"));
	ca->ca_cold[bucket][ca->ca_cold_cnt[bucket]++] = ch;
}

/* XXX cac_to_cold call for laundering cold blocks. */

static void
ca_init_chunks(struct chunkallocator *ca, uint64_t offset)
{
	const size_t blk_per_chunk = CA_CHUNKSZ / superblock.super_bsize;
	struct ca_chunk *ch;
	obj_diskptr_t ptr;
	int i;

	/* 
	 * Populate the allocator by all disk chunks into 
	 * it one at a time as if already allocated.
	 */
	for (i = 0; i < ca->ca_chunk_cnt; i++) {
		ch = &ca->ca_chunks[i];
		mtx_init(&ch->cac_mtx, "objchnmtx", NULL, MTX_DEF);
		ch->cac_index = i;
		ch->cac_blocks_used = 0;
		ch->cac_alloc_index = 0;
		ch->cac_state = CA_NOQUEUE;

		bzero(ch->cac_backmap, sizeof(ch->cac_backmap));
		bzero(ch->cac_launder, sizeof(ch->cac_launder));

		ptr.offset = offset + blk_per_chunk * i;
		ptr.size = blk_per_chunk;
		ch->cac_ptr = ptr;
	}
}

void
ca_init(struct chunkallocator *ca, uint64_t startoff, size_t numblocks)
{
	size_t diskbytes;
	int i;

	bzero(ca, sizeof(*ca));
	mtx_init(&ca->ca_mtx, "objcamtx", NULL, MTX_DEF);

	ca->ca_txnsz_blk = CA_TXNSIZE;

	/* All chunks in the allocator. */
	diskbytes = numblocks * superblock.super_bsize;
	ca->ca_chunk_cnt = diskbytes / CA_CHUNKSZ;
	ca->ca_chunks = ca_arrayalloc(sizeof(*ca->ca_chunks), ca->ca_chunk_cnt);

	/* 
	 * Initialize the free, hot, and cold lists. The free list is a stack,
	 * the hot list is a tail queue, and the cold list is a priority queue.
	 */
	ca->ca_free_cnt = 0;
	ca->ca_free = ca_arrayalloc(sizeof(*ca->ca_free), ca->ca_chunk_cnt);

	ca->ca_hot_start = 0;
	ca->ca_hot_end = 0;
	ca->ca_hot = ca_arrayalloc(sizeof(*ca->ca_hot), ca->ca_chunk_cnt);

	_Static_assert(CA_BLOCKS == 1 << CA_COLD_BUCKETS, "wrong number of cold buckets");
	for (i = 0; i < CA_COLD_BUCKETS; i++) {
		ca->ca_cold_cnt[i] = ca->ca_chunk_cnt;
		ca->ca_cold[i] = ca_arrayalloc(sizeof(struct ca_chunk *), ca->ca_chunk_cnt);
	}

	/* 
	 * Intermediate laundry list used to turn lightly
	 * loaded hot/cold chunks into free chunks. 
	 */
	TAILQ_INIT(&ca->ca_launder);
	/* Destination for used blocks extracted from laundered chunks. */
	ca->ca_launder_dst = NULL;

	/* Self contained system allocator used to handle inode and btree allocations. */
	TAILQ_INIT(&ca->ca_system_alloc);
	TAILQ_INIT(&ca->ca_system_full);

	/* Dump all chunks into the free queue. */
	mtx_lock(&ca->ca_mtx);
	ca_init_chunks(ca, startoff);
	for (i = 0; i < ca->ca_chunk_cnt; i++)
		cac_to_free(ca, &ca->ca_chunks[i]);
	mtx_unlock(&ca->ca_mtx);
}

void
ca_destroy(struct chunkallocator *ca)
{
	int i;

	for (i = 0; i < ca->ca_chunk_cnt; i++) 
		mtx_destroy(&ca->ca_chunks[i].cac_mtx);

	free(ca->ca_chunks, M_CHUNKALLOC);
	free(ca->ca_free, M_CHUNKALLOC);
	free(ca->ca_hot, M_CHUNKALLOC);
	for (i = 0; i < CA_COLD_BUCKETS; i++)
		free(ca->ca_cold[i], M_CHUNKALLOC);

	mtx_destroy(&ca->ca_mtx);
	bzero(ca, sizeof(*ca));
}

/* 
 * XXX Cold move daemon. Go through the cold list and find lightly loaded blocks.
 * These will be moved to the laundry list.
 */

void
ca_print(struct chunkallocator *ca)
{
	printf("[WARNING] Chunking allocator print unimplemented\n");
}

/* ===== Garbage collection path. ===== */

struct ca_getblk_args {
	struct task tk;
	struct chunkallocator *ca;
	struct ca_chunk *ch;
};

static void
ca_getblk(void *ctx, int __unused pending)
{
	struct ca_getblk_args *args = (struct ca_getblk_args *)ctx;
	struct chunkallocator *ca = args->ca;
	struct ca_chunk *ch = args->ch;
	struct bio *bp;
	vm_page_t m;
	int error;
	int i;

	mtx_lock(&ch->cac_mtx);
	for (i = 0; i < CA_BLOCKS; i++) {
		if (ch->cac_backmap[i].cao_ino == 0)
			continue;
		
		m = vm_page_alloc_freelist(VM_FREELIST_DEFAULT, VM_ALLOC_NORMAL | VM_ALLOC_NOOBJ | VM_ALLOC_WIRED);

		bp = g_alloc_bio();
		bp->bio_cmd = BIO_READ;
		bp->bio_done = NULL;
		bp->bio_offset = (ch->cac_ptr.offset + i) * BLOCKSIZE;
		bp->bio_data = (void *)PHYS_TO_DMAP(VM_PAGE_TO_PHYS(m));

		g_io_request(bp, osdata.os_consumer);
		error = biowait(bp, "cablk");
		if (error != 0)
			panic("error %d on chunk allocator geom read request", error);

		g_destroy_bio(bp);

		ch->cac_launder[i] = m;
	}

	ch->cac_alloc_index = 0;
	mtx_unlock(&ch->cac_mtx);

	/* We hold the only reference to the chunk, since it has no queue. */
	mtx_lock(&ca->ca_mtx);

	ca_checkstate(ch, CA_NOQUEUE);
	ch->cac_state = CA_LAUNDER;
	TAILQ_INSERT_TAIL(&ca->ca_launder, ch, cac_next);
	mtx_unlock(&ca->ca_mtx);
}

static void
ca_age(struct chunkallocator *ca)
{
	struct ca_chunk *chhot[CA_MAXHOT_TO_COLD];
	struct ca_getblk_args *args;
	struct ca_chunk *ch;
	uint64_t chind;
	int i;

	mtx_lock(&ca->ca_mtx);

	/* Is the hot list empty? */
	if (ca->ca_hot_start == ca->ca_hot_end) {
		mtx_unlock(&ca->ca_mtx);
		return;
	}

	for (chind = 0; chind < CA_MAXHOT_TO_COLD; chind++) {
		cac_from_hot(ca, &chhot[chind]);
		if (chhot[chind] == NULL)
			break;

		/* If we laundered enough blocks, break. */
		ca->ca_launder_surplus += (CA_BLOCKS - chhot[chind]->cac_blocks_used);
		if (ca->ca_launder_surplus >= CA_SURPLUS_THRESHOLD)
			break;
	}

	/* XXX Possibly turn heavily loaded chunks cold instead of laundering them. */

	for (i = 0; i < chind; i++) {
		ch = chhot[i];

		if (ch->cac_blocks_used == 0) {
			cac_to_free(ca, ch);
			continue;
		}

		args = malloc(sizeof(*args), M_OBJSNAP, M_NOWAIT);
		if (args == NULL)
			panic("out of memory");

		args->ca = ca;
		args->ch = ch;
		TASK_INIT(&args->tk, 0, ca_getblk, &args->tk);

		taskqueue_enqueue(osdata.os_tq, &args->tk);
		mtx_unlock(&ch->cac_mtx);
	}

	mtx_unlock(&ca->ca_mtx);
}

static void
ca_gc_alloc_from_dst(struct chunkallocator *ca, size_t numblocks, obj_diskptr_t *ptrp)
{
	obj_diskptr_t ptr;
	/* 
	 * Pop the required physical space from the soon-to-be cold chunk.
	 * If full, move it to the cold list and allocate a new one. 
	 */
	if (ca->ca_launder_dst != NULL && ca->ca_launder_dst->cac_alloc_index + numblocks > CA_BLOCKS) {
		cac_to_cold(ca, ca->ca_launder_dst);
		ca->ca_launder_dst = NULL;
	}

	if (ca->ca_launder_dst == NULL)
		cac_free_pop(ca, &ca->ca_launder_dst);

	ptr.offset = ca->ca_launder_dst->cac_alloc_index;
	ptr.size = numblocks;
	ca->ca_launder_dst->cac_alloc_index += numblocks;

	*ptrp = ptr;
}

static void
ca_gc_move(struct chunkallocator *ca, struct ca_chunk *ch, obj_diskptr_t ptr)
{
	struct objsnap_txn txn;
	size_t i;

	txn.d_ptr = ptr;
	txn.d_cnt = 0;
	
	for (i = ch->cac_alloc_index; i < CA_BLOCKS; i++) {
		if (ch->cac_launder[i] == NULL)
			continue;

		/* Populate the transaction with the page and remove it from the chunk. */
		txn.d_page[txn.d_cnt] = ch->cac_launder[i];
		txn.d_inode[txn.d_cnt] = ch->cac_backmap[i].cao_ino;
		txn.d_index[txn.d_cnt] = ch->cac_backmap[i].cao_off;
		txn.d_offset[txn.d_cnt] = ptr.offset + txn.d_cnt;

		ch->cac_backmap[i].cao_ino = 0;
		ch->cac_backmap[i].cao_off = 0;
		ch->cac_launder[i] = NULL;

		txn.d_cnt += 1;
		if (txn.d_cnt == ptr.size)
			break;
	}

	objsnap_txn_commit(&txn);

	for (i = 0; i < txn.d_cnt; i++)
		vm_page_free(txn.d_page[i]);
}

void
ca_gc(struct chunkallocator *ca, size_t numblocks)
{
	struct ca_chunk *ch;
	obj_diskptr_t ptr;

	ch = TAILQ_FIRST(&ca->ca_launder);

	/* Is there anything to launder in the first place? */
	if (ch == NULL)
		return;

	ca_checkstate(ch, CA_LAUNDER);

	ca_gc_alloc_from_dst(ca, numblocks, &ptr);

	ca_gc_move(ca, ch, ptr);

	/* If the old chunk is completely free, move it to the free list. */
	if (ch->cac_alloc_index == CA_BLOCKS) {
		TAILQ_REMOVE(&ca->ca_launder, ch, cac_next);
		ca_checkstate(ch, CA_LAUNDER);
		ch->cac_state = CA_NOQUEUE;

		cac_to_free(ca, ch);
	}
}

/* ===== Main allocation path. ===== */

static int
ca_free_select(struct chunkallocator *ca, int numblocks, struct ca_chunk **chp, int *chindp)
{
	struct ca_chunk *ch;
	int chind;

	mtx_lock(&ca->ca_mtx);

	/* Grab the first chunk we find in the allocator. */
	for (chind = ca->ca_free_cnt - 1; chind >= 0; chind--) {
		ch = ca->ca_free[chind];
		if (ch->cac_alloc_index + numblocks <= ca->ca_txnsz_blk)
			break;
	}

	mtx_unlock(&ca->ca_mtx);

	/* We didn't find any blocks. */
	if (chind == -1)
		return (ENOSPC);

	mtx_lock(&ch->cac_mtx);

	*chindp = chind;

	return (0);
}

static void
ca_blkalloc(struct ca_chunk *ch, struct objsnap_txn *txn)
{
	const size_t numblocks = txn->d_cnt;
	struct ca_objid *backmap;
	int ind, i;

	KASSERT(ch->cac_alloc_index + numblocks <= CA_BLOCKS, ("chunk cannot satisfy allocation"));

	/* Scan all sectors till we find a free one. */
	for (i = 0; i < numblocks ; i++) {
		ind = ch->cac_alloc_index + i; 
		backmap = &ch->cac_backmap[ind];

		backmap->cao_ino = txn->d_inode[i];
		backmap->cao_off = txn->d_index[i];
	}

	txn->d_ptr.offset = ch->cac_ptr.offset + ch->cac_alloc_index;
	txn->d_ptr.size = numblocks;
	
	ch->cac_blocks_used += numblocks;
	ch->cac_alloc_index += numblocks;
}

static int
ca_tryalloc_txn(struct chunkallocator *ca, struct objsnap_txn *txn)
{
	const size_t numblocks = txn->d_cnt;
	struct ca_chunk *ch;
	int error;
	int chind;

	if (numblocks > ca->ca_txnsz_blk)
		panic("requested allocation too large (%ld, max %d)\n", numblocks, ca->ca_txnsz_blk);

	/* Age as many hot blocks as we are allocating. */
	if (ca->ca_launder_surplus < CA_SURPLUS_THRESHOLD)
		ca_age(ca);

	/* XXX This should never happen after we implement moves. */
	if (ca->ca_free_cnt == 0)
		panic("allocator full");

	/* Find a block that can satisfy the allocation. */
	error = ca_free_select(ca, numblocks, &ch, &chind);
	if (error != 0)
		return (error);

	/* 
	 * Avoid TOCCTOU, someone may have consumed enough of the block
	 * while we tried to lock that it cannot satisfy the allocation. 
	 */
	if (ch->cac_alloc_index + numblocks > ca->ca_txnsz_blk) {
		mtx_unlock(&ca->ca_mtx);
		return (EAGAIN);
	}

	ca_blkalloc(ch, txn);

	/* Move fully allocated chunks to the hot list. */
	if (ch->cac_alloc_index >= ca->ca_txnsz_blk) {
		mtx_lock(&ca->ca_mtx);

		cac_from_free(ca, chind, &ch);
		cac_to_hot(ca, ch);

		mtx_unlock(&ca->ca_mtx);
	} 
	
	mtx_unlock(&ch->cac_mtx);

	return (0);
}

int
ca_alloc_txn(struct chunkallocator *ca, struct objsnap_txn *txn)
{
	int error;

	do {
		error = ca_tryalloc_txn(ca, txn);
	} while (error != 0);

	return (0);
}

/* ===== System allocation path. This is a self-contained allocator. ===== */

static void
ca_blkalloc_system(struct ca_chunk *ch, obj_diskptr_t *ptrp)
{
	obj_diskptr_t ptr;
	int i;

	for (i = 0; i < CA_BLOCKS; i++) {
		if (ch->cac_backmap[i].cao_ino == 0)
			break;

		KASSERT(ch->cac_backmap[i].cao_ino == CA_SYSTEM_INO,
				("corrupted system backmap"));
	}

	KASSERT(i < CA_BLOCKS, ("system chunk had no free block"));

	ch->cac_backmap[i].cao_ino = CA_SYSTEM_INO;
	ch->cac_backmap[i].cao_off = CA_SYSTEM_INO;

	ptr.offset = ch->cac_ptr.offset + i;
	ptr.size = 1;

	ch->cac_blocks_used += 1;
	 
	*ptrp = ptr;
}

static void
cac_from_system(struct chunkallocator *ca, struct ca_chunk *ch)
{
	KASSERT(!TAILQ_EMPTY(&ca->ca_system_alloc), ("empty system-alloc list"));
	mtx_assert(&ca->ca_mtx, MA_OWNED);
	ca_checkstate(ch, CA_SYSTEM);

	TAILQ_REMOVE(&ca->ca_system_alloc, ch, cac_next);
	ch->cac_state = CA_NOQUEUE;
}

static void
cac_to_system(struct chunkallocator *ca, struct ca_chunk *ch)
{
	ca_checkstate(ch, CA_NOQUEUE);
	mtx_assert(&ca->ca_mtx, MA_OWNED);
	ch->cac_state = CA_SYSTEM;

	TAILQ_INSERT_HEAD(&ca->ca_system_alloc, ch, cac_next);
}

static void
cac_from_system_full(struct chunkallocator *ca, struct ca_chunk *ch)
{
	KASSERT(!TAILQ_EMPTY(&ca->ca_system_full), ("empty system-full list"));
	mtx_assert(&ca->ca_mtx, MA_OWNED);
	ca_checkstate(ch, CA_SYSTEM_FULL);

	TAILQ_REMOVE(&ca->ca_system_full, ch, cac_next);
	ch->cac_state = CA_NOQUEUE;
}

static void
cac_to_system_full(struct chunkallocator *ca, struct ca_chunk *ch)
{
	mtx_assert(&ca->ca_mtx, MA_OWNED);
	ca_checkstate(ch, CA_NOQUEUE);
	ch->cac_state = CA_SYSTEM_FULL;

	TAILQ_INSERT_HEAD(&ca->ca_system_full, ch, cac_next);
}

static int
ca_tryalloc_system(struct chunkallocator *ca, obj_diskptr_t *ptrp)
{
	struct ca_chunk *ch;

	/* If we have a system */
	mtx_lock(&ca->ca_mtx);

	/* If necessary, pop a free block off the main allocator and into the system allocator. */
	if (TAILQ_EMPTY(&ca->ca_system_alloc)) {
		cac_free_pop(ca, &ch);
		cac_to_system(ca, ch);

		mtx_unlock(&ca->ca_mtx);
		return (EAGAIN);
	}

	ch = TAILQ_FIRST(&ca->ca_system_alloc);
	ca_checkstate(ch, CA_SYSTEM);

	ca_blkalloc_system(ch, ptrp);

	/* 
	 * If the block is full, we remove it from the non-free system list and put it
	 * in the full system list.
	 */
	if (ch->cac_blocks_used == CA_BLOCKS) {
		cac_from_system(ca, ch);
		cac_to_system_full(ca, ch);
	}

	mtx_unlock(&ca->ca_mtx);
	return (0);
}

int
ca_alloc_system(struct chunkallocator *ca, obj_diskptr_t *ptrp)
{
	int error;

	do {
		error = ca_tryalloc_system(ca, ptrp);
	} while (error != 0);

	return (0);
}

/* ===== Free path used for both regular and system blocks. ===== */

void
ca_free(struct chunkallocator *ca, obj_diskptr_t ptr)
{
	struct ca_chunk *ch;
	int chind = (ptr.offset * superblock.super_bsize) / CA_CHUNKSZ;
	int choff, ind, i;

	KASSERT(ptr.size != UINT_MAX, ("freeing invalid disk pointer"));
	KASSERT(chind < ca->ca_chunk_cnt, ("freeing out-of-bounds chunk %d %ld", chind, ca->ca_chunk_cnt));

	ch = &ca->ca_chunks[chind];
	mtx_lock(&ch->cac_mtx);

	KASSERT(ptr.offset >= ch->cac_ptr.offset, ("negative index into chunk"));
	choff = ptr.offset - ch->cac_ptr.offset;

	for (i = 0; i < ptr.size; i++) {
		ind = choff + i;
		KASSERT(ind < ch->cac_ptr.size, ("freeing out of bounds for chunk"));
		KASSERT(ind < CA_BLOCKS, ("index larger than maximum possible offset"));

		KASSERT(ch->cac_backmap[ind].cao_ino != 0, ("freeing already free block"));
		ch->cac_backmap[ind].cao_ino = 0;
		ch->cac_backmap[ind].cao_off = 0;
		
	}

	ch->cac_blocks_used -= ptr.size;

	/* Special case for the system block allocator. */
	if (ch->cac_state == CA_SYSTEM_FULL) {
		cac_from_system_full(ca, ch);
		cac_to_system(ca, ch);
	}

	/* XXX If the block is cold, then we should adjust which bucket it is in. */

	mtx_unlock(&ch->cac_mtx);
}
