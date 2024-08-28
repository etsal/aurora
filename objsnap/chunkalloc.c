#include <sys/param.h>
#include <sys/lock.h>
#include <sys/queue.h>
#include <sys/systm.h>
#include <sys/libkern.h>
#include <sys/kernel.h>
#include <sys/kthread.h>
#include <sys/mutex.h>
#include <sys/taskqueue.h>

#include "arraylist.h"
#include "chunkalloc.h"
#include "objsnap_internal.h"

MALLOC_DEFINE(M_CHUNKALLOC, "Chunk allocator", "chunkalloc");

static void *
ca_arrayalloc(size_t numelems, size_t size)
{
	return (mallocarray(size, numelems, M_CHUNKALLOC, M_WAITOK | M_ZERO));
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
	mtx_assert(&ch->cac_mtx, MA_OWNED);

	KASSERT(ch->cac_state == CA_FREE, ("invalid chunk state %d", ch->cac_state));
	/* 
	 * We assume the allocator works fine, so if we have
	 * no free space something has gone terribly wrong.
	 */
	if (ca->ca_free_cnt == 0)
		panic("out of space");

	ca->ca_free[chind] = ca->ca_free[ca->ca_free_cnt - 1];
	ca->ca_free_cnt -= 1;

	*chp = ch;
}

static void
cac_to_free(struct chunkallocator *ca, struct ca_chunk *ch)
{
	KASSERT(ch->cac_blocks_used == 0, ("freeing non-empty chunk"));
	mtx_assert(&ca->ca_mtx, MA_OWNED);

	ch->cac_alloc_index = 0;
	ch->cac_state = CA_FREE;
	ca->ca_free[ca->ca_free_cnt++] = ch;
	mtx_unlock(&ch->cac_mtx);

}

static void
cac_from_hot(struct chunkallocator *ca, struct ca_chunk **chp)
{
	struct ca_chunk *ch;

	mtx_assert(&ca->ca_mtx, MA_OWNED);

	/* There are no hot blocks available. */
	if (ca->ca_hot_start == ca->ca_hot_end)
		return;

	/* Grab the first element in the hot list (one after the start) */
	ca->ca_hot_start = (ca->ca_hot_start + 1) % ca->ca_chunk_cnt;
	ch = ca->ca_hot[ca->ca_hot_start];

	mtx_lock(&ch->cac_mtx);
	KASSERT(ch->cac_state == CA_HOT, ("invalid chunk state %d", ch->cac_state));

	*chp = ch;
}

static void
cac_to_hot(struct chunkallocator *ca, struct ca_chunk *ch)
{
	mtx_assert(&ca->ca_mtx, MA_OWNED);
	mtx_assert(&ch->cac_mtx, MA_OWNED);

	KASSERT(ch->cac_state == CA_FREE, ("appending non-free block to hot"));
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
	mtx_assert(&ch->cac_mtx, MA_OWNED);

	KASSERT(ch->cac_state == CA_HOT, ("appending non-hot block to cold"));
	ch->cac_state = CA_COLD;

	bucket = determine_bucket(ch->cac_blocks_used);
	KASSERT(bucket >= CA_COLD_BUCKETS, ("bucket offset too large"));
		ca->ca_cold[bucket][ca->ca_cold_cnt[bucket]++] = ch;
}

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

		bzero(ch->cac_backmap, sizeof(ch->cac_backmap));

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

	ca->ca_free_cnt = ca->ca_chunk_cnt;
	ca->ca_free = ca_arrayalloc(sizeof(*ca->ca_free), ca->ca_chunk_cnt);

	ca->ca_hot_start = 0;
	ca->ca_hot_end = 0;
	ca->ca_hot = ca_arrayalloc(sizeof(*ca->ca_hot), ca->ca_chunk_cnt);

	ca->ca_free_cnt = 0;
	ca->ca_free = ca_arrayalloc(sizeof(*ca->ca_free), ca->ca_chunk_cnt);

	TAILQ_INIT(&ca->ca_system_alloc);
	TAILQ_INIT(&ca->ca_system_full);

	mtx_lock(&ca->ca_mtx);
	ca_init_chunks(ca, startoff);
	for (i = 0; i < ca->ca_chunk_cnt; i++)
		cac_to_free(ca, &ca->ca_chunks[i]);
	mtx_unlock(&ca->ca_mtx);

	_Static_assert(CA_BLOCKS == 1 << CA_COLD_BUCKETS, "wrong number of cold buckets");
	for (i = 0; i < CA_COLD_BUCKETS; i++) {
		ca->ca_cold_cnt[i] = ca->ca_chunk_cnt;
		ca->ca_cold[i] = ca_arrayalloc(sizeof(struct ca_chunk *), ca->ca_chunk_cnt);
	}

}

#if 0
static void
ca_move_free_empty(struct chunkallocator *ca, struct ca_chunk *ch)
{
	int i;
	for (i = 0; i < ch->cac_sec_max; i++)
		KASSERT(ch->cac_map[i].cas_bmap == 0, ("inconsistent block map"));
	KASSERT(ch->cac_blocks_used == 0, ("used blocks in chunk"));
	ch->cac_state = CH_FREE;
	ca->ca_next[ca->ca_next_cnt++] = ch;
}

static struct ca_chunk *
ca_move_pick_chunk(struct chunkallocator *ca)
{
	struct ca_chunk *minch = NULL;
	int i, minind = -1;

	/* If we don't have ready work, find an old chunk and return it. */
	for (i = 0; i < ca->ca_old_cnt; i++) {
		/* Any old chunks we find that are free, put them into the next queue. */
		while (i < ca->ca_old_cnt && ca->ca_old[i]->cac_blocks_used == 0) {
			ca_move_free_empty(ca, ca->ca_old[i]);

			ca->ca_old[i] = ca->ca_old[ca->ca_old_cnt - 1];
			ca->ca_old_cnt -= 1;
		}

		/* 
		 * XXX Add high pressure flag to turn the cleanup policy from
		 * best-fit to first-fit.
		 */
		if (minch == NULL || ca->ca_old[i]->cac_blocks_used < minch->cac_blocks_used) {
			minch = ca->ca_old[i];
			minind = i;
		}
	}

	if (minind >= 0) {
		ca->ca_old[minind] = ca->ca_old[ca->ca_old_cnt - 1];
		ca->ca_old_cnt -= 1;
	}

	return (minch);
}

static int
ca_move_mktxn(struct ca_chunk *ch, size_t numblocks, struct objsnap_txn *txn)
{
	struct ca_sector *sec;
	uint64_t offset;
	struct txn;
	size_t ind;
	int i, j;

	bzero(txn, sizeof(*txn));
	txn->d_type = OBJTXN_BLOCK;

	/* Find enough blocks to move. */
	ind = 0;
	for (i = 0; i < ch->cac_sec_max; i++) {
		if (ind == numblocks)
			break;

		sec = &ch->cac_map[i];
		if (sec->cas_bmap == 0)
			continue;

		/* Scan the sector for blocks to write out. */
		for (j = 0; j < ch->cac_txn_size; j++) {
			if ((sec->cas_bmap & (1ULL << j)) == 0)
				continue;

			offset = ch->cac_ptr.offset + (ch->cac_txn_size * i) + j;
			txn->d_blk[ind].blkoff = offset;
			txn->d_blk[ind].objoff = sec->cas_objs[j].cao_ino;
			txn->d_blk[ind].objino = sec->cas_objs[j].cao_off;
			ind += 1;

			KASSERT(ind < MAXDRTYCNT, ("transaction too large"));
		}
	}

	KASSERT(ind < MAXDRTYCNT, ("transaction too large"));
	txn->d_cnt = ind;

	return (0);
}
#endif

static void
ca_age(struct chunkallocator *ca, int numblocks)
{
	struct ca_chunk *chhot[CA_MAXHOT_TO_COLD];
	struct ca_chunk *ch;
	uint64_t chind;
	int i;

	mtx_lock(&ca->ca_mtx);

	/* If we are in need of free blocks, if we are not return immediately. */
	if (ca->ca_free_cnt * CA_TOTAL_VS_FREE_RATIO > ca->ca_chunk_cnt)
		return;

	/* Is the hot list empty? */
	if (ca->ca_hot_start == ca->ca_hot_end)
		return;

	mtx_lock(&ca->ca_mtx);
	for (chind = 0; chind < CA_MAXHOT_TO_COLD; chind++) {
		cac_from_hot(ca, &chhot[chind]);
		if (chhot[chind] == NULL)
			break;

		/* If we laundered enough blocks, break. */
		numblocks -= (CA_BLOCKS - chhot[chind]->cac_blocks_used);
		if (numblocks <= 0)
			break;
	}

	/* 
	 * XXX Get a running sum of total blocks saved, and if it above a certain
	 * empty ratio then trigger the transaction immediately. Basically, check
	 * if this is an easy reclamation.
	 */

	for (i = 0; i < chind; i++) {
		ch = chhot[i];
		KASSERT(ch->cac_state == CA_HOT, ("invalid state %d", ch->cac_state));

		if (ch->cac_blocks_used == 0) {
			cac_to_free(ca, ch);
			continue;
		}

		cac_to_cold(ca, ch);
		mtx_unlock(&ch->cac_mtx);
	}
	mtx_unlock(&ca->ca_mtx);

}

static void
ca_blkalloc(struct ca_chunk *ch, int numblocks, obj_diskptr_t *ptrp)
{
	int __unused ind, i;

	KASSERT(ch->cac_alloc_index + numblocks <= CA_BLOCKS, ("chunk cannot satisfy allocation"));

	/* Scan all sectors till we find a free one. */
	for (i = 0; i < numblocks ; i++) {
		ind = ch->cac_ptr.offset + ch->cac_alloc_index + i; 
		panic("Have not populated the backmap");
		/* 
		 * XXX Populate the backmap. We need to get the write-combined
		 * transaction, go through it, and get the inode/offset pair for each block.
		 * We then go through the chunk and log the pair down.
		 */
	}

	ptrp->offset = ch->cac_ptr.offset + ch->cac_alloc_index;
	ptrp->size = numblocks;
	
	ch->cac_blocks_used += numblocks;
	ch->cac_alloc_index += numblocks;
}

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

static int
ca_tryalloc(struct chunkallocator *ca, int numblocks, obj_diskptr_t *ptrp)
{
	struct ca_chunk *ch;
	int error;
	int chind;

	if (numblocks > ca->ca_txnsz_blk)
		panic("requested allocation too large (%d, max %d)\n", numblocks, ca->ca_txnsz_blk);

	/* Age as many hot blocks as we are allocating. */
	ca_age(ca, numblocks);

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

	ca_blkalloc(ch, numblocks, ptrp);

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
ca_alloc(struct chunkallocator *ca, int numblocks, obj_diskptr_t *ptrp)
{
	int error;

	do {
		error = ca_tryalloc(ca, numblocks, ptrp);
	} while (error != 0);

	return (0);
}

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

		/* XXX Make sure that inode 0 can mean "free inode". */
		KASSERT(ch->cac_backmap[ind].cao_ino != 0, ("freeing already free block"));
		ch->cac_backmap[ind].cao_ino = 0;
		ch->cac_backmap[ind].cao_off = 0;
		
	}

	ch->cac_blocks_used -= ptr.size;

	/* Special case for the system block allocator. */
	if (ch->cac_state == CA_SYSTEM_FULL) {
		TAILQ_REMOVE(&ca->ca_system_full, ch, ca_next);
		KASSERT(ch->cac_state == CA_SYSTEM_FULL, ("invalid block state %d", ch->cac_state));
		ch->cac_state = CA_SYSTEM;
		TAILQ_INSERT_HEAD(&ca->ca_system_alloc, ch, ca_next);
	}

	mtx_unlock(&ch->cac_mtx);
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

/* XXX Cold move daemon. */

void
ca_print(struct chunkallocator *ca)
{
	printf("[WARNING] Chunking allocator print unimplemented\n");
}

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

static int
ca_tryalloc_system(struct chunkallocator *ca, obj_diskptr_t *ptrp)
{
	struct ca_chunk *ch;

	/* If we have a system */
	mtx_lock(&ca->ca_mtx);

	/* If necessary, pop a free block off the main allocator and into the system allocator. */
	if (TAILQ_EMPTY(&ca->ca_system_alloc)) {

		cac_from_free(ca, ca->ca_free_cnt - 1, &ch);
		KASSERT(ch->cac_state == CA_FREE, ("invalid block state %d", ch->cac_state));
		ch->cac_state = CA_SYSTEM;

		TAILQ_INSERT_HEAD(&ca->ca_system_alloc, ch, ca_next);

		mtx_unlock(&ca->ca_mtx);

		return (EAGAIN);
	}

	ch = TAILQ_FIRST(&ca->ca_system_alloc);
	KASSERT(ch->cac_state == CA_SYSTEM, ("invalid block state %d", ch->cac_state));

	ca_blkalloc_system(ch, ptrp);

	/* 
	 * If the block is full, we remove it from the non-free system list and put it
	 * in the full system list.
	 */
	if (ch->cac_blocks_used == CA_BLOCKS) {
		TAILQ_REMOVE(&ca->ca_system_alloc, ch, ca_next);
		KASSERT(ch->cac_state == CA_SYSTEM, ("invalid block state %d", ch->cac_state));
		ch->cac_state = CA_SYSTEM_FULL;
		TAILQ_INSERT_HEAD(&ca->ca_system_full, ch, ca_next);
	}

	mtx_unlock(&ca->ca_mtx);
	return (0);
}

void
ca_alloc_system(struct chunkallocator *ca, obj_diskptr_t *ptrp)
{
	int error;

	do {
		error = ca_tryalloc_system(ca, ptrp);
	} while (error != 0);

}
