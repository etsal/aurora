#include <sys/param.h>
#include <sys/lock.h>
#include <sys/queue.h>
#include <sys/systm.h>
#include <sys/kernel.h>
#include <sys/kthread.h>
#include <sys/mutex.h>
#include <sys/taskqueue.h>

#include "arraylist.h"
#include "chunkalloc.h"
#include "objsnap_internal.h"

#define CA_SETALL(size) ((size) == 64 ? UINT64_MAX : ((1ULL << (size)) - 1))
#define CA_TXNSIZE (64)
#define CA_CRITICAL_WATERMARK (4)
#define CA_NEEDCLEAN_RATIO (4)

MALLOC_DEFINE(M_CHUNKALLOC, "Chunk allocator", "chunkalloc");

static void *
ca_arrayalloc(size_t numelems, size_t size)
{
	return (mallocarray(size, numelems, M_CHUNKALLOC, M_WAITOK | M_ZERO));
}

static void
ca_populate_chunks(struct chunkallocator *ca, uint64_t offset)
{
	const size_t blk_per_chunk = CA_CHUNKSZ / superblock.super_bsize;
	struct ca_chunk *chunk;
	obj_diskptr_t ptr;
	int i;

	/* 
	 * Populate the allocator by all disk chunks into 
	 * it one at a time as if already allocated.
	 */
	for (i = 0; i < ca->ca_chunk_cnt; i++) {
		chunk = &ca->ca_chunks[i];
		mtx_init(&chunk->cac_mtx, "objchnmtx", NULL, MTX_DEF);
		chunk->cac_index = i;

		ptr.offset = offset + blk_per_chunk * i;
		ptr.size = blk_per_chunk;
		chunk->cac_ptr = ptr;

		bzero(chunk->cac_backmap, sizeof(chunk->cac_backmap));
		chunk->cac_blocks_used = 0;
		chunk->cac_alloc_index = 0;

		ca->ca_free[i] = &ca->ca_chunks[i];
	}
}

void
ca_init(struct chunkallocator *ca, uint64_t startoff, size_t numblocks)
{
	size_t diskbytes;

	bzero(ca, sizeof(*ca));
	mtx_init(&ca->ca_mtx, "objcamtx", NULL, MTX_DEF);

	ca->ca_txnsz_blk = CA_TXNSIZE;

	/* All chunks in the allocator. */
	diskbytes = numblocks * superblock.super_bsize;
	ca->ca_chunk_cnt = diskbytes / CA_CHUNKSZ;
	ca->ca_chunks = ca_arrayalloc(sizeof(*ca->ca_chunks), ca->ca_chunk_cnt);

	ca->ca_free_cnt = ca->ca_chunk_cnt;
	ca->ca_free = ca_arrayalloc(sizeof(*ca->ca_free), ca->ca_chunk_cnt);

	ca->ca_hot_cnt = ca->ca_chunk_cnt;
	ca->ca_hot = ca_arrayalloc(sizeof(*ca->ca_hot), ca->ca_chunk_cnt);

	ca->ca_cold_cnt = ca->ca_chunk_cnt;
	ca->ca_cold = ca_arrayalloc(sizeof(*ca->ca_cold), ca->ca_chunk_cnt);

	ca->ca_used_cnt = 0;

	ca_populate_chunks(ca, startoff);

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

static void
ca_move_attempt_free(struct chunkallocator *ca, int bucket)
{
	struct ca_chunk *ch;

	KASSERT(bucket < CA_MAXBUCKETS, ("invalid bucket %d", bucket));

	ch = ca->ca_move[bucket];
	if (ch == NULL)
		return;

	if (ch->cac_sec_free != ch->cac_sec_max)
		return;

	ca_move_free_empty(ca, ch);

	ca->ca_move[bucket] = NULL;
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
ca_move(struct chunkallocator *ca, int numblocks)
{
	panic("unimplemented");
#if 0
	struct objsnap_txn txn;
	struct ca_chunk *ch;
	int bucket; 

	/* Clean a chunk of blocks of the same size as the one we're allocating. */
	bucket = determine_bucket(numblocks);

	mtx_lock(&ca->ca_mtx);

	/* 
	 * If the block is already cleaned up from a previous move,
	 * free it back into the allocator.
	 */
	ca_move_attempt_free(ca, bucket);

	/* If the ratio of all to clean chunks is high enough, we're good. */
	if (ca->ca_next_cnt * CA_NEEDCLEAN_RATIO >= ca->ca_chunk_cnt) {
		mtx_unlock(&ca->ca_mtx);
		return;
	}

	/* Find a chunk to move blocks out of. */
	ch = ca->ca_move[bucket];
	if (ch == NULL) {
		ch = ca_move_pick_chunk(ca);
		if (ch == NULL) {
			mtx_unlock(&ca->ca_mtx);
			return;
		}
	}

	/* Move as many blocks as are being requested by the top-level ca_alloc call.*/
	ch->cac_state = CH_EMPTYING;

	ca_move_mktxn(ch, numblocks, &txn);

	mtx_unlock(&ca->ca_mtx);

	objsnap_txn_commit(&txn);
#endif
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
ca_free_select(struct chunkallocator *ca, int numblocks, struct ca_chunk **chp)
{
	struct ca_chunk *ch;
	int ind;

	mtx_lock(&ca->ca_mtx);

	/* Grab the first chunk we find in the allocator. */
	for (ind = ca->ca_free_cnt - 1; ind >= 0; ind--) {
		ch = ca->ca_free[ind];
		if (ch->cac_alloc_index + numblocks <= ca->ca_txnsz_blk)
			break;
	}

	mtx_unlock(&ca->ca_mtx);

	/* We didn't find any blocks. */
	if (ind == -1)
		return (ENOSPC);

	mtx_lock(&ch->cac_mtx);

	return (0);
}

static int
ca_tryalloc(struct chunkallocator *ca, int numblocks, obj_diskptr_t *ptrp)
{
	struct ca_chunk *ch;
	int error;

	if (numblocks > ca->ca_txnsz_blk)
		panic("requested allocation too large (%d, max %d)\n", numblocks, ca->ca_txnsz_blk);

#if 0
	/* 
	 * If low priority, move blocks to coalesce them. 
	 * Keep doing so if we're critically low.
	 */
	if (prio == 0) {
		do  {
			ca_move(ca, 1ULL << bucket);
		} while (ca->ca_next_cnt < CA_CRITICAL_WATERMARK);
	}
#endif

	/* XXX This should never happen when we implement moves. */
	if (ca->ca_free_cnt == 0)
		panic("allocator full");

	error = ca_free_select(ca, numblocks, &ch);
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

	if (ch->cac_alloc_index < ca->ca_txnsz_blk) {
		mtx_unlock(&ch->cac_mtx);
		return (0);
	}

	/* 
	 * XXX Free to hot: Remove the chunk from the free list, replacing it
	 * with the chunk at the end of the free list. Push the chunk to the tail
	 * of the hot queue.
	 */

	panic("move chunk from free list to hot list");

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
	panic("unimplemented");

	/* 
	 * XXX Go to the disk pointer's location and overwrite the objid/offset
	 * pair in a row. Adjust the number of blocks in use (?).
	 * XXX Add a running index
	 */

#if 0
	struct ca_chunk *ch;
	int chind = (ptr.offset * superblock.super_bsize) / CA_CHUNKSZ;
	int secind, bind, choff;

	KASSERT(ptr.size != UINT_MAX, ("freeing invalid disk pointer"));
	KASSERT(chind < ca->ca_chunk_cnt, ("freeing out-of-bounds chunk %d %ld", chind, ca->ca_chunk_cnt));

	mtx_lock(&ca->ca_mtx);
	ch = &ca->ca_chunks[chind];

	choff = ptr.offset - ch->cac_ptr.offset;
	KASSERT(choff >= 0, ("negative offset in chunk"));

	secind = choff / ch->cac_txn_size;
	KASSERT(secind < CA_MAXSEC, ("chunk sector index out of bounds"));

	bind = choff % ch->cac_txn_size;
	KASSERT(bind + ptr.size <= ch->cac_txn_size, ("bitmap index %d-%d out of bounds %d", bind, ptr.size, ch->cac_txn_size));

	KASSERT(ch->cac_map[secind].cas_bmap != 0, ("sector has no allocated blocks"));
	KASSERT(ptr.size == 1, ("freeing more than one block"));

	/* 
	 * XXXETSAL Isn't this condition supposed to always be true,
	 * otherwise this is a double free? Or is there some kind
	 * of benign race?
	 */
	if (ch->cac_map[secind].cas_bmap & (1ULL << bind)) {
		ch->cac_map[secind].cas_bmap &= ~(1ULL << bind);
		ch->cac_blocks_used -= 1;
	}

	/* Did we free up an entire sector? */
	if (ch->cac_map[secind].cas_bmap == 0)
		ch->cac_sec_free += 1;

	mtx_unlock(&ca->ca_mtx);
#endif
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
	free(ca->ca_cold, M_CHUNKALLOC);

	mtx_destroy(&ca->ca_mtx);
	bzero(ca, sizeof(*ca));
}

/* XXX Cold move daemon. */

void
ca_print(struct chunkallocator *ca)
{
	printf("[WARNING] Chunking allocator print unimplemented\n");
}
