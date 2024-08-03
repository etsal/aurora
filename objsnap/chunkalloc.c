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
ca_arrayalloc(size_t size)
{
	return (mallocarray(sizeof(struct ca_chunk), size,
		M_CHUNKALLOC, M_WAITOK | M_ZERO));
}

static void
ca_populate_chunks(struct chunkallocator *ca, uint64_t offset)
{
	const size_t blk_per_chunk = CA_CHUNKSZ / superblock.super_bsize;
	struct ca_chunk *chunk;
	diskptr_t ptr;
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

		bzero(chunk->cac_map, sizeof(chunk->cac_map));
		chunk->cac_blocks_used = 0;
		chunk->cac_state = CH_FREE;

		ca->ca_next[i] = &ca->ca_chunks[i];
	}
}

static int
ca_get_free_chunk(struct chunkallocator *ca, struct ca_chunk **chp, int bucket)
{
	struct ca_chunk *ch;
	int i;

	KASSERT(bucket >= 0, ("negative bucket index"));
	KASSERT(bucket <= MAXPOWEROFTWO, ("bucket index out of bounds"));

	mtx_lock(&ca->ca_mtx);
	if (ca->ca_next_cnt == 0) {
		mtx_unlock(&ca->ca_mtx);
		return (ENOSPC);
	}

	ch = ca->ca_next[0];
	for (i = 0; i < ca->ca_next_cnt - 1; i++) {
		ca->ca_next[i] = ca->ca_next[i + 1];
		KASSERT(ca->ca_next[i]->cac_state == CH_FREE,
			("used chunk in free list"));
	}
	ca->ca_next_cnt -= 1;
	ca->ca_used_cnt += 1;

	KASSERT(ch->cac_state == CH_FREE, ("allocated used chunk"));

	ch->cac_state = CH_ACTIVE;
	ch->cac_txn_size = 1UL << bucket;
	ch->cac_sec_max = ch->cac_ptr.size / ch->cac_txn_size;
	ch->cac_sec_free = ch->cac_sec_max;
	KASSERT(ch->cac_sec_max != 0, ("zero-length sector"));

	mtx_unlock(&ca->ca_mtx);

	*chp = ch;

	return (0);
}

static void
ca_populate_cands(struct chunkallocator *ca)
{
	int error;
	int i;

	for (i = 0; i < ca->ca_cand_cnt; i++) {
		error = ca_get_free_chunk(ca, &ca->ca_cand[i], i);
		if (error != 0)
			panic("failed to populate candidate list");

		error = ca_get_free_chunk(ca, &ca->ca_cand_old[i], i);
		if (error != 0)
			panic("failed to populate old candidate list");
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
	ca->ca_chunks = ca_arrayalloc(ca->ca_chunk_cnt);

	ca->ca_next_cnt = ca->ca_chunk_cnt;
	ca->ca_next = ca_arrayalloc(ca->ca_chunk_cnt);

	/* Old (XXX already/partially used?) chunks. */
	ca->ca_old_cnt = 0;
	ca->ca_old = ca_arrayalloc(ca->ca_chunk_cnt);

	/* Candidate chunks. XXX What does "candidate" mean here? */
	ca->ca_cand_cnt = determine_bucket(ca->ca_txnsz_blk) + 1;
	ca->ca_cand = ca_arrayalloc(ca->ca_cand_cnt);
	ca->ca_cand_old = ca_arrayalloc(ca->ca_cand_cnt);

	ca_populate_chunks(ca, startoff);
	ca_populate_cands(ca);
}

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

static void
ca_move_io(struct chunkallocator *ca, int numblocks, struct objsnap_txn *txn)
{
	struct buf *src, *dst;
	int error;
	int i;

	error = ca_alloc(ca, numblocks, &txn->d_ptr);
	KASSERT(error == 0, ("out of space"));

	/* XXX Get a buffer for the new data. */
	dst = getblk(osdata.os_vp, DEVICE_BLOCK_NUM(txn->d_ptr.offset), BLOCKSIZE * numblocks,
			0, 0, GB_UNMAPPED);
	KASSERT(dst != NULL, ("failed to get buffer"));

	for (i = 0; i < txn->d_cnt; i++) {
		src = getblk(osdata.os_vp, DEVICE_BLOCK_NUM(txn->d_blk[i].blkoff), BLOCKSIZE,
				0, 0, GB_UNMAPPED);
		/* XXX The actual copy. */
		brelse(src);
	}

	bwrite(dst);
	brelse(dst);
}

static void
ca_move(struct chunkallocator *ca, int numblocks)
{
	struct objsnap_txn txn;
	struct ca_chunk *ch;
	diskptr_t ptr;
	int bucket; 
	int i;

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

	for (i = 0; i < txn.d_cnt; i++) {
		ptr = (diskptr_t) {
			.offset = txn.d_blk[i].blkoff,
			.size = 1,
		};
		ca_free(ca, ptr);
	}

}

static int
ca_blkalloc(struct ca_chunk *ch, int numblocks, diskptr_t *ptrp)
{
	diskptr_t ptr;
	int i;

	KASSERT(numblocks <= ch->cac_txn_size, ("allocating too many blocks from sector"));

	if (ch->cac_state == CH_FULL)
		return (ENOSPC);

	if (ch->cac_sec_free == 0)
		return (ENOSPC);

	/* Scan all sectors till we find a free one. */
	for (i = 0; i < ch->cac_sec_max; i++) {
		if (ch->cac_map[i].cas_bmap != 0)
			continue;

		ch->cac_map[i].cas_bmap = CA_SETALL(numblocks);
		ch->cac_blocks_used += numblocks;
		ch->cac_sec_free -= 1;

		ptr.offset = ch->cac_ptr.offset + (i * ch->cac_txn_size);
		ptr.size = numblocks;

		*ptrp = ptr;
		return (0);
	}

	return (ENOSPC);
}

static int
ca_tryalloc(struct chunkallocator *ca, int numblocks, int bucket, bool prio, diskptr_t *ptrp)
{
	struct ca_chunk *ch, *newch;
	int error;

	/* 
	 * If low priority, move blocks to coalesce them. 
	 * Keep doing so if we're critically low.
	 */
	if (prio == 0) {
		do  {
			ca_move(ca, 1ULL << bucket);
		} while (ca->ca_next_cnt < CA_CRITICAL_WATERMARK);
	}

	/* Grab the first chunk we find in the allocator. */
	mtx_lock(&ca->ca_mtx);
	/* XXX We assume chunks_candidates_old is unset */
	ch = ca->ca_cand[bucket];

	mtx_lock(&ch->cac_mtx);
	mtx_unlock(&ca->ca_mtx);

	error = ca_blkalloc(ch, numblocks, ptrp);
	if (error == 0) {
		mtx_unlock(&ch->cac_mtx);
		return (0);
	}


	ch->cac_state = CH_FULL;
	error = ca_get_free_chunk(ca, &newch, bucket);
	if (error != 0) {
		mtx_unlock(&ch->cac_mtx);
		return (ENOSPC);
	}

	/* Append the chunk to the used list. */
	KASSERT(ch->cac_state == CH_FULL, ("appending non-full chunk"));
	mtx_lock(&ca->ca_mtx);
	ca->ca_old[ca->ca_old_cnt++] = ch;
	mtx_unlock(&ca->ca_mtx);

	/* XXX logic for old chunks */

	ca->ca_cand[bucket] = newch;
	KASSERT(ca->ca_cand[bucket]->cac_state == CH_ACTIVE, ("invalid candidate chunk"));

	mtx_unlock(&ch->cac_mtx);

	return (ENOSPC);
}

int
ca_alloc(struct chunkallocator *ca, int numblocks, diskptr_t *ptrp)
{
	const int bucket = determine_bucket(numblocks);
	bool high_pressure = false;
	int error;

	/* XXX What do we do with high pressure? */
	do {
		error = ca_tryalloc(ca, numblocks, bucket, false, ptrp);
		if (error == 0)
			break;

		high_pressure = true;
	} while (true);

	return (0);
}

void
ca_free(struct chunkallocator *ca, diskptr_t ptr)
{
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
}

void
ca_destroy(struct chunkallocator *ca)
{
	free(ca->ca_cand, M_CHUNKALLOC);
	free(ca->ca_cand_old, M_CHUNKALLOC);

	free(ca->ca_old, M_CHUNKALLOC);
	free(ca->ca_next, M_CHUNKALLOC);
	free(ca->ca_chunks, M_CHUNKALLOC);

	mtx_destroy(&ca->ca_mtx);
	bzero(ca, sizeof(*ca));
}

void
ca_print(struct chunkallocator *ca)
{
	panic("unimplemented");
}
