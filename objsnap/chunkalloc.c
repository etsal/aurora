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

MALLOC_DEFINE(M_CHUNKALLOC, "Chunk allocator", "chunkalloc");

static void *
ca_arrayalloc(size_t size)
{
	return (mallocarray(sizeof(struct ca_chunk), size,
		M_CHUNKALLOC, M_WAITOK | M_ZERO));
}

static void
ca_populate_chunks(struct chunkallocator *ca)
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

		ptr.offset = blk_per_chunk * i;
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

	/* 
	 * XXXETSAL Is treating the next array like a queue a good idea? 
	 * Chunk allocations cost linearly to the size of the entire 
	 * allocator for lightly loaded systems. Implementing is as 
	 * it was in the original for now till we discuss this.
	 */
	ch = ca->ca_next[0];
	for (i = 0; i < ca->ca_next_cnt - 1; i++) {
		ca->ca_next[i] = ca->ca_next[i + 1];
		KASSERT(ca->ca_next[i]->cac_state == CH_FREE,
			("used chunk in free list"));
	}
	ca->ca_next_cnt -= 1;
	ca->ca_num_used += 1;

	KASSERT(ch->cac_state == CH_FREE, ("allocated used chunk"));

	ch->cac_state = CH_ACTIVE;
	ch->cac_txn_size = 1UL << bucket;
	ch->cac_sec_max = ch->cac_ptr.size / ch->cac_txn_size;
	ch->cac_sec_free = ch->cac_sec_max;

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
ca_init(struct chunkallocator *ca)
{
	size_t diskbytes;

	bzero(ca, sizeof(*ca));
	mtx_init(&ca->ca_mtx, "objcamtx", NULL, MTX_DEF);

	ca->ca_txnsz_blk = CA_TXNSIZE;

	/* All chunks in the allocator. */
	diskbytes = superblock.super_size * superblock.super_bsize;
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

	ca_populate_chunks(ca);
	ca_populate_cands(ca);
}

static int
ca_move(struct chunkallocator *ca, int tid, int numblocks)
{
	return (0);
}

static int
ca_blkalloc(struct ca_chunk *ch, int numblocks)
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

		/* XXX Pass the pointer to the transaction. */
		return (0);
	}

	return (ENOSPC);
}

static int
ca_tryalloc(struct chunkallocator *ca, int numblocks, int bucket, bool prio, diskptr_t *ptr)
{
	struct ca_chunk *ch, *newch;
	int error;

	/* 
	 * If low priority, move blocks to coalesce them. 
	 * Keep doing so if we're critically low.
	 */
	if (prio) {
		do  {
			/* XXX Which TID are we using here?*/
			ca_move(ca, 0, 1ULL << bucket);
		} while (ca->ca_next_cnt < CA_CRITICAL_WATERMARK);
	}

	/* Grab the first chunk we find in the allocator. */
	mtx_lock(&ca->ca_mtx);
	/* XXX We assume chunks_candidates_old is unset */
	ch = ca->ca_cand[bucket];

	mtx_lock(&ch->cac_mtx);
	mtx_unlock(&ca->ca_mtx);

	/* XXX Actually pass the allocated blocks to the transactions. */
	error = ca_blkalloc(ch, numblocks);
	if (error == 0) {
		mtx_unlock(&ch->cac_mtx);
		return (0);
	}

	error = ca_get_free_chunk(ca, &newch, bucket); {
	if (error != 0)
		mtx_unlock(&ch->cac_mtx);
		return (ENOSPC);
	}

	/* Append the chunk to the used list. */
	KASSERT(ch->cac_state == CH_FULL, ("appending non-full chunk"));
	mtx_lock(&ca->ca_mtx);
	ca->ca_old[ca->ca_old_cnt++] = ch;
	mtx_unlock(&ca->ca_mtx);

	/* XXX old chunk logic */

	ca->ca_cand[bucket] = newch;
	KASSERT(ca->ca_cand[bucket]->cac_state == CH_ACTIVE, ("invalid candidate chunk"));

	mtx_unlock(&ch->cac_mtx);

	return (0);
}

int
ca_alloc(struct chunkallocator *ca, int numblocks, diskptr_t *ptr)
{
	const int bucket = determine_bucket(numblocks);
	bool high_pressure = false;
	int error;

	/* XXX What do we do with high pressure? */
	do {
		error = ca_tryalloc(ca, numblocks, bucket, false, ptr);
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
	KASSERT(chind < ca->ca_num_chunks, ("freeing out-of-bounds chunk"));

	mtx_lock(&ca->ca_mtx);
	ch = &ca->ca_chunks[chind];

	choff = ptr.offset - ch->cac_ptr.offset;
	KASSERT(choff >= 0, ("negative offset in chunk"));

	secind = choff / ch->cac_txn_size;
	KASSERT(secind < CA_MAXSEC, ("chunk sector index out of bounds"));

	bind = choff % ch->cac_txn_size;
	KASSERT(bind + ptr.size <= ch->cac_txn_size, ("block index out of bounds"));

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
