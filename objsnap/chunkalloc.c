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

#define CA_COUNTER(ca, counter) ((ca)->ca_stats.cs_ ## counter)
#define CA_COUNTER_INCREMENT(ca, counter) do { CA_COUNTER(ca, counter)++; } while (0)
#define CA_COUNTER_ADD(ca, counter, incr) do { CA_COUNTER(ca, counter) += (incr); } while (0)

SDT_PROBE_DEFINE0(objsnap, , , chunk_launder_start);
SDT_PROBE_DEFINE0(objsnap, , , chunk_launder_finish);

MALLOC_DEFINE(M_CHUNKALLOC, "Chunk allocator", "chunkalloc");

static inline int
ca_offset_to_chind(struct chunkallocator *ca, uint64_t offset)
{
	return ((offset  - ca->ca_startoff) * BLOCKSIZE) / CA_CHUNKSZ;
}

static inline uint64_t
ca_chind_to_offset(struct chunkallocator *ca, int chind)
{
	_Static_assert(CA_CHUNKSZ / BLOCKSIZE == CA_BLOCKS, "invalid block size");
	return ca->ca_startoff + ((CA_CHUNKSZ / BLOCKSIZE) * chind);
}

static void *
ca_arrayalloc(size_t numelems, size_t size)
{
	return (mallocarray(size, numelems, M_CHUNKALLOC, M_WAITOK | M_ZERO));
}

#ifdef INVARIANTS
static inline void
ca_checkused_unlocked(struct ca_chunk *ch)
{
	int i;
	size_t used = 0;

	for (i = 0; i < CA_BLOCKS; i++) {
		if (ch->cac_backmap[i].cao_ino != 0)
			used += 1;
	}

	KASSERT(used == ch->cac_blocks_used, ("expected %ld used blocks, found %ld", 
				ch->cac_blocks_used, used));
}
#else
static inline void
ca_checkused_unlocked(struct ca_chunk __unused *ch)
{
}

#endif /* INVARIANTS */

static inline void
ca_checkused(struct ca_chunk *ch)
{
	mtx_lock(&ch->cac_mtx);
	ca_checkused_unlocked(ch);
	mtx_unlock(&ch->cac_mtx);
}

static inline void
ca_checkstate(struct ca_chunk *ch, enum ca_state state)
{
	if (ch->cac_state != state)
		panic("invalid chunk state %d, expected %d\n", ch->cac_state, state);
	ca_checkused(ch);
}

static inline void
ca_checkstate_unlocked(struct ca_chunk *ch, enum ca_state state)
{
	if (ch->cac_state != state)
		panic("invalid chunk state %d, expected %d\n", ch->cac_state, state);
	ca_checkused_unlocked(ch);
}

/*
 * Pop off the free list the chunk in index chind.
 */
static void
cac_from_free(struct chunkallocator *ca, size_t chind, struct ca_chunk **chp)
{
	struct ca_chunk *ch;

	mtx_assert(&ca->ca_mtx, MA_OWNED);
	KASSERT(chind < ca->ca_free_cnt, ("removing invalid chind %ld (currently %ld free)",
				chind, ca->ca_free_cnt));

	ch = ca->ca_free[chind];

	ca_checkstate(ch, CA_FREE);
	ch->cac_state = CA_NOQUEUE;

	ca->ca_free[chind] = ca->ca_free[ca->ca_free_cnt - 1];
	ca->ca_free_cnt -= 1;


	*chp = ch;

	ca_checkused(ch);
}

static void
cac_free_pop(struct chunkallocator *ca, struct ca_chunk **chp)
{
	int i;

	if (ca->ca_free_cnt == 0) 
		panic("out of space");

	/* Find the first completely free block. */
	for (i = ca->ca_free_cnt - 1; i >= 0; i--) {
		if (ca->ca_free[i]->cac_alloc_index == 0) {
			CA_COUNTER_INCREMENT(ca, pop_from_free);
			return (cac_from_free(ca, i, chp));
		}
	}

	/* No completely free block. */
	panic("out of space");
}

static void
cac_to_free(struct chunkallocator *ca, struct ca_chunk *ch)
{
	int i;

	KASSERT(ch->cac_blocks_used == 0, ("freeing non-empty chunk %ld", ch->cac_blocks_used));
	mtx_assert(&ca->ca_mtx, MA_OWNED);

	for (i = ch->cac_alloc_index; i < CA_BLOCKS; i++) {
		KASSERT(ch->cac_backmap[i].cao_ino == 0, 
			("freeing chunk with dirty backmap index %d (value %d)",
			 i, ch->cac_backmap[i].cao_ino));
	}

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

	ca_checkused(ch);

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

#if 0
	bucket = min((ch->cac_blocks_used * 4) / CA_BLOCKS, CA_COLD_BUCKETS - 1);
#endif
	bucket = 0;
	KASSERT(bucket >= 0, ("negative bucket"));
	KASSERT(bucket < CA_COLD_BUCKETS, ("bucket offset %d too large", bucket));

	_Static_assert(CA_COLD_BUCKETS == 1, "using priority queue for cold chunks");
	TAILQ_INSERT_TAIL(&ca->ca_cold[bucket], ch, cac_next);

	ch->cac_cold_bucket = bucket;
	ca->ca_cold_cnt[bucket] += 1;
	KASSERT(ca->ca_cold_cnt[bucket] < ca->ca_chunk_cnt, ("cold list index overflow"));

}

static void
cac_from_cold(struct chunkallocator *ca, struct ca_chunk *ch)
{
	mtx_assert(&ca->ca_mtx, MA_OWNED);

	/* 
	 * XXX We temporarily use a single list for cold chunks instead of a priority
	 * queue. The checks below prevent us from using it as the latter and can be
	 * removed once we implement them.
	 */
	_Static_assert(CA_COLD_BUCKETS == 1, "using priority queue for cold chunks");
	KASSERT(ch->cac_cold_bucket == 0, ("passing nonzero bucket"));

	TAILQ_REMOVE(&ca->ca_cold[ch->cac_cold_bucket], ch, cac_next);
	ca->ca_cold_cnt[ch->cac_cold_bucket] -= 1;

	ca_checkstate(ch, CA_COLD);
	ch->cac_state = CA_NOQUEUE;

	ch->cac_cold_bucket = CA_NOBUCKET;
}

static void
ca_init_chunks(struct chunkallocator *ca)
{
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
		ch->cac_cold_bucket = CA_NOBUCKET;

		bzero(ch->cac_backmap, sizeof(ch->cac_backmap[0]) * CA_BLOCKS);
		bzero(ch->cac_launder, sizeof(ch->cac_launder[0]) * CA_BLOCKS);

		ptr.offset = ca_chind_to_offset(ca, i); 
		ptr.size = CA_CHUNKSZ / BLOCKSIZE;
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

	ca->ca_startoff = startoff;

	/* All chunks in the allocator. */
	diskbytes = numblocks * BLOCKSIZE;
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

	for (i = 0; i < CA_FREESLOTS; i++)
		mtx_init(&ca->ca_slot_mtx[i], "objslotmtx", NULL, MTX_DEF);

	for (i = 0; i < CA_COLD_BUCKETS; i++) {
		ca->ca_cold_cnt[i] = 0;
		TAILQ_INIT(&ca->ca_cold[i]);
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
	ca_init_chunks(ca);
	for (i = 0; i < ca->ca_chunk_cnt; i++)
		cac_to_free(ca, &ca->ca_chunks[i]);
	mtx_unlock(&ca->ca_mtx);
}

void
ca_destroy(struct chunkallocator *ca)
{
	vm_page_t m;
	int i, j;

	for (i = 0; i < CA_FREESLOTS; i++)
		mtx_destroy(&ca->ca_slot_mtx[i]);

	for (i = 0; i < ca->ca_chunk_cnt; i++)  {
		for (j = 0; j < CA_BLOCKS; j++) {
			m = ca->ca_chunks[i].cac_launder[j];
#if 0
			if (m != NULL)
				vm_page_free(m);
#endif
		}
		mtx_destroy(&ca->ca_chunks[i].cac_mtx);
	}

	free(ca->ca_chunks, M_CHUNKALLOC);
	free(ca->ca_free, M_CHUNKALLOC);
	free(ca->ca_hot, M_CHUNKALLOC);

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
	uint64_t queues[CA_STATES];
	uint64_t denom, nom;
	struct ca_chunk *ch;
	int i;

	printf("===== CHUNK ALLOCATOR STATS =====\n");
	if (ca->ca_chunk_cnt == 0) {
		printf("Allocator uninitialized, exiting.\n");
		return;
	}
	printf("Chunks: %ld\n", ca->ca_chunk_cnt);
	printf("Blocks per chunk: %d\n", CA_BLOCKS);
	printf("Hot list size in chunks: %ld\n", (ca->ca_chunk_cnt + ca->ca_hot_end - ca->ca_hot_start) % ca->ca_chunk_cnt);
	printf("Launder list size: %ld\n", ca->ca_launder_cnt);
	printf("Free list size in chunks : %ld\n", ca->ca_free_cnt);

	nom = denom = 0;
	for (i = 0; i < ca->ca_chunk_cnt; i++) {
		if (ca->ca_chunks[i].cac_state == CA_NOQUEUE) {
			ca_checkused(&ca->ca_chunks[i]);
			nom += ca->ca_chunks[i].cac_blocks_used;
			denom += 1;
		}
	}
	printf("Chunks without a queue: %ld (total load %ld, average load %ld)\n", denom, nom, denom ? nom / denom : denom);

	printf("Chunk size in bytes : %ld\n", CA_CHUNKSZ);
	printf("Chunks Popped Off of [FREE]: %ld\n", CA_COUNTER(ca, pop_from_free));
	printf("[FREE] Chunks Sent to [HOT]: %ld\n", CA_COUNTER(ca, free_to_hot));
	printf("[FREE] Chunks Sent to [LAUNDER]: %ld\n", CA_COUNTER(ca, free_to_launder));
	printf("[HOT] Chunks Sent to [FREE]: %ld\n", CA_COUNTER(ca, hot_to_free));
	printf("[HOT] Chunks Sent To [COLD]: %ld\n", CA_COUNTER(ca, hot_to_cold));
	printf("[HOT] Chunks Sent To [LAUNDER]: %ld\n", CA_COUNTER(ca, hot_to_launder));
	printf("[COLD] Chunks Sent to [LAUNDER]: %ld\n", CA_COUNTER(ca, cold_to_launder));
	printf("[LAUNDER] Chunks Sent to [COLD]: %ld\n", CA_COUNTER(ca, launder_to_cold));
	printf("[FREE] Chunks Sent To [SYSTEM]: %ld\n", CA_COUNTER(ca, free_to_system));
	printf("[SYSTEM] Chunks Sent To [SYSTEM-FULL]: %ld\n", CA_COUNTER(ca, system_to_full));
	printf("[SYSTEM] Chunks Reclaimed From [SYSTEM-FULL]: %ld\n", CA_COUNTER(ca, full_to_system));
	printf("Chunks Reclaimed To [FREE]: %ld\n", CA_COUNTER(ca, reclaimed_to_free));
	printf("[COLD] Chunks Sent To [FREE]: %ld\n", CA_COUNTER(ca, cold_to_free));
	printf("Aging operations: %ld\n", CA_COUNTER(ca, op_aging));
	printf("Free operations: %ld\n", CA_COUNTER(ca, op_free));
	printf("Chunk move operations: %ld\n", CA_COUNTER(ca, op_move));
	printf("Aging-related IO operations: %ld\n", CA_COUNTER(ca, op_age_io));
	printf("Failed GC operations: %ld\n", CA_COUNTER(ca, op_gc_failed));
	for (i = 0; i < CA_FREESLOTS; i++)
		printf("Fast alloc (SLOT %d) operations: %ld\n", i, CA_COUNTER(ca, op_alloc_fast[i]));
	printf("Failed fast allocation operations: %ld\n", CA_COUNTER(ca, op_alloc_fast_fail));

	denom = CA_COUNTER(ca, op_move);
	printf("Pages Moved: %ld (avg %ld)\n", CA_COUNTER(ca, page_moves), denom ? CA_COUNTER(ca, page_moves) / denom : -1);

	denom = CA_COUNTER(ca, hot_to_free) + CA_COUNTER(ca, hot_to_launder) + CA_COUNTER(ca, hot_to_cold);
	printf("Avg Blocks Used in [HOT]: %ld\n", denom ? CA_COUNTER(ca, hot_blocks_used) / denom : -1);

	nom = denom = 0;
	for (i = 0; i < CA_FREESLOTS; i++) {
		nom += CA_COUNTER(ca, op_alloc_fast[i]);
		denom += CA_COUNTER(ca, page_alloc_fast[i]);
	}
	printf("Pages (TXN) Allocated: %ld (avg %ld)\n", nom, denom ? nom / denom : -1);
	printf("Fast allocations Allocated: %ld (avg %ld)\n", nom, denom ? nom / denom : -1);
	printf("Total data pages allocated: %ld\n", CA_COUNTER(ca, page_alloc));
	printf("Total data pages freed: %ld\n", CA_COUNTER(ca, page_free));

	/* Find out how many chunks without a queue are empty. */
	nom = 0;
	for (i = 0; i < ca->ca_chunk_cnt; i++) {
		if ((ca->ca_chunks[i].cac_state == CA_NOQUEUE) && (ca->ca_chunks[i].cac_blocks_used == 0))
			nom += 1;
	}
	printf("Found %ld empty chunks without queue\n", nom);

	/* Print queue lengths. */
	for (i = 0; i < CA_STATES; i++)
		queues[i] = 0;

	for (i = 0; i < ca->ca_chunk_cnt; i++)
		queues[ca->ca_chunks[i].cac_state] += 1;

	for (i = 0; i < CA_STATES; i++)
		printf("Queue %d has size %ld\n", i, queues[i]);

	nom = denom = 0;
	for (i = 0; i < CA_COLD_BUCKETS; i++) {
		TAILQ_FOREACH(ch, &ca->ca_cold[i], cac_next) {
			nom += ch->cac_blocks_used;
			denom += 1;
		}
	}
	printf("Cold chunks: %ld (total load %ld, average load %ld)\n", denom, nom, denom ? nom / denom : denom);

	printf("===== STATS END =====\n");


}

/* ===== Garbage collection path. ===== */

struct ca_hot_to_launder_args {
	struct task tk;
	struct chunkallocator *ca;
	struct ca_chunk *ch;
};

static void
ca_hot_to_launder(void *ctx, int __unused pending)
{
	struct ca_hot_to_launder_args *args = (struct ca_hot_to_launder_args *)ctx;
	struct chunkallocator *ca = args->ca;
	struct ca_chunk *ch = args->ch;
	int i;
#if 0
	vm_page_t m;
	struct bio *bp;
	int error;
#endif

	SDT_PROBE0(objsnap, , , chunk_launder_start);
	for (i = 0; i < CA_BLOCKS; i++) {
		if (ch->cac_backmap[i].cao_ino == 0)
			continue;
		
#if 0
		m = vm_page_alloc_freelist(VM_FREELIST_DEFAULT, VM_ALLOC_NORMAL | VM_ALLOC_NOOBJ);

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
#endif

		ch->cac_launder[i] = hackpage;
	}

	/* We hold the only reference to the chunk, since it has no queue. */
	mtx_lock(&ca->ca_mtx);
	mtx_lock(&ch->cac_mtx);

	ch->cac_alloc_index = 0;

	for (i = 0; i < CA_BLOCKS; i++) {
		if (ch->cac_launder[i] != NULL && ch->cac_backmap[i].cao_ino == 0) {
#if 0
			vm_page_free(ch->cac_launder[i]);
#endif
			ch->cac_launder[i] = NULL;
		}

		KASSERT(((ch->cac_backmap[i].cao_ino == 0) || (ch->cac_launder[i] != NULL)),
				("no launder page %p for allocation in chunk block (inode) %d",
				 ch->cac_launder[i], ch->cac_backmap[i].cao_ino));
	}

	ca_checkstate_unlocked(ch, CA_NOQUEUE);
	ch->cac_state = CA_LAUNDER;

	TAILQ_INSERT_TAIL(&ca->ca_launder, ch, cac_next);
	ca->ca_launder_cnt += 1;

	mtx_unlock(&ch->cac_mtx);
	mtx_unlock(&ca->ca_mtx);

	CA_COUNTER_INCREMENT(ca, op_age_io);

	SDT_PROBE0(objsnap, , , chunk_launder_finish);
	free(ctx, M_OBJSNAP);
}

static void
ca_age(struct chunkallocator *ca)
{
	struct ca_chunk *chhot[CA_MAXHOT_TO_COLD];
	struct ca_hot_to_launder_args *args;
	struct ca_chunk *ch;
	uint64_t chind;
	int i;

	MPASS(ca != NULL);
	mtx_lock(&ca->ca_mtx);

	/* Is the hot list empty? */
	if (ca->ca_hot_start == ca->ca_hot_end) {
		mtx_unlock(&ca->ca_mtx);
		return;
	}

	CA_COUNTER_INCREMENT(ca, op_aging);

	for (chind = 0; chind < CA_MAXHOT_TO_COLD; chind++) {
		cac_from_hot(ca, &chhot[chind]);
		if (chhot[chind] == NULL)
			break;
	}

	for (i = 0; i < chind; i++) {
		ch = chhot[i];
		ca_checkstate(ch, CA_NOQUEUE);
		MPASS(ch != NULL);

		CA_COUNTER_ADD(ca, hot_blocks_used, ch->cac_blocks_used);

		if (ch->cac_blocks_used == 0) {
			CA_COUNTER_INCREMENT(ca, hot_to_free);
			ch->cac_alloc_index = 0;
			cac_to_free(ca, ch);
			continue;
		}

#if 0
		if (ch->cac_blocks_used >= CA_COLD_LOAD_THRESHOLD) {
			CA_COUNTER_INCREMENT(ca, hot_to_cold);
			cac_to_cold(ca, ch);
			continue;
		}
#endif

		args = malloc(sizeof(*args), M_OBJSNAP, M_NOWAIT);
		if (args == NULL)
			panic("out of memory");

		args->ca = ca;
		args->ch = ch;
		TASK_INIT(&args->tk, 0, ca_hot_to_launder, &args->tk);

		MPASS(osdata.os_tq != NULL);
		taskqueue_enqueue(osdata.os_tq, &args->tk);

		CA_COUNTER_INCREMENT(ca, hot_to_launder);
	}

	mtx_unlock(&ca->ca_mtx);
}

static void
ca_blkalloc(struct ca_chunk *ch, struct objsnap_txn *txn, struct chunkallocator *ca)
{
	const size_t numblocks = txn->d_cnt;
	struct ca_objid *backmap;
	int ind, i;

	mtx_assert(&ch->cac_mtx, MA_OWNED);
	KASSERT(ch->cac_alloc_index + numblocks <= CA_BLOCKS, ("chunk cannot satisfy allocation"));

	/* Scan all sectors till we find a free one. */
	for (i = 0; i < numblocks ; i++) {
		MPASS(txn != NULL);
		ind = ch->cac_alloc_index + i; 
		backmap = &ch->cac_backmap[ind];

		KASSERT(txn->d_inode[i] != 0, ("allocating block for inode 0"));
		KASSERT(backmap->cao_ino == 0, ("block already allocated"));
		backmap->cao_ino = txn->d_inode[i];
		backmap->cao_off = txn->d_index[i];
	}

	txn->d_ptr.offset = ch->cac_ptr.offset + ch->cac_alloc_index;
	txn->d_ptr.size = numblocks;
	
	ch->cac_blocks_used += numblocks;
	ch->cac_alloc_index += numblocks;
	ca_checkused_unlocked(ch);

	atomic_add_64(&CA_COUNTER(ca, page_alloc), numblocks);
}

static __attribute__((noinline)) void
ca_gc_alloc(struct chunkallocator *ca, struct objsnap_txn *txn)
{
	/* 
	 * Pop the required physical space from the soon-to-be cold chunk.
	 * If full, move it to the cold list and allocate a new one. 
	 */

	mtx_lock(&ca->ca_mtx);

	if (ca->ca_launder_dst != NULL && ca->ca_launder_dst->cac_alloc_index + txn->d_cnt > CA_BLOCKS) {
		CA_COUNTER_INCREMENT(ca, launder_to_cold);
		cac_to_cold(ca, ca->ca_launder_dst);
		ca->ca_launder_dst = NULL;
	}

	if (ca->ca_launder_dst == NULL) {
		CA_COUNTER_INCREMENT(ca, free_to_launder);
		cac_free_pop(ca, &ca->ca_launder_dst);
	}

	KASSERT(ca->ca_launder_dst->cac_alloc_index < CA_BLOCKS, ("invalid index %ld", ca->ca_launder_dst->cac_alloc_index));
	mtx_lock(&ca->ca_launder_dst->cac_mtx);
	ca_blkalloc(ca->ca_launder_dst, txn, ca);
	mtx_unlock(&ca->ca_launder_dst->cac_mtx);

	mtx_unlock(&ca->ca_mtx);
}


static __attribute__((noinline)) void
ca_gc_move(struct chunkallocator *ca, struct ca_chunk *ch, size_t numblocks)
{
	struct objsnap_txn txn;
	vm_page_t ma[MAXDRTYCNT];
	size_t cnt;
	size_t i;

	//bzero(&txn, sizeof(txn));
	txn.d_cnt = cnt = 0;

	KASSERT(numblocks <= MAXDRTYCNT, ("too many blocks to move: %ld", numblocks));

	mtx_lock(&ch->cac_mtx);
	for (i = ch->cac_alloc_index; i < CA_BLOCKS; i++) {
		KASSERT(txn.d_cnt <= MAXDRTYCNT, ("transaction count overflow %d", txn.d_cnt));
		if (ch->cac_backmap[i].cao_ino == 0) {
			/* Did the block get freed while we were in the laundry list? */
			if (ch->cac_launder[i] != NULL) {
#if 0
				vm_page_free(ch->cac_launder[i]);
#endif
				ch->cac_launder[i] = NULL;
			}
			continue;
		}

		/* Populate the transaction with the page and remove it from the chunk. */
		KASSERT(ch->cac_launder[i] != NULL, ("no laundered page for (%d, %ld %p %p)",
					ch->cac_index, i, &ch->cac_launder[i], ch->cac_launder[i]));

		/* Count and page list saved for later. */
		ma[cnt++] = ch->cac_launder[i];

		txn.d_page[txn.d_cnt] = ch->cac_launder[i];
		txn.d_inode[txn.d_cnt] = ch->cac_backmap[i].cao_ino;
		txn.d_index[txn.d_cnt] = ch->cac_backmap[i].cao_off;

		ch->cac_launder[i] = NULL;

		txn.d_cnt += 1;
		if (txn.d_cnt >= numblocks)
			break;
	}

	mtx_unlock(&ch->cac_mtx);

	ca_gc_alloc(ca, &txn);

	CA_COUNTER_ADD(ca, page_moves, txn.d_cnt);

	objsnap_txn_commit(&txn, 0, false);

#if 0
	for (i = 0; i < cnt; i++) {
		vm_page_free(ma[i]);
	}
#endif

	CA_COUNTER_INCREMENT(ca, op_move);
}

void
ca_gc(struct chunkallocator *ca, size_t numblocks)
{
	struct ca_chunk *ch;

	mtx_lock(&ca->ca_mtx);
	ch = TAILQ_FIRST(&ca->ca_launder);

	/* Is there anything to launder in the first place? */
	if (ch == NULL) {
		mtx_unlock(&ca->ca_mtx);
		CA_COUNTER_INCREMENT(ca, op_gc_failed);
		return;
	}


	/* 
	 * Remove the chunk from all queues, ca_free() will put it back
	 * in the free queue when it's ready.
	 */
	TAILQ_REMOVE(&ca->ca_launder, ch, cac_next);
	ca_checkstate(ch, CA_LAUNDER);
	ch->cac_state = CA_NOQUEUE;

	ca->ca_launder_cnt -= 1;
	mtx_unlock(&ca->ca_mtx);

	ca_gc_move(ca, ch, numblocks);
}

/* ===== Main allocation path. ===== */

static bool
ca_tryalloc_fastpath(struct chunkallocator *ca, int ind, struct objsnap_txn *txn)
{
	struct ca_chunk *ch = ca->ca_free_slots[ind];

	KASSERT(ind >= 0, ("negative slot index"));
	KASSERT(ind < CA_FREESLOTS, ("slot index too large"));

	mtx_lock(&ca->ca_slot_mtx[ind]);
	if ((ch == NULL) || (ch->cac_alloc_index + txn->d_cnt > CA_BLOCKS)) {
		mtx_unlock(&ca->ca_slot_mtx[ind]);
		return (false);
	}

	mtx_lock(&ch->cac_mtx);
	ca_blkalloc(ch, txn, ca);
	mtx_unlock(&ch->cac_mtx);
	CA_COUNTER_INCREMENT(ca, op_alloc_fast[ind]);
	CA_COUNTER_ADD(ca, page_alloc_fast[ind], txn->d_cnt);

	mtx_unlock(&ca->ca_slot_mtx[ind]);

	return (true);
}

static void
ca_tryalloc_fastpath_fix(struct chunkallocator *ca, int ind)
{
	struct ca_chunk **chp = &ca->ca_free_slots[ind];
	struct ca_chunk *ch;

	mtx_lock(&ca->ca_mtx);
	mtx_lock(&ca->ca_slot_mtx[ind]);

	CA_COUNTER_INCREMENT(ca, op_alloc_fast_fail);

	if (*chp != NULL)
		cac_to_hot(ca, *chp);

	cac_free_pop(ca, chp);
	ch = *chp;

	KASSERT(ca_offset_to_chind(ca, ch->cac_ptr.offset) == ch->cac_index,
			("chunk pointer inconsistent with its index: "
			 "has %d while pointer is for %d\n", ch->cac_index,
			 ca_offset_to_chind(ca, ch->cac_ptr.offset)));

	mtx_unlock(&ca->ca_slot_mtx[ind]);
	mtx_unlock(&ca->ca_mtx);
}

static int
ca_tryalloc_txn(struct chunkallocator *ca, int ind, struct objsnap_txn *txn)
{
	const size_t numblocks = txn->d_cnt;
	size_t hot_cnt, hot_threshold;

	MPASS(ca != NULL);
	if (numblocks > CA_BLOCKS)
		panic("requested allocation too large (%ld, max %d)\n", numblocks, CA_BLOCKS);

	hot_cnt = (ca->ca_chunk_cnt + ca->ca_hot_end - ca->ca_hot_start) % ca->ca_chunk_cnt;
	hot_threshold = ca->ca_chunk_cnt * CA_HOT_CHUNKS_PERCENT / 100;
	/* Age as many hot blocks as we are allocating. */
	if (hot_cnt >= hot_threshold)
		ca_age(ca);

	if (ca->ca_free_cnt == 0) {
		ca_print(ca);
		panic("allocator full");
	}

	if (ca_tryalloc_fastpath(ca, ind, txn))
		return (0);

	ca_tryalloc_fastpath_fix(ca, ind);

	return (EAGAIN);
}

int
ca_alloc_txn(struct chunkallocator *ca, int tid, struct objsnap_txn *txn)
{
	int error;

	do {
		error = ca_tryalloc_txn(ca, tid % CA_FREESLOTS, txn);
	} while (error != 0);

	return (0);
}

/* ===== System allocation path. This is a self-contained allocator. ===== */

static void
ca_blkalloc_system(struct ca_chunk *ch, obj_diskptr_t *ptrp)
{
	obj_diskptr_t ptr;
	int i;

	mtx_lock(&ch->cac_mtx);

	for (i = 0; i < CA_BLOCKS; i++) {
		if (ch->cac_backmap[i].cao_ino == 0)
			break;

		KASSERT(ch->cac_backmap[i].cao_ino == CA_SYSTEM_INO,
				("corrupted system backmap for (%d, %d), got (%d, %d) instead",
				 ch->cac_ptr.offset + i,
				 1,
				 ch->cac_backmap[i].cao_ino,
				 ch->cac_backmap[i].cao_off));
	}

	KASSERT(i < CA_BLOCKS, ("system chunk had no free block"));

	ch->cac_backmap[i].cao_ino = CA_SYSTEM_INO;
	ch->cac_backmap[i].cao_off = CA_SYSTEM_INO;

	ptr.offset = ch->cac_ptr.offset + i;
	ptr.size = 1;

	ch->cac_blocks_used += 1;
	mtx_unlock(&ch->cac_mtx);
	 
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
	int i;

	/* If we have a system */
	mtx_lock(&ca->ca_mtx);

	/* If necessary, pop a free block off the main allocator and into the system allocator. */
	if (TAILQ_EMPTY(&ca->ca_system_alloc)) {
		cac_free_pop(ca, &ch);
		CA_COUNTER_INCREMENT(ca, free_to_system);
		for (i = 0; i < CA_BLOCKS; i++) {
			KASSERT(ch->cac_backmap[i].cao_ino == 0, ("block has set backmap values "
						"(%d, %d)",
						ch->cac_backmap[i].cao_ino,
						ch->cac_backmap[i].cao_off));
		}

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
		CA_COUNTER_INCREMENT(ca, system_to_full);
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
	int chind = ca_offset_to_chind(ca, ptr.offset);
	struct ca_chunk *ch;
	int choff, ind, i;

	KASSERT(ptr.size != UINT_MAX, ("freeing invalid disk pointer"));
	KASSERT(chind < ca->ca_chunk_cnt, ("freeing out-of-bounds chunk %d %ld %d", chind, ca->ca_chunk_cnt, ptr.offset));
	KASSERT(ptr.size > 0, ("freeing empty disk pointer"));

	ch = &ca->ca_chunks[chind];
	mtx_lock(&ch->cac_mtx);

	KASSERT(ptr.offset >= ch->cac_ptr.offset, ("negative index into chunk"));
	choff = ptr.offset - ch->cac_ptr.offset;
	KASSERT(choff < CA_BLOCKS, ("invalid block offset %d in chunk %d\n", choff, ch->cac_index));

	for (i = 0; i < ptr.size; i++) {
		ind = choff + i;
		KASSERT(ind < ch->cac_ptr.size, ("freeing out of bounds for chunk"));
		KASSERT(ind < CA_BLOCKS, ("index larger than maximum possible offset"));

		KASSERT(ch->cac_backmap[ind].cao_ino != 0, ("freeing already free block %d", ptr.offset));
		ch->cac_backmap[ind].cao_ino = 0;
		ch->cac_backmap[ind].cao_off = 0;

		/* No need to clean the page anymore. */
		if (ch->cac_launder[ind] != 0) {
#if 0
			vm_page_free(ch->cac_launder[ind]);
#endif
			ch->cac_launder[ind] = NULL;
		}
	}

	KASSERT(ch->cac_blocks_used >= ptr.size, ("blocks used counter underflow"));
	ch->cac_blocks_used -= ptr.size;

	/* Special case for the system block allocator. */
	if (ch->cac_state == CA_SYSTEM_FULL) {
		cac_from_system_full(ca, ch);
		cac_to_system(ca, ch);
		CA_COUNTER_INCREMENT(ca, full_to_system);
	} 

	if (ch->cac_state != CA_SYSTEM && ch->cac_state != CA_SYSTEM_FULL)
		atomic_add_64(&CA_COUNTER(ca, page_free), ptr.size);

	CA_COUNTER_INCREMENT(ca, op_free);
	/* XXX If the block is cold, then we should adjust which bucket it is in. */

	mtx_unlock(&ch->cac_mtx);

	if (ch->cac_blocks_used != 0)
		return;

	ca_checkused(ch);
	ch->cac_alloc_index = 0;

	mtx_lock(&ca->ca_mtx);

	switch(ch->cac_state) {
	case CA_NOQUEUE:
		CA_COUNTER_INCREMENT(ca, reclaimed_to_free);
		cac_to_free(ca, ch);
		break;

	case CA_COLD:
		CA_COUNTER_INCREMENT(ca, cold_to_free);
		cac_from_cold(ca, ch);
		cac_to_free(ca, ch);
		break;

	default:
		printf("Found empty block that belongs to list %d\n", ch->cac_state);
	}

	mtx_unlock(&ca->ca_mtx);
}
