#include <sys/param.h>
#include <sys/queue.h>
#include <sys/kernel.h>
#include <sys/lock.h>
#include <sys/mutex.h>
#include <sys/taskqueue.h>

#include "arraylist.h"
#include "chunkalloc.h"
#include "objsnap_internal.h"

#define CA_TXNSIZE (64)

MALLOC_DEFINE(M_CHUNKALLOC, "Chunk allocator", "chunkalloc");

static void *
ca_alloc_chunkarray(size_t size)
{
	return (mallocarray(sizeof(struct ca_chunk), size,
		M_CHUNKALLOC, M_WAITOK | M_ZERO));
}

void
ca_init(struct chunkallocator *ca)
{
	size_t diskbytes;

	bzero(ca, sizeof(*ca));
	mtx_init(&ca->ca_mtx, "objcamtx", NULL, MTX_DEF);

	ca->ca_startoff = 0;
	ca->ca_txnsz_blk = CA_TXNSIZE;

	/* All chunks in the allocator. */
	diskbytes = superblock.super_size * superblock.super_bsize;
	ca->ca_chunk_cnt = diskbytes / CA_CHUNKSZ;
	ca->ca_chunks = ca_alloc_chunkarray(ca->ca_chunk_cnt);

	ca->ca_next_cnt = ca->ca_chunk_cnt;
	ca->ca_next = ca_alloc_chunkarray(ca->ca_chunk_cnt);

	/* Old (XXX already/partially used?) chunks. */
	ca->ca_old_cnt = 0;
	ca->ca_old = ca_alloc_chunkarray(ca->ca_chunk_cnt);

	/* Candidate chunks. XXX What does "candidate" mean here? */
	ca->ca_cand_cnt = determine_bucket(ca->ca_txnsz_blk) + 1;
	ca->ca_cand = ca_alloc_chunkarray(ca->ca_cand_cnt);
	ca->ca_cand_old = ca_alloc_chunkarray(ca->ca_cand_cnt);

	/* XXX Initialize thread worklist state */

	/* XXX Create the background thread */

	/* XXX Mark all chunks as freed */
}

int
ca_alloc(struct chunkallocator *ca, int numblocks, diskptr_t *ptr)
{
	return (EOPNOTSUPP);
}

void
ca_free(struct chunkallocator *ca, diskptr_t tofree)
{
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
}
