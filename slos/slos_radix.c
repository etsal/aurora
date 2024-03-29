#include <sys/param.h>
#include <sys/bio.h>
#include <sys/lock.h>
#include <sys/malloc.h>
#include <sys/mount.h>
#include <sys/rwlock.h>
#include <sys/stat.h>

#include <vm/uma.h>

#include <slos.h>
#include <slsfs.h>

#include "slos_alloc.h"
#include "slos_inode.h"
#include "slos_radix.h"

static uma_zone_t slos_rdxtree_zone;
static uma_zone_t slos_rdxnode_zone;

#define SLOS_RDXTREE_WARM (4096)
#define SLOS_RDXNODE_WARM (4096)

#define STREE_SYNC_DBG(...) \
	do {                \
	} while (0)
#define STREE_ITER_DBG(...) \
	do {                \
	} while (0)
#define STREE_DBG(...) \
	do {           \
	} while (0)
#define SRDX_DBG(...) \
	do {          \
	} while (0)

MALLOC_DEFINE(M_SRDX, "slosradix", "SLOS radix tree");

/*
 * XXX Make this arbitrarily large (up to 64 bits). Right now all
 * trees have the same height, even if they don't need it.
 */
#define STREE_DEPTH (5)

static int
slos_radix_rdxtree_init(void *mem, int size, int flags __unused)
{
	struct slos_rdxtree *stree = (struct slos_rdxtree *)mem;

	stree->stree_srdxcap = SLOS_BSIZE(slos) / sizeof(diskblk_t);
	stree->stree_max = 1ULL
	    << ((fls(stree->stree_srdxcap) - 1) * STREE_DEPTH);
	stree->stree_mask = (1ULL << (fls(stree->stree_srdxcap) - 1)) - 1;

	return (0);
}

static void
slos_radix_rdxtree_fini(void *mem, int size)
{
}

/* XXX Have this function be debug only. */
static void
slos_radix_rdxtree_dtor(void *mem, int size, void *arg __unused)
{
	struct slos_rdxtree *stree = (struct slos_rdxtree *)mem;
	stree->stree_root = 0;
	stree->stree_vp = NULL;
}

static int
slos_radix_rdxnode_init(void *mem, int size, int flags __unused)
{
	return (0);
}

static void
slos_radix_rdxnode_fini(void *mem, int size)
{
}

static int
slos_radix_rdxnode_ctor(void *mem, int size, void *arg, int flags __unused)
{
	struct slos_rdxnode *srdx = (struct slos_rdxnode *)mem;
	struct buf *bp = (struct buf *)arg;

	/* Mappings into the buffer. */
	srdx->srdx_vals = (diskblk_t *)bp->b_data;

	/* Backpointer to the buffer. */
	srdx->srdx_buf = bp;
	srdx->srdx_key = 0;

	return (0);
}

/* XXX Make this debug only. */
static void
slos_radix_rdxnode_dtor(void *mem, int size, void *arg __unused)
{
	struct slos_rdxnode *srdx = (struct slos_rdxnode *)mem;
	bzero(srdx, sizeof(*srdx));
}

int
slos_radix_init(void)
{
	/* The tree zone. */
	slos_rdxtree_zone = uma_zcreate("slos_rdxtree",
	    sizeof(struct slos_rdxtree), NULL, slos_radix_rdxtree_dtor,
	    slos_radix_rdxtree_init, slos_radix_rdxtree_fini,
	    UMA_ALIGNOF(struct slos_rdxtree), 0);
	if (slos_rdxtree_zone == NULL)
		return (ENOMEM);

	// uma_prealloc(slos_rdxtree_zone, SLOS_RDXTREE_WARM);

	/* The zone for individual nodes. */
	slos_rdxnode_zone = uma_zcreate("slos_rdxnode",
	    sizeof(struct slos_rdxnode), slos_radix_rdxnode_ctor,
	    slos_radix_rdxnode_dtor, slos_radix_rdxnode_init,
	    slos_radix_rdxnode_fini, UMA_ALIGNOF(struct slos_rdxnode), 0);
	if (slos_rdxnode_zone == NULL) {
		uma_zdestroy(slos_rdxnode_zone);
		return (ENOMEM);
	}

	// uma_prealloc(slos_rdxnode_zone, SLOS_RDXNODE_WARM);

	return (0);
}

void
slos_radix_fini(void)
{
	if (slos_rdxtree_zone != NULL) {
		uma_zdestroy(slos_rdxtree_zone);
		slos_rdxtree_zone = NULL;
	}

	if (slos_rdxnode_zone != NULL) {
		uma_zdestroy(slos_rdxnode_zone);
		slos_rdxnode_zone = NULL;
	}
}

static inline uint64_t
stree_localkey(struct slos_rdxtree *stree, uint64_t key, int depth)
{
	uint64_t movbits = (STREE_DEPTH - 1 - depth) * fls(stree->stree_mask);

	return ((key >> movbits) & stree->stree_mask);
}

/*
 * ============ Node management operations. ============
 */

/*
 * Retrieve a radix tree node from the disk.
 */
static int
srdx_retrieve(struct slos_rdxtree *stree, daddr_t lblkno, bool dirty,
    struct slos_rdxnode **srdxp)
{
	struct vnode *vp = stree->stree_vp;
	struct thread *td = curthread;
	struct slos_rdxnode *srdx;
	struct buf *bp;
	int rv;

	ASSERT_VOP_LOCKED(stree->stree_vp, "slosradix");
	KASSERT(lblkno != 0, ("requesting to read at offset 0"));

	bp = getblk(vp, lblkno, BLKSIZE(&slos), 0, 0, 0);
	if (bp == NULL)
		return (EIO);

	if ((bp->b_flags & B_INVAL) != 0 || (bp->b_flags & B_CACHE) == 0) {
		/*
		 * SLOS specific configuration (the only reason we are
		 * not doing a bread() outright).
		 */
		KASSERT(BP_SRDX_GET(bp) == NULL,
		    ("buffer already has backing srdx node"));
		bp->b_flags |= B_MANAGED | B_CLUSTEROK;

		STREE_SYNC_DBG("[RETRIEVE] %p\n", bp);
		BP_UNSET_NEEDSCOW(bp);

		/* Initiate the read for the requested data. */
		td->td_ru.ru_inblock++;
		bp->b_iocmd = BIO_READ;
		bp->b_flags &= ~B_INVAL;
		bp->b_ioflags &= ~BIO_ERROR;
		bp->b_rcred = crhold(curthread->td_ucred);
		vfs_busy_pages(bp, 0);
		bp->b_iooffset = dbtob(bp->b_blkno);

		bstrategy(bp);

		/* Wait for the IO to be done. */
		rv = bufwait(bp);
		if (rv != 0) {
			KASSERT(BP_SRDX_GET(bp) == NULL,
			    ("half retrieved buffer has backing srdx node"));
			bp->b_flags &= ~B_MANAGED;
			brelse(bp);
			return (rv);
		}
	}

	/*
	 * Attach the in-memory node state to the buffer. The buffer cannot be
	 * invalidated, since it is manually managed, so the buffer is
	 * guaranteed to not be freed from under us, taking the slos_rdxnode
	 * with it.
	 */
	srdx = BP_SRDX_GET(bp);
	if (srdx == NULL) {
		srdx = uma_zalloc_arg(slos_rdxnode_zone, bp, M_NOWAIT);
		if (srdx == NULL) {
			bp->b_flags &= ~B_MANAGED;
			brelse(bp);
			return (ENOMEM);
		}
		BP_SRDX_SET(bp, srdx);
		bp->b_flags |= B_MANAGED | B_CLUSTEROK;
		SRDX_DBG("(SRDX) Created %p for buffer %p\n", srdx, bp);
		srdx->srdx_tree = stree;
	} else {
		KASSERT(srdx->srdx_buf == bp,
		    ("radix node %p has stale buffer pointer %p",
			srdx->srdx_buf, bp));
	}

	/*
	 * We have to dirty all nodes in the path we take to insert a key
	 * to properly apply COW during the sync operation. Call bdwrite()
	 * to mark the buffer as dirty.
	 */
	if (dirty) {
		bdwrite(bp);
		BUF_LOCK(bp, LK_EXCLUSIVE, 0);
	}

	SRDX_DBG("(SRDX) Retrieving %p (buffer %p)\n", srdx, bp);
	*srdxp = srdx;
	BUF_ASSERT_LOCKED(srdx->srdx_buf);

	return (0);
}

/*
 * Create an in-memory radix tree node from an on-disk one.
 */
static int
srdx_create(
    struct slos_rdxtree *stree, diskptr_t *ptrp, struct slos_rdxnode **srdxp)
{
	struct slos_rdxnode *srdx;
	struct buf *bp;
	diskptr_t ptr;
	int error;
	int i;

	ASSERT_VOP_LOCKED(stree->stree_vp, "slosradix");

	/* Eagerly allocate just for trees, since they are metadata. */
	ptr = *ptrp;
	if (ptr.offset == DISKPTR_NULL.offset) {
		error = slos_blkalloc(&slos, BLKSIZE(&slos), &ptr);
		if (error != 0)
			return (ENOSPC);
	}

	bp = getblk(stree->stree_vp, ptr.offset, BLKSIZE(&slos), 0, 0, 0);
	if (bp == NULL) {
		/* XXX We are losing the allocated disk block in this case. */
		return (EIO);
	}

	BUF_ASSERT_LOCKED(bp);

	bp->b_blkno = ptr.offset;
	bp->b_flags |= B_MANAGED | B_CLUSTEROK;

	STREE_SYNC_DBG("[CREATE] %p\n", bp);
	BP_UNSET_NEEDSCOW(bp);

	srdx = uma_zalloc_arg(slos_rdxnode_zone, bp, M_NOWAIT);
	if (srdx == NULL) {
		/* XXX Make sure this error handling is proper. */
		bp->b_flags &= ~B_MANAGED;
		brelse(bp);
		return (ENOMEM);
	}
	BP_SRDX_SET(bp, srdx);

	srdx->srdx_tree = stree;
	for (i = 0; i < stree->stree_srdxcap; i++)
		srdx->srdx_vals[i] = STREE_INVAL;

	/*
	 * XXX If we call bdwrite() we drop the lock, find a way to mark the
	 * buffer as dirty without losing ownership.
	 */
	bdwrite(bp);

	*ptrp = ptr;
	KASSERT(ptr.offset != 0, ("NULL radix node pointer"));
	*srdxp = srdx;

	SRDX_DBG("(SRDX) Created %p\n", srdx);
	BUF_LOCK(bp, LK_EXCLUSIVE, 0);

	return (0);
}

static void
srdx_release(struct slos_rdxnode *srdx)
{
	struct buf *bp = srdx->srdx_buf;

	SRDX_DBG("(SRDX) Releasing %p (buffer %p)\n", srdx, bp);
	KASSERT(
	    bp->b_flags & B_MANAGED, ("unmanaged buffer %p backing srdx", bp));
	brelse(bp);
}

static void
srdx_destroy(struct slos_rdxnode *srdx)
{
	struct buf *bp = srdx->srdx_buf;

	SRDX_DBG("(SRDX) Destroying %p (buffer %p)\n", srdx, bp);
	/* Disassociate the node from the buffer and free both of them. */
	KASSERT(!BP_NEEDSCOW(bp), ("destroying buffer %p in need of COW", bp));
	BP_SRDX_SET(bp, NULL);
	bp->b_flags &= ~B_MANAGED;

	uma_zfree(slos_rdxnode_zone, srdx);
	brelse(bp);
}

int
stree_init(struct vnode *vp, daddr_t daddr, struct slos_rdxtree **streep)
{
	struct slos_rdxtree *stree = uma_zalloc(slos_rdxtree_zone, M_WAITOK);
	diskptr_t ptr;

	stree->stree_vp = vp;
	stree->stree_root = daddr;

	ptr.offset = daddr;
	ptr.size = BLKSIZE(&slos);
	ptr.epoch = slos.slos_sb->sb_epoch;

	*streep = stree;
	return (0);
}

void
stree_destroy(struct slos_rdxtree *stree)
{
	struct bufobj *bo = &stree->stree_vp->v_bufobj;
	struct slos_rdxnode *srdx;
	struct buf *bp, *bptmp;

	VOP_LOCK(stree->stree_vp, LK_EXCLUSIVE);
	TAILQ_FOREACH_SAFE (bp, &bo->bo_dirty.bv_hd, b_bobufs, bptmp) {
		/*
		 * XXX What about flushing? I assume no due to checkpoint
		 * consistency.
		 */
		BUF_LOCK(bp, LK_EXCLUSIVE, 0);
		if (bp->b_flags & B_MANAGED) {
			bp->b_flags &= ~B_MANAGED;
			srdx = BP_SRDX_GET(bp);
			KASSERT(srdx != NULL,
			    ("managed dirty buffer %p has no node", bp));
			srdx_destroy(srdx);
		} else {
			srdx = BP_SRDX_GET(bp);
			KASSERT(srdx == NULL,
			    ("unmanaged dirty buffer has backing srdx node"));
			bremfree(bp);
			brelse(bp);
		}
	}

	TAILQ_FOREACH_SAFE (bp, &bo->bo_clean.bv_hd, b_bobufs, bptmp) {
		BUF_LOCK(bp, LK_EXCLUSIVE, 0);
		if (bp->b_flags & B_MANAGED) {
			bp->b_flags &= ~B_MANAGED;
			srdx = BP_SRDX_GET(bp);
			KASSERT(srdx != NULL,
			    ("managed clean buffer %p has no node", bp));
			srdx_destroy(srdx);
		} else {
			srdx = BP_SRDX_GET(bp);
			KASSERT(srdx == NULL,
			    ("unmanaged clean buffer has backing srdx node"));
			bremfree(bp);
			brelse(bp);
		}
	}

	/* We are dropping the reference we were passed in stree_init(). */
	vput(stree->stree_vp);
	uma_zfree(slos_rdxtree_zone, stree);
}

void
stree_rootcreate(struct slos_rdxtree *stree, diskptr_t ptr)
{
	struct slos_rdxnode *srdx;

	KASSERT(
	    ptr.offset != DISKPTR_NULL.offset, ("root radix node points to 0"));
	srdx_create(stree, &ptr, &srdx);
	srdx_release(srdx);
}

/*
 * ============ Basic tree traversal operations ============
 */

static int
stree_leaf(struct slos_rdxtree *stree, uint64_t key, bool insert,
    struct slos_rdxnode **srdxp)
{
	struct slos_rdxnode *srdx, *schild;
	diskblk_t localvalue;
	uint64_t localkey;
	diskptr_t ptr;
	int depth;
	int error;

	KASSERT(key < stree->stree_max, ("Key out of bounds"));

	ASSERT_VOP_LOCKED(stree->stree_vp, "streeleaf");

	error = srdx_retrieve(stree, stree->stree_root, insert, &srdx);
	if (error != 0)
		return (error);

	for (depth = 0; depth < STREE_DEPTH - 1; depth++) {
		localkey = stree_localkey(stree, key, depth);
		KASSERT(localkey < stree->stree_srdxcap,
		    ("invalid local key %lx %lx", localkey,
			stree->stree_srdxcap));
		localvalue = srdx->srdx_vals[localkey];

		/* Check if intermedate node is actually allocated. */
		if (!STREE_VALVALID(localvalue)) {
			/* Only inserts create missing intermediates. */
			if (!insert) {
				srdx_release(srdx);
				*srdxp = NULL;
				return (0);
			}

			ptr = DISKPTR_NULL;
			error = srdx_create(stree, &ptr, &schild);
			if (error != 0) {
				srdx_release(srdx);
				return (error);
			}

			STREE_DBG("[%d] Creating %p\n", __LINE__, schild);
			localvalue = (diskblk_t) { ptr.offset, ptr.epoch };
			srdx->srdx_vals[localkey] = localvalue;
		} else {
			error = srdx_retrieve(
			    stree, localvalue.offset, insert, &schild);
			if (error != 0) {
				srdx_release(srdx);
				return (error);
			}
			STREE_DBG("[%d] Retrieving %p\n", __LINE__, schild);
		}

		srdx_release(srdx);
		if (insert)
			srdx->srdx_key = key;
		srdx = schild;
	}

	STREE_DBG("[%d] Leaf return %p\n", __LINE__, srdx);
	if (insert)
		srdx->srdx_key = key;
	*srdxp = srdx;

	return (0);
}

int
stree_insert(struct slos_rdxtree *stree, uint64_t key, diskblk_t value)
{
	struct slos_rdxnode *srdx;
	uint64_t localkey;
	int error;
	/* Decide on how many levels we have, what sizes we are */

	ASSERT_VOP_LOCKED(stree->stree_vp, "stree");

	error = stree_leaf(stree, key, true, &srdx);
	if (error != 0) {
		return (error);
	}

	KASSERT(STREE_VALVALID(value),
	    ("overflow in existing value (%lx, %lx)", value.offset,
		value.epoch));

	localkey = stree_localkey(stree, key, STREE_DEPTH - 1);
	srdx->srdx_vals[localkey] = value;

	STREE_DBG("[%d] Releasing %p\n", __LINE__, srdx);
	srdx_release(srdx);

	return (0);
}

int
stree_find(struct slos_rdxtree *stree, uint64_t key, diskblk_t *value)
{
	struct slos_rdxnode *srdx;
	diskblk_t localval;
	uint64_t localkey;
	int error;

	ASSERT_VOP_LOCKED(stree->stree_vp, "streefind");

	error = stree_leaf(stree, key, false, &srdx);
	if (error != 0)
		return (error);

	/* Check if we found the element. */
	if (srdx == NULL) {
		*value = STREE_INVAL;
		return (0);
	}

	localkey = stree_localkey(stree, key, STREE_DEPTH - 1);
	localval = srdx->srdx_vals[localkey];

	/*
	 * Finding an invalid value at the last level still counts
	 * as finding a value, the caller handles the "miss". This
	 * is, useful, e.g., when finding empty unallocated ranges.
	 */

	STREE_DBG("[%d] Releasing %p\n", __LINE__, srdx);
	srdx_release(srdx);

	*value = localval;

	return (0);
}

int
stree_delete(struct slos_rdxtree *stree, uint64_t key)
{
	struct slos_rdxnode *srdx;
	uint64_t localkey;
	int error;
	/* Decide on how many levels we have, what sizes we are */

	/*
	 * DANGER: THE CODE ASSUMES WE CANNOT EMPTY OUT BLOCKS WHICH IS NOT TRUE
	 * IN THE GENERAL CASE! TO FIX THIS EITHER WE MUST EAGERLY REMOVE TREE
	 * NODES IN THE DELETE OPERATION (WHICH SHOULD BE EASY).
	 */

	ASSERT_VOP_LOCKED(stree->stree_vp, "streedelete");

	error = stree_leaf(stree, key, false, &srdx);
	if (error != 0)
		return (error);

	/* Deleting nonexistent keys is always successful. */
	if (srdx == NULL)
		return (0);

	localkey = stree_localkey(stree, key, STREE_DEPTH - 1);
	srdx->srdx_vals[localkey] = STREE_INVAL;

	STREE_DBG("[%d] Releasing  %p\n", __LINE__, srdx);
	srdx_release(srdx);

	return (0);
}

/*
 * ============ Extent functions for buffer retrieval. Used mostly ============
 * ============    in slsfs_strategy() and slsfs_retrieve_buf().    ============
 */

struct stree_iter {
	struct slos_rdxnode *siter_nodes[STREE_DEPTH];
	struct slos_rdxtree *siter_stree;
	uint64_t siter_key;
	bool siter_addmissing;
};

static int
siter_start(struct slos_rdxtree *stree, uint64_t key, bool addmissing,
    struct stree_iter *siter)
{
	struct slos_rdxnode *srdx, *schild;
	diskblk_t localvalue;
	uint64_t localkey;
	diskptr_t ptr;
	int error;
	int depth;
	int i;

	ASSERT_VOP_LOCKED(stree->stree_vp, "streestart");

	siter->siter_stree = stree;
	siter->siter_key = key;
	siter->siter_addmissing = addmissing;
	for (depth = 0; depth < STREE_DEPTH; depth++)
		siter->siter_nodes[depth] = NULL;

	KASSERT(key < stree->stree_max,
	    ("Key %lx out of bounds %ld", key, stree->stree_max));

	error = srdx_retrieve(stree, stree->stree_root, addmissing, &srdx);
	if (error != 0)
		panic("Needs proper cleanup here");

	for (depth = 0; depth < STREE_DEPTH - 1; depth++) {
		localkey = stree_localkey(stree, siter->siter_key, depth);
		KASSERT(localkey < stree->stree_srdxcap,
		    ("invalid local key %lx %lx", localkey,
			stree->stree_srdxcap));
		localvalue = srdx->srdx_vals[localkey];

		/* Check if intermediate node is actually allocated. */
		if (!STREE_VALVALID(localvalue)) {
			if (!siter->siter_addmissing)
				goto error;

			/* Create any missing nodes. */
			ptr = DISKPTR_NULL;
			error = srdx_create(stree, &ptr, &schild);
			if (error != 0) {
				panic("Needs proper cleanup here");
			}

			localvalue = (diskblk_t) { ptr.offset, ptr.epoch };
			STREE_ITER_DBG("[%d] Creating %lx\n", __LINE__, schild);
			srdx->srdx_vals[localkey] = localvalue;
		} else {
			error = srdx_retrieve(
			    stree, localvalue.offset, addmissing, &schild);
			if (error != 0) {
				panic("Needs proper cleanup here");
			}
			STREE_ITER_DBG(
			    "[%d] Retrieving %p\n", __LINE__, schild);
		}

		STREE_ITER_DBG(
		    "[%d] Insert depth %d (%p)\n", __LINE__, depth, schild);
		siter->siter_nodes[depth] = srdx;
		srdx->srdx_key = siter->siter_key;
		srdx = schild;
	}

	srdx->srdx_key = key;
	siter->siter_nodes[depth] = srdx;
	return (0);

error:
	/* Release all nodes already held. */
	for (i = 0; i < depth; i++) {
		if (siter->siter_nodes[i] == NULL)
			continue;

		srdx_release(siter->siter_nodes[i]);
	}
	srdx_release(srdx);

	return (EINVAL);
}

static int
siter_iter(struct stree_iter *siter)
{
	struct slos_rdxtree *stree = siter->siter_stree;
	struct slos_rdxnode *srdx, *schild;
	diskblk_t localvalue;
	uint64_t localkey;
	diskptr_t ptr;
	int depth;
	int error;
	int i;

	if (siter->siter_key + 1 == stree->stree_max)
		goto error;

	for (depth = STREE_DEPTH - 1; depth >= 0; depth--) {
		localkey = stree_localkey(stree, siter->siter_key, depth);
		if (localkey + 1 < stree->stree_srdxcap)
			break;

		STREE_ITER_DBG("[%d] Releasing depth %d (%p)\n", __LINE__,
		    depth, siter->siter_nodes[depth]);
		srdx_release(siter->siter_nodes[depth]);
		siter->siter_nodes[depth] = NULL;
	}

	siter->siter_key += 1;

	for (srdx = siter->siter_nodes[depth], siter->siter_nodes[depth] = NULL;
	     depth < STREE_DEPTH - 1; depth++) {
		localkey = stree_localkey(stree, siter->siter_key, depth);
		KASSERT(localkey < stree->stree_srdxcap,
		    ("invalid local key %lx %lx", localkey,
			stree->stree_srdxcap));
		localvalue = srdx->srdx_vals[localkey];

		/* Check if intermediate node is actually allocated. */
		if (!STREE_VALVALID(localvalue)) {
			if (!siter->siter_addmissing)
				goto error;

			ptr = DISKPTR_NULL;
			error = srdx_create(stree, &ptr, &schild);
			if (error != 0) {
				panic("Needs proper cleanup here");
			}

			localvalue = (diskblk_t) { ptr.offset, ptr.epoch };
			srdx->srdx_vals[localkey] = localvalue;
			STREE_ITER_DBG("[%d] Creating %p\n", __LINE__, schild);
		} else {
			error = srdx_retrieve(stree, localvalue.offset,
			    siter->siter_addmissing, &schild);
			if (error != 0) {
				panic("Needs proper cleanup here");
			}
			STREE_ITER_DBG(
			    "[%d] Retrieving %p\n", __LINE__, schild);
		}

		STREE_ITER_DBG(
		    "[%d] Replace depth %d (%p)\n", __LINE__, depth, schild);
		siter->siter_nodes[depth] = srdx;
		srdx->srdx_key = siter->siter_key;
		srdx = schild;
	}

	srdx->srdx_key = siter->siter_key;
	siter->siter_nodes[depth] = srdx;

	return (0);

error:
	/* Release all nodes already held. */
	for (i = 0; i < STREE_DEPTH; i++) {
		if (siter->siter_nodes[i] == NULL)
			continue;

		srdx_release(siter->siter_nodes[i]);
	}
	srdx_release(srdx);

	return (EINVAL);
}

static void
siter_access(struct stree_iter *siter, diskblk_t *value)
{
	struct slos_rdxnode *srdx = siter->siter_nodes[STREE_DEPTH - 1];
	uint64_t localkey;

	if (siter->siter_key == siter->siter_stree->stree_max) {
		*value = STREE_INVAL;
		return;
	}

	localkey = stree_localkey(
	    siter->siter_stree, siter->siter_key, STREE_DEPTH - 1);
	*value = srdx->srdx_vals[localkey];
}

static void
siter_replace(struct stree_iter *siter, diskblk_t value)
{
	struct slos_rdxnode *srdx = siter->siter_nodes[STREE_DEPTH - 1];
	uint64_t localkey;

	localkey = stree_localkey(
	    siter->siter_stree, siter->siter_key, STREE_DEPTH - 1);
	srdx->srdx_vals[localkey] = value;
}

static void
siter_end(struct stree_iter *siter)
{
	int depth;

	for (depth = 0; depth < STREE_DEPTH; depth++) {
		if (siter->siter_nodes[depth] == NULL)
			continue;
		srdx_release(siter->siter_nodes[depth]);
		siter->siter_nodes[depth] = NULL;
	}

	siter->siter_key = siter->siter_stree->stree_max;
}

int
stree_extent_replace(struct slos_rdxtree *stree, uint64_t offset, diskptr_t ptr)
{
	struct stree_iter siter;
	diskblk_t pblk;
	int error;

	error = siter_start(stree, offset, true, &siter);
	KASSERT(error == 0, ("siter_start failed in insert mode"));

	while (ptr.size > 0) {
		/* Replace a single block. */
		pblk = (diskblk_t) { ptr.offset, ptr.epoch };
		siter_replace(&siter, pblk);

		ptr.offset += 1;
		ptr.size -= BLKSIZE(&slos);

		error = siter_iter(&siter);
		KASSERT(error == 0, ("siter_iter failed in insert mode"));
	}
	siter_end(&siter);

	return (0);
}

/*
 * Get the largest possible extent starting from the given offset.
 * The extent must start from an already valid block.
 */
int
stree_extent_find(struct slos_rdxtree *stree, uint64_t offset, diskptr_t *ptr)
{
	struct stree_iter siter;
	diskblk_t pblk;
	size_t blklen;
	int error;

	error = siter_start(stree, offset, false, &siter);
	if (error != 0) {
		ptr->offset = STREE_INVAL.offset;
		ptr->size = 0;
		ptr->epoch = STREE_INVAL.epoch;
		return (0);
	}

	siter_access(&siter, &pblk);
	ptr->offset = pblk.offset;
	ptr->size = BLKSIZE(&slos);
	ptr->epoch = pblk.epoch;

	for (blklen = 1; siter_iter(&siter) == 0; blklen++) {
		siter_access(&siter, &pblk);

		/* Roll the pointer as far as possible. */
		if (ptr->offset + blklen != pblk.offset) {
			siter_end(&siter);
			break;
		}

		ptr->size += BLKSIZE(&slos);
	}

	return (0);
}

/*
 * ============ Checkpoint related functions. ============
 */

static void
stree_rootchange(struct slos_rdxtree *stree, diskptr_t ptr)
{
	struct slos_node *svp = SLSVP(stree->stree_vp);

	svp->sn_ino.ino_btree = ptr;
	stree->stree_root = ptr.offset;
	if (svp != SLSVP(slos.slsfs_inodes))
		slos_update(svp);
}

static int
srdx_forcecopy(
    struct slos_rdxtree *stree, struct slos_rdxnode *srdx, diskptr_t *ptrp)
{
	struct bufobj *bo = &stree->stree_vp->v_bufobj;
	b_xflags_t xflags;
	struct buf *bp;
	daddr_t oldoff;
	diskptr_t ptr;
	int error;

	ASSERT_VOP_LOCKED(stree->stree_vp, "slosradix");

	bp = srdx->srdx_buf;
	BUF_ASSERT_LOCKED(bp);

	/* If already COWed, we're done. */
	if (BP_NEEDSCOW(bp)) {
		*ptrp = DISKPTR_NULL;
		return (0);
	}

	error = slos_blkalloc(&slos, BLKSIZE(&slos), &ptr);
	if (error != 0) {
		*ptrp = DISKPTR_NULL;
		return (ENOMEM);
	}

	BO_LOCK(bo);

	STREE_SYNC_DBG("[FORCECOPY] %p\n", bp);
	BP_SET_NEEDSCOW(bp);

	xflags = bp->b_xflags;
	buf_vlist_remove(bp);

	oldoff = bp->b_lblkno;
	/*
	 * We are changing both the logical and the physical block numbers,
	 * which are identical for tree nodes anyway. This is the only place
	 * we manually set the logical block number, since slsfs_retrieve_buf()
	 * takes care of it everywhere else.
	 */
	bp->b_lblkno = ptr.offset;
	bp->b_blkno = ptr.offset;

	buf_vlist_add(bp, bo, xflags);

	BO_UNLOCK(bo);

	*ptrp = ptr;
	BUF_ASSERT_LOCKED(bp);

	return (0);
}

/*
 * Force copy all tree nodes in a path from the root to a key
 * containing the key.
 */
static int
stree_forcecopy(struct slos_rdxtree *stree, uint64_t key)
{
	struct slos_rdxnode *srdxpath[STREE_DEPTH];
	uint64_t localkeys[STREE_DEPTH];
	diskptr_t newptrs[STREE_DEPTH];
	struct slos_rdxnode *srdx, *schild;
	diskblk_t localvalue;
	uint64_t localkey;
	diskblk_t bptr;
	diskptr_t ptr;
	int error;
	int depth;

	ASSERT_VOP_LOCKED(stree->stree_vp, "slosradix");

	KASSERT(key < stree->stree_max,
	    ("Key %lx out of bounds %ld", key, stree->stree_max));

	/*
	 * At this point the retrieval is read only, we have dirtied the parents
	 * of the dirty nodes during the insert.
	 */
	error = srdx_retrieve(stree, stree->stree_root, false, &srdx);
	if (error != 0)
		panic("Needs proper cleanup here");

	/* Traverse the path, copying throughout. */
	for (depth = 0; depth < STREE_DEPTH - 1; depth++) {
		localkey = stree_localkey(stree, key, depth);
		KASSERT(localkey < stree->stree_srdxcap,
		    ("invalid local key %lx %lx", localkey,
			stree->stree_srdxcap));

		localvalue = srdx->srdx_vals[localkey];
		/*
		 * In the case the stored key was in a node that was later
		 * deleted, the path to the key terminates at some point. We
		 * only need to handle the path up to the point where it is
		 * valid.
		 *
		 * XXX Implement the above. We currently don't have branch
		 * deletes, so the case below cannot happen. Modify this
		 * function to accurately reflect the above description.
		 */
		if (!STREE_VALVALID(localvalue)) {
			for (int i = 0; i < stree->stree_srdxcap; stree++) {
				localvalue = srdx->srdx_vals[localkey];
			}

			panic(
			    "[%p] COW key %lx %lx leads to invalid path depth %d (inode %lx)",
			    srdx, key, localkey, depth,
			    SLSVP(stree->stree_vp)->sn_ino.ino_pid);
		}

		KASSERT(localvalue.offset != srdx->srdx_buf->b_lblkno,
		    ("loop detected"));
		error = srdx_retrieve(stree, localvalue.offset, false, &schild);
		if (error != 0) {
			panic("Needs proper cleanup here");
		}

		STREE_ITER_DBG("[%d] Retrieving %p\n", __LINE__, schild);
		STREE_ITER_DBG(
		    "[%d] Insert depth %d (%p)\n", __LINE__, depth, schild);

		srdxpath[depth] = srdx;
		localkeys[depth] = localkey;
		srdx_forcecopy(stree, srdx, &newptrs[depth]);

		srdx = schild;
	}

	/*
	 * XXX Same point as above regarding the recursive deletes.
	 * Use a maxdepth variable instead of STREE_DEPTH.
	 */
	KASSERT(
	    depth == STREE_DEPTH - 1, ("stopped before we reached the bottom"));
	srdxpath[depth] = srdx;
	localkeys[depth] = localkey;
	srdx_forcecopy(stree, srdx, &newptrs[depth]);

	/* Mend the pointers on the path. */
	for (depth = 0; depth < STREE_DEPTH - 1; depth++) {
		ptr = newptrs[depth + 1];
		if (ptr.offset == DISKPTR_NULL.offset)
			continue;

		srdx = srdxpath[depth];
		bptr = (diskblk_t) { ptr.offset, ptr.epoch };
		srdx->srdx_vals[localkeys[depth]] = bptr;
	}

	/* All nodes fixed, flush them out. */
	for (depth = 0; depth < STREE_DEPTH; depth++) {
		srdx = srdxpath[depth];
		BUF_ASSERT_LOCKED(srdx->srdx_buf);
		srdx_release(srdx);
		BUF_ASSERT_UNLOCKED(srdx->srdx_buf);
	}

	/* Fix up the tree root in the inode. */
	if (newptrs[0].offset != DISKPTR_NULL.offset)
		stree_rootchange(stree, newptrs[0]);

	return (0);
}

int
stree_sync(struct slos_rdxtree *stree)
{
	/* XXX To be implemented in a future patch. */
	struct bufobj *bo = &stree->stree_vp->v_bufobj;
	struct slos_rdxnode *srdx;
	struct buf *bp, *tbd;
	int attempts = 0;
	int error = 0;

	VOP_LOCK(stree->stree_vp, LK_EXCLUSIVE);

	BO_LOCK(bo);
tryagain:
	BO_UNLOCK(bo);
	if (bo->bo_dirty.bv_cnt) {
		TAILQ_FOREACH_SAFE (bp, &bo->bo_dirty.bv_hd, b_bobufs, tbd) {
			srdx = BP_SRDX_GET(bp);
			BUF_ASSERT_UNLOCKED(bp);

			STREE_SYNC_DBG("[TRAVERSE] %p\n", bp);
			stree_forcecopy(stree, srdx->srdx_key);
		}

		BO_LOCK(bo);

		/* Apply COW to every tree buffer. */
		TAILQ_FOREACH_SAFE (bp, &bo->bo_dirty.bv_hd, b_bobufs, tbd) {
			/* XXX Use the COW bit properly  */
			if (!BP_NEEDSCOW(bp)) {
				if (attempts > 100) {
					panic(
					    "btree not synced after 100 attempts");
				}
				attempts++;
				goto tryagain;
			}
		}

		/* Now that everything is COWed and immutable, flush it to the
		 * disk. */
		TAILQ_FOREACH_SAFE (bp, &bo->bo_dirty.bv_hd, b_bobufs, tbd) {
			error = BUF_LOCK(
			    bp, LK_EXCLUSIVE | LK_INTERLOCK, BO_LOCKPTR(bo));
			if (error != 0) {
				panic("Unhandled BUF_LOCK failure %d\n", error);
			}

			if (!BP_NEEDSCOW(bp)) {
				panic("Buffer is not COW anymore");
			}

			BP_UNSET_NEEDSCOW(bp);
			bawrite(bp);
			BO_LOCK(bo);
		}

		/* Ensure everything was properly flushed. */
		MPASS(bo->bo_dirty.bv_cnt == 0);
		BO_UNLOCK(bo);
	}

	BO_LOCK(bo);

	error = bufobj_wwait(bo, 0, 0);
	MPASS(error == 0);

	TAILQ_FOREACH_SAFE (bp, &bo->bo_clean.bv_hd, b_bobufs, tbd) {
		error = BUF_LOCK(
		    bp, LK_EXCLUSIVE | LK_INTERLOCK, BO_LOCKPTR(bo));
		if (error != 0) {
			panic("Unhandled BUF_LOCK failure %d\n", error);
		}
		if (bp->b_flags & B_MANAGED) {
			/*
			 * Catch any buffers moved to the clean queue between
			 * us marking dirty buffers and initiating :he writes.
			 */
			if (BP_NEEDSCOW(bp))
				BP_UNSET_NEEDSCOW(bp);

			srdx = BP_SRDX_GET(bp);
			if (srdx == NULL)
				panic("Managed buffer without an srdx\n");
			srdx_destroy(srdx);
		} else {
			bremfree(bp);
			brelse(bp);
		}
		BO_LOCK(bo);
	}

	error = bufobj_wwait(bo, 0, 0);
	MPASS(error == 0);

	BO_UNLOCK(bo);

	VOP_UNLOCK(stree->stree_vp, 0);

	return (0);
}

/*
 * ============ Sparse extent operations. ============
 * ============     Used by the SLS.	  ============
 */

static inline int
stree_localkey_increment(struct slos_rdxtree *stree, uint64_t *key, int depth)
{
	uint64_t movbits = (STREE_DEPTH - 1 - depth) * fls(stree->stree_mask);

	if (*key == stree->stree_max)
		return (EINVAL);

	/* Zero out the bits before the local key. */
	if (movbits > 0)
		*key = *key & ~((1ULL << movbits) - 1);
	/* Increment the local key. */
	*key = *key + (1ULL << movbits);
	return (0);
}

/*
 * Find the first valid value in the node with a key >= the one given.
 */
static bool
stree_siter_moveright(struct slos_rdxnode *srdx, uint64_t *key, int depth)
{
	struct slos_rdxtree *stree = srdx->srdx_tree;
	uint64_t localkey;
	diskblk_t localvalue;

	for (;;) {
		localkey = stree_localkey(stree, *key, depth);
		/* Will trigger if the tree is empty. */
		KASSERT(localkey < stree->stree_srdxcap,
		    ("invalid local key %lx %lx", localkey,
			stree->stree_srdxcap));

		/* Go down if possible, otherwise go up. */
		localvalue = srdx->srdx_vals[localkey];
		if (STREE_VALVALID(localvalue))
			return true;

		/* Reached the end of the node. */
		if (localkey + 1 == stree->stree_srdxcap)
			return false;

		stree_localkey_increment(stree, key, depth);
	}
}

static bool
stree_siter_moveup(struct stree_iter *siter, uint64_t *keyp, int *depthp)
{
	struct slos_rdxtree *stree = siter->siter_stree;
	diskblk_t localvalue;
	uint64_t key = *keyp;
	int depth = *depthp;
	uint64_t localkey;
	bool found;
	uint64_t oldkey;

	for (;;) {
		oldkey = key;

		/* Go as far up as possible to continue scanning right. */
		localkey = stree_localkey(stree, key, depth);
		while (localkey + 1 == stree->stree_srdxcap) {

			if (siter->siter_nodes[depth] != NULL) {
				srdx_release(siter->siter_nodes[depth]);
				siter->siter_nodes[depth] = NULL;
			}

			if (depth == 0) {
				*depthp = depth;
				*keyp = key;
				return (false);
			}

			depth -= 1;
			localkey = stree_localkey(stree, key, depth);
		}

		/* Move to the right. */
		stree_localkey_increment(stree, &key, depth);

		found = stree_siter_moveright(
		    siter->siter_nodes[depth], &key, depth);
		if (found) {
			localkey = stree_localkey(stree, key, depth);
			localvalue =
			    siter->siter_nodes[depth]->srdx_vals[localkey];
			KASSERT(STREE_VALVALID(localvalue),
			    ("stopped on invalid value"));
			break;
		}

		if (oldkey == key) {
			panic("loop");
		}
	}

	*depthp = depth;
	*keyp = key;
	return (true);
}

/*
 * Find the closest extent to the right of the key. If the key
 * is in the middle of an extent, we ignore the part to the left.
 */
static int
siter_extent_keymin_start(
    struct slos_rdxtree *stree, uint64_t key, struct stree_iter *siter)
{
	struct slos_rdxnode *srdx, *schild;
	diskblk_t localvalue;
	uint64_t localkey;
	bool found;
	int error;
	int depth;

	ASSERT_VOP_LOCKED(stree->stree_vp, "streestart");

	siter->siter_stree = stree;
	siter->siter_key = key;
	siter->siter_addmissing = false;
	for (depth = 0; depth < STREE_DEPTH; depth++)
		siter->siter_nodes[depth] = NULL;

	KASSERT(key < stree->stree_max,
	    ("Key %lx out of bounds %ld", key, stree->stree_max));

	error = srdx_retrieve(stree, stree->stree_root, false, &srdx);
	if (error != 0)
		panic("Needs proper cleanup here");

	for (depth = 0; depth < STREE_DEPTH; depth++) {
		siter->siter_nodes[depth] = srdx;

		srdx = siter->siter_nodes[depth];
		found = stree_siter_moveright(srdx, &key, depth);
		if (!found) {
			KASSERT(depth > 0, ("already at the top level"));

			/*
			 * The only way we fail is if the key rolled over.
			 * In that case it is safe to move one level up.
			 */
			KASSERT(stree_localkey(stree, key, depth) + 1 ==
				stree->stree_srdxcap,
			    ("key not at the edge of the node"));

			found = stree_siter_moveup(siter, &key, &depth);
			if (!found)
				panic("No keys to our right (handle)");
			srdx = siter->siter_nodes[depth];
		}

		localkey = stree_localkey(stree, key, depth);
		localvalue = srdx->srdx_vals[localkey];
		KASSERT(STREE_VALVALID(localvalue), ("invalid local value"));

		if (depth == STREE_DEPTH - 1) {
			found = stree_siter_moveright(srdx, &key, depth);
			MPASS(found);
			break;
		}

		error = srdx_retrieve(stree, localvalue.offset, false, &schild);
		if (error != 0) {
			panic("Needs proper cleanup here");
		}

		srdx = schild;
	}

	siter->siter_key = key;
	siter->siter_nodes[depth] = srdx;
	return (0);
}

/*
 * siter_keymin is our stree_iter start with modifications
 * stree_extent_next is the userspace siter_iter
 * stree_extent_find is similar to the existing one, but
 * uses the two functions above
 */

static int
siter_extent_keymin_next(struct stree_iter *siter)
{
	struct slos_rdxtree *stree = siter->siter_stree;
	struct slos_rdxnode *srdx = NULL, *schild;
	uint64_t key = siter->siter_key;
	diskblk_t localvalue;
	uint64_t localkey;
	bool found;
	int depth;
	int error;

	ASSERT_VOP_LOCKED(stree->stree_vp, "streeiter");

	if (key + 1 == stree->stree_max)
		goto iterdone;

	depth = STREE_DEPTH - 1;
	found = stree_siter_moveup(siter, &key, &depth);
	if (!found)
		goto iterdone;

	srdx = siter->siter_nodes[depth];
	siter->siter_nodes[depth] = NULL;
	for (;;) {
		/* Move laterally. */
		found = stree_siter_moveright(srdx, &key, depth);
		KASSERT(
		    found, ("did not find next value during keymin iteration"));
		localkey = stree_localkey(stree, key, depth);

		localvalue = srdx->srdx_vals[localkey];
		KASSERT(STREE_VALVALID(localvalue), ("invalid local value"));

		if (depth == STREE_DEPTH - 1)
			break;

		/* Move downwards. */
		error = srdx_retrieve(stree, localvalue.offset, false, &schild);
		if (error != 0)
			panic("Needs proper cleanup here");

		KASSERT(siter->siter_nodes[depth] == NULL,
		    ("overwriting existing node"));
		siter->siter_nodes[depth] = srdx;
		srdx = schild;

		depth += 1;
	}

	siter->siter_key = key;
	siter->siter_nodes[depth] = srdx;

	return (0);

iterdone:
	siter_end(siter);
	if (srdx != NULL)
		srdx_release(srdx);

	return (EINVAL);
}

/*
 * Get the largest possible extent starting from the given offset.
 * The extent must start from an already valid block. The iterator
 * points to the next valid extent by the end of the function.
 */
static int
siter_extent_keymin_find(struct stree_iter *siter, diskptr_t *ptr)
{
	uint64_t key = siter->siter_key;
	diskblk_t pblk;
	size_t blklen;

	siter_access(siter, &pblk);

	if (!STREE_VALVALID(pblk)) {
		ptr->offset = STREE_INVAL.offset;
		ptr->size = 0;
		ptr->epoch = STREE_INVAL.epoch;
		return (EINVAL);
	}

	ptr->offset = pblk.offset;
	ptr->size = BLKSIZE(&slos);
	ptr->epoch = pblk.epoch;

	for (blklen = 1; siter_extent_keymin_next(siter) == 0; blklen++) {
		/* Ensure the data is logically contiguous. */
		if (key + blklen != siter->siter_key)
			break;

		/* Ensure the data is physically contiguous. */
		siter_access(siter, &pblk);
		if (ptr->offset + blklen != pblk.offset) {
			siter_end(siter);
			break;
		}

		if (ptr->size == MAXBCACHEBUF)
			break;

		ptr->size += BLKSIZE(&slos);
	}

	return (0);
}

int
stree_numextents(struct slos_rdxtree *stree, uint64_t *numextentsp)
{
	uint64_t start = *numextentsp;
	struct stree_iter siter;
	uint64_t numextents = 0;
	diskptr_t ptr;
	int error;

	error = siter_extent_keymin_start(stree, start, &siter);
	if (error != 0) {
		*numextentsp = 0;
		return (0);
	}

	uint64_t past = siter.siter_key;
	while (siter_extent_keymin_find(&siter, &ptr) == 0) {
		numextents += 1;
		if (siter.siter_key == past) {
			diskblk_t pblk;
			siter_access(&siter, &pblk);
			panic("[%lx] Loop on %lx (%s)\n",
			    SLSVP(stree->stree_vp)->sn_ino.ino_pid, past,
			    STREE_VALVALID(pblk) ? "yes" : "no");
		}
		past = siter.siter_key;
	}

	*numextentsp = numextents;

	return (0);
}

int
stree_getextents(struct slos_rdxtree *stree, struct slos_extent *extents)
{
	uint64_t key = extents[0].sxt_lblkno;
	struct stree_iter siter;
	diskptr_t ptr;
	int error;
	int i;

	error = siter_extent_keymin_start(stree, key, &siter);
	if (error != 0)
		return (error);
	key = siter.siter_key;

	for (i = 0; siter_extent_keymin_find(&siter, &ptr) == 0; i++) {

		KASSERT(ptr.size > 0, ("extent unexpectedly missing"));

		extents[i].sxt_lblkno = key;
		extents[i].sxt_cnt = ptr.size / BLKSIZE(&slos);

		key = siter.siter_key;
	}

	KASSERT(key == stree->stree_max, ("did not fully traverse the tree"));

	return (0);
}
