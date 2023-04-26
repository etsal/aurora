
#include <sys/types.h>
#include <sys/param.h>
#include <sys/buf.h>
#include <sys/queue.h>
#include <sys/uio.h>
#include <sys/vnode.h>

#include <vm/uma.h>

#include <machine/atomic.h>

#include <slos.h>
#include <slos_bnode.h>
#include <slos_btree.h>
#include <slos_inode.h>
#include <slos_io.h>
#include <slsfs.h>

#include "btree.h"
#include "debug.h"
#include "slsfs_buf.h"

SDT_PROVIDER_DEFINE(slsfsbuf);
SDT_PROBE_DEFINE(slsfsbuf, , , start);
SDT_PROBE_DEFINE(slsfsbuf, , , end);

/*
 * This function assume that the tree at the iter does not exist or is no in
 * the way.  It then increments the iterator to find the position of the next
 * key either the key is close and we have to truncate our size to squeeze
 * between the previos key and the next or we can fully insert ourselves into
 * the truee
 */
static int
slsfs_buf_insert(struct vnode *vp, diskptr_t *ptr, uint64_t bno,
    uint64_t size, enum uio_rw rw, int gbflag, struct buf **bp)
{
	int error;
	struct slos_node *svp = SLSVP(vp);
	struct vtree *tree = &svp->sn_vtree;
	uint64_t blksize = BLKSIZE(&slos);
	ptr->offset = 0;
	ptr->size = size;
	ptr->epoch = EPOCH_INVAL;
	error = vtree_insert(tree, bno, ptr);
	if (error) {
		panic("Problem inserting into tree");
		return (error);
	}

	error = slsfs_balloc(vp, bno, blksize, gbflag, bp);
	if (error != 0)
		panic("Balloc failed\n");

	if (*bp == NULL)
		panic("slsfs_buf_nocollide failed to allocate\n");

	return (error);
}

int
slsfs_retrieve_buf(struct vnode *vp, uint64_t offset, uint64_t size,
    enum uio_rw rw, int gbflag, struct buf **bp)
{
	diskptr_t ptr;
	int error = 0;
	struct slos_node *svp = SLSVP(vp);

	size_t blksize = IOSIZE(svp);
	uint64_t bno = offset / blksize;

#ifdef VERBOSE
	DEBUG1("Attemping to retrieve buffer %lu bno", bno);
#endif
	KASSERT(
	    vp->v_type != VCHR, ("Retrieving buffer for btree backing vnode"));

  /* Round up to our minimum size so we dont read less the a sector size */
  size = roundup(size, IOSIZE(svp));

  /* If we error that means no entry was found */
	error = slsfs_lookupbln(svp, bno, &ptr);
	if (error != 0) {
		return slsfs_buf_insert(vp, &ptr, bno, size, rw, gbflag, bp);
	}

  /* This has not been written yet so the blk must be in our cache */
  if (ptr.offset == 0) {
		*bp = getblk(vp, bno, size, 0, 0, gbflag);
		if (*bp == NULL)
			panic("LINE %d: null bp for %lu, %lu", __LINE__, bno,
			    size);
	} else {
		/* Otherwise do an actual IO to retrieve the buf */
		error = slsfs_bread(vp, bno, size, NULL, gbflag, bp);
		if (*bp == NULL)
			panic("LINE %d: null bp for %lu, %lu", __LINE__, bno,
			    size);
    printf("POSSIBLE ERROR SLSFS BREAD %d\n", error);
	}

	return (error);
}

/*
 * Create a buffer corresponding to logical block lbn of the vnode.
 *
 * Since we put off allocation till sync time (checkpoint time) we just need to
 * keep track of logical blocks and sizes.
 *
 * NOTES:
 *
 * getblk uses a function called allocbuf to retrieve pages for the underlying
 * data that is required for it.  These pages can be mapped in or unmapped in
 * (GB_UNMAPPED), mapping in these pages to the address space is incredbly
 * expensive so should only be used if the caller requires the data from the
 * read/write in the kernel itself.
 *
 * Pages that are attached to the b_pages buffer when unmanaged have the
 * VPO_UNMANAGED flag.
 *
 * An issue occurs though on subsequent calls to the same lbn of a vp,
 * if you call a size smaller then the orginal when GB_UNMAPPED was orginally
 * called.  allocbuf is called and if the call is truncating the data, it will
 * release underlying pages, but these pages are unmanaged so this will panic
 * the kernel.  Block based file systems don't really need to worry about this
 * as they always just get a block, so pages are not released.
 *
 *
 * XXX:  Currently we only create blocks of blksize (this is just for
 * simplicity right now)
 */
int
slsfs_balloc(
    struct vnode *vp, uint64_t lbn, size_t size, int gbflag, struct buf **bp)
{
	struct buf *tempbuf = NULL;
	int error = 0;
	KASSERT(size % IOSIZE(SLSVP(vp)) == 0, ("Multiple of iosize"));
	/* Currently a hack (maybe not)  since this is the only inode we use
	 * VOP_WRITES for that is also a system node, we need to make sure that
	 * is only reads in its actual blocksize, if our first read is 64kb and
	 * our subsequent calls to getblk are 4kb then it will try to truncate
	 * our pages resulting in the attempted release of unmanaged pages
	 */
	tempbuf = getblk(vp, lbn, size, 0, 0, gbflag);
	if (tempbuf == NULL) {
		*bp = NULL;
		return (EIO);
	}

	vfs_bio_clrbuf(tempbuf);
	tempbuf->b_blkno = (daddr_t)(-1);

	*bp = tempbuf;

	return (error);
}

/*
 * Read a block corresponding to the vnode from the buffer cache.
 */
/* I love bread  - Me, 2020 */
int
slsfs_bread(struct vnode *vp, uint64_t lbn, size_t size, struct ucred *cred,
    int flags, struct buf **buf)
{
	int error;

#ifdef VERBOSE
	DEBUG3("Reading block at %lx of size %lu for node %p", lbn, size, vp);
#endif
	error = breadn_flags(vp, lbn, size, NULL, NULL, 0, curthread->td_ucred,
	    flags, NULL, buf);
	if (error != 0)
		return (error);

	return (0);
}

int
slsfs_devbread(struct slos *slos, uint64_t lbn, size_t size, struct buf **bpp)
{
	int fsbsize, devbsize;
	uint64_t bn;
	int change;

	fsbsize = BLKSIZE(slos);
	devbsize = slos->slos_vp->v_bufobj.bo_bsize;

	change = fsbsize / devbsize;
	bn = lbn * change;

	KASSERT(size > 0, ("Device read of size 0"));
	KASSERT(size % BLKSIZE(slos) == 0, ("Unaligned device read"));

	return (bread(slos->slos_vp, bn, size, curthread->td_ucred, bpp));
}

/*
 * Mark a buffer as dirty, initializing its flush to disk.
 */
void
slsfs_bdirty(struct buf *buf)
{
	uint64_t size;
	// If we are dirtying a buf thats already que'd for a write we should
	// not signal another bawrite as the system will panic wondering why we
	if (buf->b_flags & B_DELWRI) {
		bqrelse(buf);
		return;
	}

	/* Get the value before we lose ownership of the buffer. */
	size = buf->b_bufsize;

	/* Be aggressive and start the IO immediately. */
	buf->b_flags |= B_CLUSTEROK;
	SLSVP(buf->b_vp)->sn_status |= SLOS_DIRTY;
	bawrite(buf);

	atomic_add_64(&slos.slos_sb->sb_used, size / BLKSIZE(&slos));

	return;
}

/*
 * Synchronously flush a buffer out.
 *
 * Assumes buf is locked - bwrite will unlock and release the buffer.
 */
int
slsfs_bundirty(struct buf *buf)
{
	if (!(buf->b_flags & B_MANAGED)) {
		bremfree(buf);
	}
	buf->b_flags &= ~(B_MANAGED);
	return bwrite(buf);
}

/*
 * Find the logically largest extent that starts before the block number
 * provided.
 */
int
slsfs_lookupbln(struct slos_node *svp, uint64_t lbn, diskptr_t *ptr)
{
	return vtree_find(&svp->sn_vtree, lbn, ptr);
}
