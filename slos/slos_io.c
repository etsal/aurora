#include <sys/param.h>

#include <sys/bio.h>
#include <sys/buf.h>
#include <sys/condvar.h>
#include <sys/conf.h>
#include <sys/fcntl.h>
#include <sys/file.h>
#include <sys/filedesc.h>
#include <sys/kthread.h>
#include <sys/libkern.h>
#include <sys/limits.h>
#include <sys/malloc.h>
#include <sys/namei.h>
#include <sys/param.h>
#include <sys/pcpu.h>
#include <sys/syscallsubr.h>
#include <sys/stat.h>
#include <sys/systm.h>
#include <sys/vnode.h>
#include <sys/uio.h>

#include <geom/geom.h>
#include <geom/geom_vfs.h>

#include <vm/vm.h>
#include <vm/pmap.h>
#include <vm/vm_map.h>
#include <vm/vm_page.h>

#include <machine/atomic.h>
#include <machine/vmparam.h>

#include "slos_bootalloc.h"
#include "slos_internal.h"
#include "slos_io.h"
#include "slosmm.h"
#include "slsfs.h"

/* 
 * Initialize a UIO for operation rwflag at offset off,
 * and asssign an IO vector to the given UIO. 
 */
void
slos_uioinit(struct uio *auio, uint64_t off, enum uio_rw rwflag,
	struct iovec *aiov, size_t iovcnt)
{
	size_t len;
	int i;

	bzero(auio, sizeof(*auio));

	auio->uio_iov = NULL;
	auio->uio_offset = off;
	auio->uio_segflg = UIO_SYSSPACE;
	auio->uio_rw = rwflag;
	auio->uio_iovcnt = 0;
	auio->uio_resid = 0;
	auio->uio_td = curthread;

	for (len = 0, i = 0; i < iovcnt; i++)
	    len += aiov[i].iov_len;

	auio->uio_iov = aiov;
	auio->uio_iovcnt = iovcnt;
	auio->uio_resid = len;
}

/* 
 * Fill a UIO with len bytes worth of zeros. 
 * Code follows the same logic as the one
 * in dev/null/null.c, and ffs_read_hole().
 */
int
slos_uiozero(struct uio *auio, size_t xfersize)
{
	ssize_t saved_resid, tlen;
	uint64_t offset;
	int error = 0;
	void *zbuf;


	/* 
	 * Temporarily save offset, it will be 
	 * restored after we read the hole. 
	 */
	offset = auio->uio_offset;
	auio->uio_offset = 0;
	

	if (xfersize > auio->uio_resid)
	    xfersize = auio->uio_resid;

	zbuf = __DECONST(void *, zero_region);
	while (xfersize > 0) {
	    /* 
	     * Read as much of the zero 
	     * region as possible/needed.
	     */
	    tlen = min(xfersize, ZERO_REGION_SIZE);
	    saved_resid = auio->uio_resid;

	    /* The actual transfer. */
	    error = vn_io_fault_uiomove(zbuf, tlen, auio);
	    if (error != 0)
		break;

	    /* Find out how much was actually transferred. */
	    tlen = saved_resid - auio->uio_resid;
	    xfersize -= tlen;
	    offset += tlen;
	}

	auio->uio_offset = offset;


	return error;
}

/*
 * Routines for filesystems actually backed by disks. The functions work
 * with the buffer cache and the device itself.
 */

/* Read from the specified extent. */
static int
slos_read_disk(struct slos *slos, struct slos_diskptr *diskptr, struct uio *uio)
{
	struct buf *bp;
	uint64_t boff;
	uint64_t bno;
	long size, xfersize;
	uint64_t bytesinextent;
	uint64_t sectorsperblock;
	int error;
	/* 
	 * There is a distinction between logical 
	 * blocks and physical sectors. Most disks
	 * expose 512 byte sectors even if they actually
	 * use 4k for compatibility reasons, and so the
	 * block numbers given to the buffer cache 
	 * correspond to 512 byte increments.
	 */
	sectorsperblock = slos->slos_sb->sb_bsize / slos->slos_sb->sb_ssize;
	/* XXX This doesn't work for sb_ssize != 512, find out why */

	for (error = 0, bp = NULL; uio->uio_resid > 0; bp = NULL) {
	    /* Check if the offset is still in the extent. */
	    bytesinextent = (diskptr->size * slos->slos_sb->sb_bsize) - uio->uio_offset;
	    if (bytesinextent <= 0)
		break;


	    /* Get the block number and the offset in the block. */
	    bno = (blkno(slos, uio->uio_offset) + diskptr->offset) * sectorsperblock;
	    boff = blkoff(slos, uio->uio_offset);

	    /* Get the size to be transferred from the buffer. */
	    xfersize = slos->slos_sb->sb_bsize - boff;

	    /* Check if we need to read the full block. */
	    if (uio->uio_resid < xfersize)
		xfersize = uio->uio_resid;

	    /* Check if there are enough bytes in the extent to read the block */
	    if (bytesinextent < xfersize)
		xfersize = bytesinextent;

	    /* Get the blocks from the buffer cache. */
	    error = bread(slos->slos_vp, bno, slos->slos_sb->sb_bsize, 
			    curthread->td_proc->p_ucred, &bp);
	    if (error != 0) {
		brelse(bp);
		bp = NULL;
		break;
	    }

	    /* 
	     * If we got couldn't read as much as we needed,
	     * make sure we don't read past valid data.
	     */
	    size = slos->slos_sb->sb_bsize - bp->b_resid;
	    if (size < xfersize)
		xfersize = size;

	    if (xfersize == 0)
		break;

	    
	    /* Copy over the data we need from the block buffer. */
	    if (buf_mapped(bp)) {
		error = vn_io_fault_uiomove((char *) bp->b_data + (boff & PAGE_MASK), (int) xfersize, uio);
	    } else {
		error = vn_io_fault_pgmove(bp->b_pages, boff & PAGE_MASK, (int) xfersize, uio);
	    }

	    if (error != 0)
		break;

	    /* Release the buffer without freeing it. */
	    bqrelse(bp);
	}

	/* Release the buffer without freeing it. */
	if (bp != NULL)
	    bqrelse(bp);

	
	return error;
}

/* Write to an extent beginning from offset off for len bytes. */
static int
slos_write_disk(struct slos *slos, struct slos_diskptr *diskptr, struct uio *uio)
{
	struct buf *bp;
	uint64_t boff;
	uint64_t bno;
	long xfersize;
	uint64_t bytesinextent;
	uint64_t sectorsperblock;
	int error;

	struct vnode *vp = slos->slos_vp;

	/* 
	 * There is a distinction between logical 
	 * blocks and physical sectors. Most disks
	 * expose 512 byte sectors even if they actually
	 * use 4k for compatibility reasons, and so the
	 * block numbers given to the buffer cache correspond
	 * to 512 byte increments.
	 */
	sectorsperblock = slos->slos_sb->sb_bsize / slos->slos_sb->sb_ssize;

	for (error = 0; uio->uio_resid > 0;) {

	    /* 
	     * XXX If the IO is not aligned, read the buffer from the disk. 
	     * It's OK right now, since we never partially overwrite pages, but still.
	     */

	    /* Check if the offset is still in the extent. */
	    bytesinextent = (diskptr->size * slos->slos_sb->sb_bsize) - uio->uio_offset;
	    if (bytesinextent <= 0)
		break;

	    /* Get the block number and the offset in the block. */
	    bno = (blkno(slos, uio->uio_offset) + diskptr->offset) * sectorsperblock;
	    boff = blkoff(slos, uio->uio_offset);

	    /* Get the size to be transferred from the buffer. */
	    xfersize = slos->slos_sb->sb_bsize - boff;

	    /* Check if we need to read the full block. */
	    if (uio->uio_resid < xfersize)
		xfersize = uio->uio_resid;

	    /* Check if there are enough bytes in the extent to read the block */
	    if (bytesinextent < xfersize)
		xfersize = bytesinextent;

	    bp = getblk(vp, bno, slos->slos_sb->sb_bsize, 0, 0, 0);
	    if (bp == NULL)
		break;

	    /* Copy over the data we need to the block buffer. */
	    if (buf_mapped(bp)) {
		error = vn_io_fault_uiomove((char *) bp->b_data + (boff & PAGE_MASK), (int) xfersize, uio);
	    } else {
		error = vn_io_fault_pgmove(bp->b_pages, boff & PAGE_MASK, (int) xfersize, uio);
	    }

	    if (error != 0)
		break;

	    /* Issue the write itself. */
	    bawrite(bp);

	    if (xfersize == 0)
		break;
	}

	return error;
}

int
slos_read(struct slos *slos, struct slos_diskptr *diskptr, struct uio *uio)
{
	struct uio *newuio;
	size_t extent_size;
	size_t file_offset;
	int error;  
	struct vnode *vp = slos->slos_vp;

	if (!vn_isdisk(vp, &error)) {

	    newuio = cloneuio(uio);

	    /* Get the offset in the file which we're backing the FS with.  */
	    file_offset = diskptr->offset * slos->slos_sb->sb_bsize;
	    newuio->uio_offset = file_offset + uio->uio_offset;

	    /* Truncate the size of the operation to fit into the extent. */
	    extent_size = diskptr->size * slos->slos_sb->sb_bsize;
	    newuio->uio_resid = min(uio->uio_resid, extent_size - uio->uio_offset);

	    /* Issue the read itself. */
	    error = VOP_READ(vp, newuio, 0, curthread->td_ucred);
	    /* Increment the size of the original UIO. */
	    uio->uio_resid = newuio->uio_resid;
	    uio->uio_offset = newuio->uio_offset - file_offset;

	    free(newuio, M_IOV);

	    return error;
	} else {

	    return slos_read_disk(slos, diskptr, uio);
	}
}

int
slos_write(struct slos *slos, struct slos_diskptr *diskptr, struct uio *uio)
{
	struct uio *newuio;
	size_t extent_size;
	size_t file_offset;
	int error;
	struct vnode *vp = slos->slos_vp;

	if (!vn_isdisk(vp, &error)) {
	    newuio = cloneuio(uio);

	    /* Get the offset in the file which we're backing the FS with.  */
	    file_offset = diskptr->offset * slos->slos_sb->sb_bsize;
	    newuio->uio_offset = file_offset + uio->uio_offset;

	    /* Truncate the size of the operation to fit into the extent. */
	    extent_size = diskptr->size * slos->slos_sb->sb_bsize;
	    newuio->uio_resid = min(uio->uio_resid, extent_size - uio->uio_offset);

	    /* Issue the write itself. */
	    error = VOP_WRITE(vp, newuio, 0, curthread->td_ucred);

	    /* Increment the size of the original UIO. */
	    uio->uio_resid = newuio->uio_resid;
	    uio->uio_offset = newuio->uio_offset - file_offset;

	    free(newuio, M_IOV);

	    return error;
	} else {

	    return slos_write_disk(slos, diskptr, uio);
	}
}


/* 
 * Internal function that issues a one-block transfer
 * from the disk to a contiguous buffer.
 */
static int
slos_opblk(struct slos *slos, uint64_t blkno, void *buf, int write)
{
	struct slos_diskptr sbptr;
	struct iovec aiov;
	struct uio auio;
	int error;

	sbptr = (struct slos_diskptr) {
	    .offset = blkno,
	    .size = 1,
	};

	aiov.iov_base = buf;
	aiov.iov_len = slos->slos_sb->sb_bsize;

	slos_uioinit(&auio, 0, (write != 0) ? UIO_WRITE : UIO_READ, &aiov, 1);

	if (write != 0)
	    error = slos_write(slos, &sbptr,  &auio);
	else 
	    error = slos_read(slos, &sbptr,  &auio);

	return error;
}

/* Read a block from disk into a contiguous buffer. */
int
slos_readblk(struct slos *slos, uint64_t blkno, void *buf)
{
    return slos_opblk(slos, blkno, buf, 0);
}

/* Write a block from disk into a contiguous buffer. */
int
slos_writeblk(struct slos *slos, uint64_t blkno, void *buf)
{
    return slos_opblk(slos, blkno, buf, 1);
}

/* Read the superblock of the SLOS into the in-memory struct. */
int
slos_sbread(struct slos * slos)
{
	struct slos_sb *sb;
	struct buf *bp;
	struct stat st;
	struct iovec aiov;
	struct uio auio;
	int error;

	/* If we're backed by a file, just call VOP_READ. */
	if (slos->slos_vp->v_type == VREG) {
	    sb = malloc(SLOS_FILEBLKSIZE, M_SLOS, M_WAITOK | M_ZERO);

	    /* Read the first SLOS_FILEBLKSIZE bytes. */
	    aiov.iov_base = sb;
	    aiov.iov_len = SLOS_FILEBLKSIZE;
	    slos_uioinit(&auio, 0,UIO_READ, &aiov, 1);

	    /* Issue the read. */
	    error = VOP_READ(slos->slos_vp, &auio, 0, curthread->td_ucred);
	    if (error != 0) 
		free(sb, M_SLOS);

	    /* Make the superblock visible. */
	    slos->slos_sb = sb;

	    return error;
	}

	/* Lock needed to call vn_stat. */
	error = vn_lock(slos->slos_vp, LK_EXCLUSIVE);
	if (error != 0) {
	    printf("ERROR: Locking vnode failed with %d\n", error);
	    return error;
	}

	/* 
	 * Our read and write routines depend on our superblock
	 * for information like the block size, so we can't use them.
	 * We instead do a stat call on the vnode to get it directly.
	 */
	error = vn_stat(slos->slos_vp, &st, NULL, NULL, curthread);
	VOP_UNLOCK(slos->slos_vp, 0);
	if (error != 0) {
	    return error;
	}

	sb = malloc(st.st_blksize, M_SLOS, M_WAITOK | M_ZERO);

	/* 
	 * Since our read routines use the superblock's data to find
	 * the block size, we can't use them. We do a bread instead.
	 */
	/* Get the blocks from the buffer cache. */
	error = bread(slos->slos_vp, 0, st.st_blksize, curthread->td_proc->p_ucred, &bp);
	if (error != 0) {
	    free(sb, M_SLOS);
	    return error;
	}

	memcpy(sb, bp->b_data, st.st_blksize);
	brelse(bp);
	
	if (sb->sb_magic != SLOS_MAGIC) {
	    printf("ERROR: Magic for SLOS is %lx, should be %llx\n",
		    sb->sb_magic, SLOS_MAGIC);
	    free(sb, M_SLOS);
	    return EINVAL;
	} 

	/* Make the superblock visible to the struct. */
	slos->slos_sb = sb;
	return 0;
}

/* Write the superblock back to disk. */
int
slos_sbwrite(struct slos *slos)
{
	struct slos_diskptr sbptr;
	struct iovec aiov;
	struct uio auio;
	int error;

	/* If we're backed by a file, just call VOP_READ. */
	if (slos->slos_vp->v_type == VREG) {
	    /* Write the first SLOS_FILEBLKSIZE bytes. */
	    aiov.iov_base = slos->slos_sb;
	    aiov.iov_len = SLOS_FILEBLKSIZE;
	    slos_uioinit(&auio, 0, UIO_WRITE, &aiov, 1);

	    /* Issue the write. */
	    error = VOP_WRITE(slos->slos_vp, &auio, 0, curthread->td_ucred);

	    return error ;
	}

	sbptr = (struct slos_diskptr) {
	    .offset = 0,
	    .size = 1,
	};

	aiov.iov_base = slos->slos_sb;
	aiov.iov_len = slos->slos_sb->sb_bsize;

	slos_uioinit(&auio, 0, UIO_WRITE, &aiov, 1);

	/* 
	 * Since we have the superblock in memory , we can use
	 * our routines which make use of its fields. No need
	 * for a raw block operation as in slos_sbread().
	 */
	error = slos_write(slos, &sbptr,  &auio);

	return error;
}


#ifdef SLOS_TESTS

#define	EXTENTS 16	/* Number of extents to be written */ 
#define MAXSIZE	8	/* Maximum size of each extent in blocks */
#define MAXDIST 8 	/* Maximum distance between extent offsets */
#define ITERS	10	/* Number of iterations */
#define LINELEN	80	/* Characters in each output line */

#define POISON  ('0')	/* Poison value for the buffer */

/* 
 * Extent test, used to verify that we address on-disk blocks
 * correctly. Each extent gets assigned a value with which 
 * it is filled. Extents get repeatedly read and written, so
 * if there is overlap between them the wrong values will be read.
 */
int 
slos_testio_random(void)
{
	struct slos_diskptr *extents;
	struct iovec aiov;
	struct uio auio;
	uint64_t curmax;
	size_t buflen;
	int error;
	int i, j, iter;
	char *buf;
	char *vals;
	
	extents = malloc(sizeof(*extents) * EXTENTS, M_SLOS, M_WAITOK);
	buf = malloc(sizeof(*buf) * slos->slos_sb->sb_bsize * MAXSIZE, M_SLOS, M_WAITOK);
	vals = malloc(sizeof(*vals) * EXTENTS, M_SLOS, M_WAITOK);

	/* 
	 * Set up the current lower bound for the block offset. 
	 * We set it up so that we do not include the superblock
	 * or the bootstrap allocator region.
	 */
	curmax = slos->slos_sb->sb_data.offset;

	/* Get a random chunk of the disk for each extent. */
	for (i = 0; i < EXTENTS; i++) {
	    extents[i].offset = curmax + 1;
	    extents[i].size = 1 + (random() % MAXSIZE);

	    /* Get a random letter for each extent to fill with. */
	    vals[i] = 'a' + (random() % ('z' - 'a'));

	    curmax = extents[i].offset + extents[i].size;
	}

	for (iter = 0; iter < ITERS; iter++) {

	    /* Do the initial write on each extent. */
	    for (i = 0; i < EXTENTS; i++) {
		/* Fill up the buffer with the random letter. */
		buflen = extents[i].size * slos->slos_sb->sb_bsize;
		for (int j = 0; j < buflen; j++)
		    buf[j] = vals[i];

		/* Construct the UIO. */
		aiov.iov_base = buf;
		aiov.iov_len = buflen;

		slos_uioinit(&auio, 0, UIO_WRITE, &aiov, 1);

		/* Do the actual write. */
		error = slos_write(slos->slos_vp, &extents[i], &auio);
		if (error != 0) {
		    printf("ERROR: Error %d for slos_write\n", error);
		    error = EINVAL;
		    goto out;
		
		}
	    }


	    /* Read the blocks back, making sure they are read correctly. */
	    for (i = 0; i < EXTENTS; i++) {
		/* Poison the buffer to make sure there's not stale data. */
		buflen = extents[i].size * slos->slos_sb->sb_bsize;
		for (int j = 0; j < buflen; j++)
		    buf[j] = POISON;

		/* Construct the UIO. */
		aiov.iov_base = buf;
		aiov.iov_len = buflen;

		slos_uioinit(&auio, 0, UIO_READ, &aiov, 1);

		/* Do the actual read. */
		error = slos_read(slos->slos_vp, &extents[i], &auio);
		if (error != 0) {
		    printf("ERROR: Error %d for slos_read\n", error);
		    error = EINVAL;
		    goto out;
		}

		/* Make sure the data has been written properly by reading it back. */
		for (j = 0; j < buflen; j++) {
		    if (buf[j] != vals[i]) {
			printf("ERROR: Value %c (0x%x) read at offset %d, should be %c\n", 
				buf[j], buf[j], j, vals[i]);
			error = EINVAL;
			break;
		    }
		}
	    }
	}

out:
	if (error != 0) {
	    printf("Block size: %lu\n", slos->slos_sb->sb_bsize);
	    printf("Buffer values:\n");
	    for (i = 0; i < EXTENTS; i++)
		printf("%d: %c\n", i, vals[i]);
	    printf("\n");

	    printf("Extents:\n");
	    for (i = 0; i < EXTENTS; i++)
		printf("(%d) (%lu, %lu)\n", i, extents[i].offset, extents[i].size);
	} 
	
	printf("Random IO test %s.\n", (error == 0) ? "successful" : "failed");

	free(buf, M_SLOS);
	free(vals, M_SLOS);
	free(extents, M_SLOS);

	return error;
}


/* 
 * Intraextent test, used to make sure no individual 
 * bytes get messed up. Get a random segment and fill
 * it with a random sequence of bytes, then read it
 * back to make sure they have been written exactly
 * into the disk.
 */
int 
slos_testio_intraseg(void)
{
	struct slos_diskptr extent;
	struct iovec aiov;
	struct uio auio;
	size_t buflen;
	int error;
	int i, iter;
	char *buf;
	char *vals;
	

	/* 
	 * Get a random chunk of the disk for the extent. 
	 * Get it from the data region to avoid overwriting
	 * any nodes of the allocator btree or the superblock.
	 */
	extent.offset = slos->slos_sb->sb_data.offset;
	extent.size = 1 + (random() % MAXSIZE);
	buflen = extent.size * slos->slos_sb->sb_bsize;

	buf = malloc(sizeof(*buf) * buflen,  M_SLOS, M_WAITOK);
	vals = malloc(sizeof(*vals) * buflen,  M_SLOS, M_WAITOK);

	/* Fill the buffer with random letters . */
	for (i = 0; i < buflen; i++)
	    vals[i] = 'a' + (random() % ('z' - 'a'));

	for (iter = 0; iter < ITERS; iter++) {

	    /* Copy the values into the buffer. */
	    memcpy(buf, vals, buflen);

	    /* Construct the UIO. */
	    aiov.iov_base = buf;
	    aiov.iov_len = buflen;

	    slos_uioinit(&auio, 0, UIO_WRITE, &aiov, 1);

	    /* Do the actual write. */
	    error = slos_write(slos->slos_vp, &extent, &auio);
	    if (error != 0) {
		printf("ERROR: Error %d for slos_write\n", error);
		error = EINVAL;
		goto out;
	    }
	    
	    /* Read the block back, making sure it's read correctly. */

	    /* Poison the buffer to make sure there's not stale data. */
	    bzero(buf, buflen);


	    /* Construct the UIO. */
	    aiov.iov_base = buf;
	    aiov.iov_len = buflen;

	    slos_uioinit(&auio, 0, UIO_READ, &aiov, 1);

	    /* Do the actual read. */
	    error = slos_read(slos->slos_vp, &extent, &auio);
	    if (error != 0) {
		printf("ERROR: Error %d for slos_read\n", error);
		error = EINVAL;
		goto out;
	    }

	    /* Make sure the data has been written properly by reading it back. */
	    for (i = 0; i < buflen; i++) {
		if (buf[i] != vals[i]) {
		    printf("ERROR: Value %c (0x%x) read at offset %d, should be %c\n", 
			    buf[i], buf[i], i, vals[i]);
		    error = EINVAL;
		    break;
		}
	    }
	}



out:
	if (error != 0) {
	    printf("Block size: %lu\n", slos->slos_sb->sb_bsize);
	    printf("Buffer values:\n");
	    for (i = 0; i < buflen; i++) {
		printf("%c", buf[i]);
		if (i != 0 && i % LINELEN == 0)
		    printf("\n");
	    }
	    printf("\n");

	    for (i = 0; i < buflen; i++) {
		printf("%c", vals[i]);
		if (i != 0 && i % LINELEN == 0)
		    printf("\n");
	    }
	    printf("\n");
	}

	printf("Extent IO test %s.\n", (error == 0) ? "successful" : "failed");

	free(buf, M_SLOS);
	free(vals, M_SLOS);

	return error;
}

#endif /* SLOS_TESTS */