#include <sys/param.h>
#include <sys/bio.h>
#include <sys/buf.h>
#include <sys/kernel.h>
#include <sys/malloc.h>
#include <sys/mount.h>
#include <sys/sbuf.h>
#include <sys/sysctl.h>
#include <sys/time.h>
#include <sys/ucred.h>
#include <sys/uio.h>
#include <sys/vnode.h>

#include <vm/uma.h>

#include <slos.h>
#include <slos_bnode.h>
#include <slos_btree.h>
#include <slos_inode.h>
#include <slos_io.h>
#include <slsfs.h>
#include <slsfs_buf.h>

#include <btree.h>

#include "debug.h"
#include "slos_alloc.h"
#include "slos_subr.h"
#include "slsfs_buf.h"

#include "vtree.h"

static MALLOC_DEFINE(M_SLOS_INO, "slos inodes", "SLOSI");

struct unrhdr *slsid_unr;
uma_zone_t slos_node_zone;
struct sysctl_ctx_list slos_ctx;
uint64_t slos_bytes_opened;
static int slos_count_opened_bytes;

#ifdef INVARIANTS
static void
slos_node_dtor(void *mem, int size, void *arg)
{
	struct slos_node *node = (struct slos_node *)mem;

	mtx_lock(&node->sn_mtx);
	mtx_unlock(&node->sn_mtx);
}
#endif

static int
slos_node_init(void *mem, int size, int rflags)
{
	struct slos_node *node = (struct slos_node *)mem;

	mtx_init(&node->sn_mtx, "slosvno", NULL, MTX_DEF);

	return (0);
}

static void
slos_node_fini(void *mem, int size)
{
	struct slos_node *node = (struct slos_node *)mem;

	/* Free its resources. */
	mtx_lock(&node->sn_mtx);
	mtx_destroy(&node->sn_mtx);
}

int
slos_init(void)
{
  vtree_interface_init();

	struct sysctl_oid *root;
	int error;

	error = slos_io_init();
	if (error != 0)
		return (error);

	slos_node_zone = uma_zcreate("SLOS node zone", sizeof(struct slos_node),
	    NULL,
#ifdef INVARIANTS
	    slos_node_dtor,
#else
	    NULL,
#endif
	    slos_node_init, slos_node_fini, 0, 0);
	if (slos_node_zone == NULL) {
		slos_io_uninit();
		return (ENOMEM);
	}

	bzero(&slos, sizeof(struct slos));
	lockinit(&slos.slos_lock, PVFS, "sloslock", VLKTIMEOUT, LK_NOSHARE);

	sysctl_ctx_init(&slos_ctx);
	root = SYSCTL_ADD_ROOT_NODE(&slos_ctx, OID_AUTO, "aurora_slos",
	    CTLFLAG_RW, 0,
	    "Aurora object store statistics and configuration variables");

	(void)SYSCTL_ADD_INT(&slos_ctx, SYSCTL_CHILDREN(root), OID_AUTO,
	    "checksum_enabled", CTLFLAG_RW, &checksum_enabled, 0,
	    "Checksum enabled");

	(void)SYSCTL_ADD_U64(&slos_ctx, SYSCTL_CHILDREN(root), OID_AUTO,
	    "checkpointtime", CTLFLAG_RW, &checkpointtime, 0,
	    "Checkpoint every X ms");

	(void)SYSCTL_ADD_U64(&slos_ctx, SYSCTL_CHILDREN(root), OID_AUTO,
	    "bytes_opened", CTLFLAG_RD, &slos_bytes_opened, 0,
	    "Total byte count of opened files");

	(void)SYSCTL_ADD_INT(&slos_ctx, SYSCTL_CHILDREN(root), OID_AUTO,
	    "count_opened_bytes", CTLFLAG_RW, &slos_count_opened_bytes, 0,
	    "Count size of opened files in bytes");

	(void)SYSCTL_ADD_U64(&slos_ctx, SYSCTL_CHILDREN(root), OID_AUTO,
	    "io_initiated", CTLFLAG_RD, &slos_io_initiated, 0,
	    "Direct buffer IOs initiated");
	(void)SYSCTL_ADD_U64(&slos_ctx, SYSCTL_CHILDREN(root), OID_AUTO,
	    "io_done", CTLFLAG_RD, &slos_io_done, 0, "Direct buffer IOs done");

	/* Get a new unique identifier generator. */
	slsid_unr = new_unrhdr(SLOS_SYSTEM_MAX, INT_MAX, NULL);

	/* The constructor never fails. */
	KASSERT(slsid_unr != NULL, ("slsid unr creation failed"));

	return (0);
}

int
slos_uninit(void)
{
	/* Destroy the identifier generator. */
	clean_unrhdr(slsid_unr);
	clear_unrhdr(slsid_unr);
	delete_unrhdr(slsid_unr);
	slsid_unr = NULL;

	/* Destroy the SLOS struct lock. */
	lockdestroy(&slos.slos_lock);

	sysctl_ctx_free(&slos_ctx);
	uma_zdestroy(slos_node_zone);
	return (0);
}

static int
compare_vnode_t(const void *k1, const void *k2)
{
	const size_t *key1 = (const size_t *)k1;
	const size_t *key2 = (const size_t *)k2;

	if (*key1 > *key2) {
		return 1;
	} else if (*key1 < *key2) {
		return -1;
	}

	return (0);
}

/*
 * Measure the bytes of data occupied by an imported file.
 * Ignores the data occupied by the backing btrees.
 */
static int
slos_svpsize(struct slos_node *svp)
{
	/* struct slos_diskptr ptr; */
	/* struct fnode_iter iter; */
	/* uint64_t offset = 0; */
	size_t bytecount = 0;
	/* int error; */

	/* Start from the beginning of the file. */
	/* error = fbtree_keymin_iter(tree, &offset, &iter); */
	/* if (error != 0) */
	/* 	return (error); */

	/* /1* Each key-value pair is an extent. *1/ */
	/* for (; !ITER_ISNULL(iter); ITER_NEXT(iter)) { */
	/* 	/1* Get size from the physical disk pointer. *1/ */
	/* 	ptr = ITER_VAL_T(iter, diskptr_t); */
	/* 	bytecount += ptr.size; */
	/* } */

	atomic_add_64(&slos_bytes_opened, bytecount);

	return (0);
}

int
inode_btree_rootchange(void *ctx, diskptr_t ptr) {
  struct slos_node *svp = (struct slos_node *)ctx;
  svp->sn_ino.ino_btree = ptr;
  return 0;
}

/*
 * Create the in-memory inode from the on-disk inode.
 * The inode needs to have no existing vnode in memory.
 */
int
slos_svpimport(
    struct slos *slos, uint64_t svpid, bool system, struct slos_node **svpp)
{
	int error;
	struct slos_node *svp = NULL;
	struct slos_inode *ino;
  diskptr_t ptr;
	struct buf *bp = NULL;

	/* Read the inode from disk. */

	DEBUG("Creating slos_node in memory");
	svp = uma_zalloc(slos_node_zone, M_WAITOK);
	ino = &svp->sn_ino;

	printf("Importing inode for %s %lu\n", system ? "block" : "OID", svpid);
	/*
	 * System inodes are read from set locations in the SLOS.
	 * The rest are retrieved from the inode btree.
	 */
	if (system) {
		error = slsfs_devbread(slos, svpid, BLKSIZE(slos), &bp);
    MPASS(error == 0);
		if (error != 0)
			goto error;
	} else {
		VOP_LOCK(slos->slsfs_inodes, LK_EXCLUSIVE);
    /* Check to make sure we dont have the buffer */
    printf("LOOKUP %lu\n", svpid);
    error = slsfs_lookupbln(SLSVP(slos->slsfs_inodes), svpid, &ptr);
    if (error) {
      printf("LOOKUP FAILED %lu\n", svpid);
		  VOP_UNLOCK(slos->slsfs_inodes, 0);
      error = ENOENT;
      goto error;
    }

    printf("Retrieve BUF %lu\n", svpid);
    error = slsfs_retrieve_buf(slos->slsfs_inodes, svpid * BLKSIZE(slos), BLKSIZE(slos), UIO_READ, 0, &bp);
    printf("Retrieve BUF %lu %d\n", svpid, error);
		VOP_UNLOCK(slos->slsfs_inodes, 0);
		if (error != 0)
			goto error;
	}

	memcpy(ino, bp->b_data, sizeof(struct slos_inode));
	bqrelse(bp);

	if (ino->ino_magic != SLOS_IMAGIC) {
		error = EINVAL;
		goto error;
	}

  KASSERT(ino->ino_btree.size == VTREE_BLKSZ, ("Block size for tree is incorrect"));
  error = vtree_create(&svp->sn_vtree, defaultops,
      ino->ino_btree, sizeof(diskptr_t), 0, &inode_btree_rootchange, svp);

	/*
	 * Move each field separately, translating between the two.
	 * The refcount will be incremented by the caller.
	 */
	svp->sn_slos = slos;

	/* Measure the size in bytes of the imported inode. */

	/*
	 * CAREFUL: Calling this every svpimport means we
	 * measure a file multiple times if the userspace
	 * access pattern causes us to constantly
	 * deallocate/reallocate a vnode for it.
   * TODO: Update btrees to keep track of number of keys
	 */
	/* if (!system && slos_count_opened_bytes) { */
	/* 	error = slos_svpsize(svp); */
	/* 	if (error) */
	/* 		printf("Error %d for slos_svpsize\n", error); */
	/* } */

	*svpp = svp;

	return (0);

error:

	DEBUG1("failed with %d", error);
	uma_zfree(slos_node_zone, svp);
	return (error);
}

/* Free an in-memory vnode. */
void
slos_vpfree(struct slos *slos, struct slos_node *vp)
{
  vtree_checkpoint(&vp->sn_vtree);
	vtree_free(&vp->sn_vtree);
	uma_zfree(slos_node_zone, vp);
}

/* Create an inode with the given ID and write it to the disk. */
int
slos_icreate(struct slos *slos, uint64_t svpid, mode_t mode)
{
	int error;
	struct slos_inode ino;
	struct buf *bp;
	diskptr_t ptr;

	struct vnode *root_vp = slos->slsfs_inodes;
	struct slos_node *svp = SLSVP(root_vp);
	size_t blksize = IOSIZE(svp);

	// For now we will use the blkno for our svpids
	VOP_LOCK(root_vp, LK_EXCLUSIVE);
  printf("FIND ID %lu\n", svpid);
	error = vtree_find(&svp->sn_vtree, svpid, &ptr);
	if (error == 0) {
    printf("FIND ID EXISTS %lu\n", svpid);
	  VOP_UNLOCK(root_vp, LK_EXCLUSIVE);
		return (EEXIST);
	}

  printf("Creating Inode %lu\n", svpid);

  ptr.size = blksize;
  ptr.offset = 0;
  ptr.flags = 0;
	error = vtree_insert(&svp->sn_vtree, svpid, &ptr);
  MPASS(error == 0);
	VOP_UNLOCK(root_vp, LK_EXCLUSIVE);

	ino.ino_flags = IN_UPDATE | IN_ACCESS | IN_CHANGE | IN_CREATE;

	slos_updatetime(&ino);

	ino.ino_pid = svpid;
	ino.ino_nlink = 1;
	ino.ino_flags = 0;
	ino.ino_blk = 0;
	ino.ino_magic = SLOS_IMAGIC;
	ino.ino_mode = mode;
	ino.ino_asize = 0;
	ino.ino_size = 0;
	ino.ino_blocks = 0;
	ino.ino_rstat.type = 0;
	ino.ino_rstat.len = 0;
	ino.ino_wal_segment.size = 0;
	ino.ino_wal_segment.offset = 0;
	ino.ino_wal_segment.epoch = 0;

	error = slos_blkalloc(slos, VTREE_BLKSZ, &ptr);
	if (error) {
		return (error);
	}

  /* TODO Delay this allocation */
	slsfs_devbread(slos, ptr.offset, VTREE_BLKSZ, &bp);
	MPASS(bp);
	bzero(bp->b_data, bp->b_bcount);
	slsfs_bdirty(bp);
	ino.ino_btree = ptr;

   
  VOP_LOCK(root_vp, LK_EXCLUSIVE);
  slsfs_retrieve_buf(root_vp, svpid * blksize, blksize, UIO_WRITE, 0, &bp);
  VOP_UNLOCK(root_vp, 0);
  memcpy(bp->b_data, &ino, sizeof(struct slos_inode));
  slsfs_bdirty(bp);

	// We will use this private pointer as a way to change this ino with
	// the proper ino blk number when it syncs
	DEBUG1("Created inode %lu", svpid);

	return (0);
}

int
slos_iremove(struct slos *slos, uint64_t pid)
{
	/* Not yet implemented. */
	return (ENOSYS);
}

/*
 * Retrieve an in-memory SLOS inode, or create one from disk if not present.
 */
int
slos_iopen(struct slos *slos, uint64_t oid, struct slos_node **svpp)
{
	int error;
	struct slos_node *svp = NULL;

	printf("Opening Inode %lu", oid);

	oid = OIDTOSLSID(oid);

	/*
	 * We should not hold this essentially global lock in this object. We
	 * can run into a scenario where when we search through the btree, we
	 * end up having to sleep while
	 * we wait for the buffer, current fix is to allow sleeping on this lock
	 */
	if (oid == SLOS_INODES_ROOT) {
		error = slos_svpimport(
		    slos, slos->slos_sb->sb_root.offset, true, &svp);
		if (error != 0) {
			SLOS_UNLOCK(slos);
			return (error);
		}
	} else {
		/* Create a vnode for the inode. */
		error = slos_svpimport(slos, oid, false, &svp);
		if (error)
			return (error);
	}


	*svpp = svp;

	return (0);
}

// We assume that svp is under the VOP_LOCK, we currently just check if the svp
// being updated is the root itself
int
slos_updatetime(struct slos_inode *ino)
{
	struct timespec ts;

	if ((ino->ino_flags & (IN_ACCESS | IN_CHANGE | IN_UPDATE)) == 0) {
		return (0);
	}

	vfs_timestamp(&ts);

	if (ino->ino_flags & IN_ACCESS) {
		ino->ino_atime = ts.tv_sec;
		ino->ino_atime_nsec = ts.tv_nsec;
	}

	if (ino->ino_flags & IN_UPDATE) {
		ino->ino_mtime = ts.tv_sec;
		ino->ino_mtime_nsec = ts.tv_nsec;
	}

	if (ino->ino_flags & IN_CHANGE) {
		ino->ino_ctime = ts.tv_sec;
		ino->ino_ctime_nsec = ts.tv_nsec;
	}

	if (ino->ino_flags & IN_CREATE) {
		ino->ino_birthtime = ts.tv_sec;
		ino->ino_birthtime_nsec = ts.tv_nsec;
	}

	ino->ino_flags &= ~(IN_ACCESS | IN_CHANGE | IN_UPDATE | IN_CREATE);

	return (0);
}

int
slos_update(struct slos_node *svp)
{
	slos_updatetime(&svp->sn_ino);
	return (0);
}

int
initialize_inode(struct slos *slos, uint64_t pid, diskptr_t *p)
{
	struct buf *bp;
	int error;

	struct slos_inode ino = {};
	// We can use the fake device from the allocators they should be inited
	struct vnode *fdev = slos->slos_alloc.a_offset->sn_vtree.v_vp;

	error = slos_blkalloc(slos, BLKSIZE(slos), p);
	MPASS(error == 0);

	slos_updatetime(&ino);

	ino.ino_blk = p->offset;
	ino.ino_magic = SLOS_IMAGIC;
	ino.ino_pid = pid;
	ino.ino_gid = 0;
	ino.ino_uid = 0;

	bp = getblk(fdev, ino.ino_blk, BLKSIZE(slos), 0, 0, 0);
	MPASS(bp);

	error = slos_blkalloc(slos, VTREE_BLKSZ, &ino.ino_btree);
	MPASS(error == 0);
	memcpy(bp->b_data, &ino, sizeof(struct slos_inode));
	bwrite(bp);

	bp = getblk(fdev, ino.ino_btree.offset, VTREE_BLKSZ, 0, 0, 0);
	MPASS(bp);

	vfs_bio_clrbuf(bp);
	bwrite(bp);

	return (0);
}
