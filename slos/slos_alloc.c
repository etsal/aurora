#include <sys/types.h>
#include <sys/param.h>
#include <sys/buf.h>
#include <sys/mount.h>
#include <sys/uio.h>
#include <sys/vnode.h>

#include <slos.h>
#include <slos_alloc.h>
#include <slos_inode.h>
#include <slsfs.h>
#include <vtree.h>

#include "debug.h"
#include "slos_subr.h"
#include "slsfs_buf.h"

#include "slos_vbtree.h"

#define NEWOSDSIZE (30)


// Blocks are always allocated where offset is the block offset (not byte
// addressable) and size is the size in bytes
#define KB ((size_t)1024)
#define MB ((size_t)((KB) * (size_t)1024))
#define GB ((size_t)((MB) * (size_t)1024))

static const size_t CHUNK_SIZE = (16 * MB);
static const size_t WAL_CHUNK = (10 * GB);
static diskptr_t wal_allocations;
/*
 * Generic uint64_t comparison function.
 */
int
uint64_t_comp(const void *k1, const void *k2)
{
	const uint64_t *key1 = (const uint64_t *)k1;
	const uint64_t *key2 = (const uint64_t *)k2;

	if (*key1 > *key2) {
		return 1;
	} else if (*key1 < *key2) {
		return -1;
	}
	return 0;
}

/*
 * General hack to work with postgres, for all the WALs in the system make one
 * large allocation. XXX: Fix the bug which causes corruption of a WAL
 * allocation bumping into a file. Add more tests to make sure each allocation
 * is what it should be!
 */
int
slos_blkalloc_wal(struct slos *slos, size_t bytes, diskptr_t *ptr)
{
	uint64_t blksize = BLKSIZE(slos);
	size_t rounded = roundup(bytes, blksize);
	size_t blocks = rounded / blksize;

	if (wal_allocations.size >= blocks * blksize) {
		ptr->offset = wal_allocations.offset;
		ptr->size = rounded;
		ptr->epoch = slos->slos_sb->sb_epoch;
    ptr->flags = 0;
		wal_allocations.offset += blocks;
		wal_allocations.size -= rounded;
		return (0);
	}
	panic("Not enough wal space");
	return (-1);
}

static int
fast_path(struct slos *slos, uint64_t bytes, diskptr_t *ptr)
{
	uint64_t blksize = BLKSIZE(slos);
	diskptr_t *chunk = &slos->slos_alloc.chunk;
	size_t rounded = roundup(bytes, blksize);
	size_t blocks = rounded / blksize;

	if (chunk->size >= blocks * blksize) {
		ptr->offset = chunk->offset;
		ptr->size = rounded;
		ptr->epoch = slos->slos_sb->sb_epoch;
    ptr->flags = 0;
		chunk->offset += blocks;
		chunk->size -= rounded;
		return (0);
	}

	return (-1);
}

static int
slos_blkalloc_large_unlocked(struct slos *slos, size_t size, diskptr_t *ptr)
{
	uint64_t temp;
	uint64_t fullsize;
	uint64_t off;
	uint64_t location;

	uint64_t blksize = BLKSIZE(slos);
	uint64_t asked = roundup(size, blksize);
  fullsize = asked;
	int error;

	error = vtree_ge(STREE(slos), &fullsize, &off);
	if (error != 0) {
		return (ENOSPC);
	}

	location = off;

	/* Temporarily remove the extent from the allocator. */
	error = vtree_delete(STREE(slos), fullsize, &off);
	if (error) {
		panic("Problem removing element in allocation");
	}

	KASSERT(fullsize >= asked, ("Simple allocation first"));

	error = vtree_delete(OTREE(slos), off, &temp);
	if (error) {
		panic("Failure in allocation - %d", error);
	}

	KASSERT(temp == fullsize, ("Should be reverse mappings"));
	fullsize -= asked;
	off += (asked / blksize);

	error = vtree_insert(STREE(slos), fullsize, &off);
	if (error) {
		panic("Problem removing element in allocation");
	}

	error = vtree_insert(OTREE(slos), off, &fullsize);
	if (error) {
		panic("Problem removing element in allocation");
	}

	ptr->offset = location;
	ptr->size = asked;
	ptr->epoch = slos->slos_sb->sb_epoch;
  ptr->flags = 0;

	return (0);
}

int
slos_blkalloc_large(struct slos *slos, size_t size, diskptr_t *ptr)
{
	int error;

	BTREE_LOCK(STREE(slos), LK_EXCLUSIVE);
	BTREE_LOCK(OTREE(slos), LK_EXCLUSIVE);
	error = slos_blkalloc_large_unlocked(slos, size, ptr);
	BTREE_UNLOCK(OTREE(slos), 0);
	BTREE_UNLOCK(STREE(slos), 0);

	return (error);
}

/*
 * Generic block allocator for the SLOS. We never explicitly free.
 */
int
slos_blkalloc(struct slos *slos, size_t bytes, diskptr_t *ptr)
{
	diskptr_t *chunk = &slos->slos_alloc.chunk;
	int error;

	/* Get an extent large enough to cover the allocation. */
	BTREE_LOCK(STREE(slos), LK_EXCLUSIVE);
	while (true) {
		if (!fast_path(slos, bytes, ptr)) {
	    BTREE_UNLOCK(STREE(slos), 0);
			return (0);
		}

		BTREE_LOCK(OTREE(slos), LK_EXCLUSIVE);
		if (chunk->size > bytes) {
			BTREE_UNLOCK(OTREE(slos), 0);
			continue;
		}

		error = slos_blkalloc_large_unlocked(slos, CHUNK_SIZE,
		    &slos->slos_alloc.chunk);
		if (error != 0) {
			panic("Problem allocating %d\n", error);
		}

		BTREE_UNLOCK(OTREE(slos), 0);
	}
}

/* Returns the amount of free bytes in the SLOS. */
/*
 * HACKS HACKS HACKS: Without GC what we're left with
 * is a bump allocator. We just read the size of the
 * leftover chunks to find how much space is left.
 */
int slos_freebytes(SYSCTL_HANDLER_ARGS)
{
	uint64_t freebytes;
  uint64_t value;
	uint64_t asked = 0;
	int error;

	error = vtree_ge(STREE(&slos), &asked, &value);
	if (error != 0) {
    return (ENOSPC);
	}

	error = SYSCTL_OUT(req, &value, sizeof(freebytes));

	return (error);
}

/*
 * Initialize the in-memory allocator state at mount time.
 */
int
slos_allocator_init(struct slos *slos, int new_start)
{
	struct slos_node *offt;
	struct slos_node *sizet;
	diskptr_t ptr;
	uint64_t off;
	uint64_t wal_off;
	uint64_t total;
	int error;

	/*
	 * If epoch is -1 then this is the first time we mounted this device,
	 * this means we have to manually allocate, since we know how much space
	 * we used on disk (just the superblock array) we just take that offset
	 * and bump it to allocate.
	 */

	size_t offset = ((NUMSBS * slos->slos_sb->sb_ssize) /
			    slos->slos_sb->sb_bsize) +
	    1;
	// Checksum tree is allocated first.
  uint64_t blks_per_tree = VTREE_BLKSZ / BLKSIZE(slos);
	offset += 1 + blks_per_tree;
	if (new_start) {
		DEBUG1(
		    "Bootstrapping Allocator for first time startup starting at offset %lu",
		    offset);
		/*
		 * When initing the allocator, we have to start out by just
		 * bump allocating the initial setup of the trees,  we bump
		 * each tree by two, one for the inode itself, and the second
		 * for the root of the tree.
		 */
		fbtree_sysinit(slos, offset, &slos->slos_sb->sb_allocoffset);
		offset += 1 + blks_per_tree;
		fbtree_sysinit(slos, offset, &slos->slos_sb->sb_allocsize);
		offset += 1 + blks_per_tree;
	}

	uint64_t offbl = slos->slos_sb->sb_allocoffset.offset;
	uint64_t sizebl = slos->slos_sb->sb_allocsize.offset;

	DEBUG("Initing Allocator");
	/* Create the in-memory vnodes from the on-disk state. */
	error = slos_svpimport(slos, offbl, true, &offt);
  MPASS(error == 0);

  /* DISABLING COW FOR NOW
   * TODO: Remake allocator to allow for COW allocations
   * Have to readjust the trees to use uint64_t as values 
   */
  vtree_create(&offt->sn_vtree, defaultops, offt->sn_ino.ino_btree, 
      sizeof(uint64_t), VTREE_NOCOW, &inode_btree_rootchange, offt);


	error = slos_svpimport(slos, sizebl, true, &sizet);
  MPASS(error == 0);
  /* Have to readjust the trees to use uint64_t as values */
  vtree_create(&sizet->sn_vtree, defaultops, sizet->sn_ino.ino_btree, 
      sizeof(uint64_t), VTREE_NOCOW, &inode_btree_rootchange, sizet);

	MPASS(offt && sizet);
	// This is a remount so we must free the allocator slos_nodes as they
	// are not cleaned up in the vflush as they have no associated vnode to
	// them.
	if (slos->slos_alloc.a_offset != NULL) {
		slos_vpfree(slos, slos->slos_alloc.a_offset);
	}
	slos->slos_alloc.a_offset = offt;

	if (slos->slos_alloc.a_size != NULL) {
		slos_vpfree(slos, slos->slos_alloc.a_size);
	}
	slos->slos_alloc.a_size = sizet;

	slos->slos_alloc.chunk.offset = 0;
	slos->slos_alloc.chunk.size = 0;

	
	// New tree add the initial amount allocations.  Im just making some
	// constant just makes it easier
	/*
	 * If the allocator is uninitialized, populate the trees with the
	 * initial values.
	 * TODO Error handling for fbtree_insert().
	 */
	if (new_start) {
		DEBUG("First time start up for allocator");
		off = offset;
		total = slos->slos_sb->sb_size - (offset * BLKSIZE(slos));
		wal_off = off + ((total - WAL_CHUNK) / BLKSIZE(slos));
		wal_allocations.size = WAL_CHUNK;
		wal_allocations.offset = wal_off;
		total -= WAL_CHUNK;

    vtree_insert(OTREE(slos), off, &total);
    vtree_insert(STREE(slos), total, &off);

		/*
		 * Carve of a region from the beginning of the device.
		 * We have statically allocated some of these blocks using
		 * the userspace tool, so we retroactively log that allocation
		 * using the call below.
		 * TODO: More dynamic allocation that does exactly the
		 * allocations done?
		 */
		slos_blkalloc(slos, NEWOSDSIZE * BLKSIZE(slos), &ptr);
		DEBUG("First time start up for allocator done");
	}

	return (0);
};

/*
 * Initialize the in-memory allocator state at mount time.
 */
int
slos_allocator_uninit(struct slos *slos)
{
	slos_vpfree(slos, slos->slos_alloc.a_offset);
	slos->slos_alloc.a_offset = NULL;
	slos_vpfree(slos, slos->slos_alloc.a_size);
	slos->slos_alloc.a_size = NULL;

	return (0);
}

/*
 * Flush the allocator state to disk.
 */
int
slos_allocator_sync(struct slos *slos, struct slos_sb *newsb)
{
	/* int error; */
	/* struct buf *bp; */
	/* diskptr_t ptr; */

	/* This is just arbitrary right now.  We need a better way of
	 * calculating the dirty blocks we over allocate because we need to
	 * reallocate parents as well even though they may not be dirty, we are
	 * changing the location of one or more of their children so we must
	 * also mark them as CoW
	 *
	 * This could be done by going through each of the dirtylsit (just leave
	 * nodes) crawl upwards and mark something as new copy on write and then
	 * cnt unique times you've done it. Once youve reached something that
	 * has been marked you can just stop that iteration. As you know all the
	 * parents have been marked
	 */
  vtree_checkpoint(OTREE(slos));
  vtree_checkpoint(STREE(slos));
	/* int total_allocations = (vtree_dirty_cnt(OTREE(slos)) * 5) + */
	/*     (vtree_dirty_cnt(STREE(slos)) * 5) + 2; */

	/* DEBUG2("off(%p), size(%p)", slos->slos_alloc.a_offset->sn_fdev, */
	/*     slos->slos_alloc.a_size->sn_fdev); */
	/* // Update the inodes and dirty them as well */
	/* bp = getblk(slos->slos_alloc.a_offset->sn_fdev, ptr.offset, */
	/*     BLKSIZE(slos), 0, 0, 0); */
	/* MPASS(bp); */
	/* struct slos_inode *ino = &slos->slos_alloc.a_offset->sn_ino; */
	/* ino->ino_blk = ptr.offset; */
	/* slos->slos_sb->sb_allocoffset = ptr; */
	/* ino->ino_btree = (diskptr_t) { OTREE(slos)->bt_root, BLKSIZE(slos) }; */
	/* memcpy(bp->b_data, ino, sizeof(struct slos_inode)); */
	/* bwrite(bp); */
	/* ptr.offset += 1; */

	/* bp = getblk(slos->slos_alloc.a_size->sn_fdev, ptr.offset, BLKSIZE(slos), */
	/*     0, 0, 0); */
	/* MPASS(bp); */
	/* ino = &slos->slos_alloc.a_size->sn_ino; */
	/* ino->ino_blk = ptr.offset; */
	/* ino->ino_btree = (diskptr_t) { STREE(slos)->bt_root, BLKSIZE(slos) }; */
	/* slos->slos_sb->sb_allocsize = ptr; */
	/* memcpy(bp->b_data, ino, sizeof(struct slos_inode)); */
	/* bwrite(bp); */

	// XXX Check how much is left and free them - but since we dont have
	// free yet leave this for later.
	return (0);
}
