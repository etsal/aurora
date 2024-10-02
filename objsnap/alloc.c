#include <sys/param.h>
#include <sys/condvar.h>
#include <sys/kernel.h>
#include <sys/limits.h>
#include <sys/lock.h>
#include <sys/lockmgr.h>
#include <sys/malloc.h>
#include <sys/mutex.h>
#include <sys/uuid.h>
#include <sys/rwlock.h>
#include <sys/bufobj.h>
#include <sys/buf.h>
#include <machine/atomic.h>

#include <vm/vm.h>
#include <vm/uma.h>
#include <vm/vm_object.h>
#include <geom/geom.h>
#include <geom/geom_vfs.h>

#include <objsnap_ioctl.h>

#include "objsnap_internal.h"
#include "arraylist.h"
#include "alloc.h"
#include "vtree.h"
#include "btree.h"
#include "array.h"

const enum obj_alloctype obj_alloctype = OBJALLOC_CHUNK;

struct allocator alloc;

// We need to allocate inodes and jazz in a SSD block size (256 MB), or rather 
// the WALs should definately be allocated serially
void
allocator_init()
{
	bzero(&alloc, sizeof(struct allocator));
	alloc.alloc_size_total_blocks = superblock.super_size;
	alloc.alloc_starting_offset = (2 * superblock.super_max_inodes) + MAX_WAL_ENTRIES + 2;

	lockinit(&alloc.alloc_lk, 0, "Objsnap Syncer Lock", 0, 0); 
	alloc.alloc_bsize = BLOCKSIZE;

	// Start the offset after inodes and wal thread list
	// Every inode points to two objects
	uint32_t left = alloc.alloc_size_total_blocks - alloc.alloc_starting_offset;

	oa_init(&alloc.alloc_impl, alloc.alloc_starting_offset, left);

	printf("Initial allocator State:\n");
	oa_print(&alloc.alloc_impl);

	// Every inode points to two objects
	alloc.alloc_base = (2 * superblock.super_max_inodes) + 1;
	printf("Allocator base: %lu\n", alloc.alloc_base);
	alloc.alloc_walptr_head = 0; 
	alloc.alloc_walptr_tail = 0;
};

void 
allocator_destroy()
{
    oa_print(&alloc.alloc_impl);
    oa_destroy(&alloc.alloc_impl);
}

int
objsnap_blkalloc_wal(obj_diskptr_t *ptr)
{
    uint64_t before;
    int check_behind;

    lockmgr(&alloc.alloc_lk, LK_EXCLUSIVE, 0);
    size_t curhead = alloc.alloc_walptr_head;
    size_t curtail = alloc.alloc_walptr_tail;
    check_behind = ((curhead + 1) % MAX_WAL_ENTRIES) == curtail;
    while (check_behind) {
    	OS_START(LOCKANDCOPY, &before);
    	lockmgr(&alloc.alloc_lk, LK_RELEASE, 0);
	pause_sbt("combiner wait", 10 * SBT_1US, 0 ,0);
    	lockmgr(&alloc.alloc_lk, LK_EXCLUSIVE, 0);
        curhead = alloc.alloc_walptr_head;
        curtail = alloc.alloc_walptr_tail;
        check_behind = ((curhead + 1) % MAX_WAL_ENTRIES) == curtail;
    	OS_STOP(LOCKANDCOPY, &before);
    }
    
    ptr->offset = alloc.alloc_walptr_head + alloc.alloc_base;
    ptr->size = 1; 

    alloc.alloc_walptr_head = (alloc.alloc_walptr_head + 1) % MAX_WAL_ENTRIES;
    // Release it
    lockmgr(&alloc.alloc_lk, LK_RELEASE, 0);


    return (0);
}

int
allocate_txn_block(struct objsnap_txn *txn, int tid)
{
    uint64_t before;
    int error;

    OS_START(ALLOCATE, &before);
    error = oa_alloc_txn(&alloc.alloc_impl, tid, txn);
    if (error) {
        panic("Problem allocating!");
    }
    if (txn->d_ptr.offset <= (superblock.super_max_inodes + MAX_WAL_ENTRIES + 2)) {
	    printf("ERROR: PTR TOO EARLY INCORRECT LOCATION OVERWRITE"
		"FOR NOW %u\n", txn->d_ptr.offset);
    }
    OS_STOP(ALLOCATE, &before);

    return (0);
}

int
allocate_system_block(obj_diskptr_t *ptr)
{
    uint64_t before;
    int error;
    OS_START(ALLOCATE, &before);
    error = oa_alloc_system(&alloc.alloc_impl, ptr);
    if (error) {
        panic("Problem allocating!");
    }
    if (ptr->offset <= (superblock.super_max_inodes + MAX_WAL_ENTRIES + 2)) {
	    printf("ERROR: PTR TOO EARLY INCORRECT LOCATION OVERWRITE FOR NOW %u\n", ptr->offset);
    }
    OS_STOP(ALLOCATE, &before);

    return (0);
}

void 
free_block_system(obj_diskptr_t ptr)
{
    oa_free_system(&alloc.alloc_impl, ptr);
}

void 
free_block_data(obj_diskptr_t ptr)
{
    oa_free_data(&alloc.alloc_impl, ptr);
}

int 
write_ondisk_inode(osinode_t *inode)
{

    struct buf *ino_bp;
    // We don't need to hold the locks for VCHR vnodes. 
    ino_bp = getblk(osdata.os_vp, DEVICE_BLOCK_NUM(inode->i_index), 
        BLOCKSIZE, 0, 0, 0);

    ino_bp->b_blkno = DEVICE_BLOCK_NUM(inode->i_index);
    ino_bp->b_lblkno = DEVICE_BLOCK_NUM(inode->i_index);
	ino_bp->b_iooffset = dbtob(ino_bp->b_blkno);

    memcpy(ino_bp->b_data, inode, BLOCKSIZE);
    bdwrite(ino_bp);

    return (0);
}

static int 
flush() {
    struct buf *bp = NULL; // *nbp;
    struct bufobj *bo = &osdata.os_vp->v_bufobj;

    BO_BDFLUSH(bo, bp);

    return (0);
}

// We allocate in groups of two to support copy on write transactions,
// our copy on write system always goes to the sister tree to push
// changes that way we never change data in place. The same is
// done with superblocks.
//
// It should be noted that we assume object creation is rare and 
// designing it this way means we keep things flat in the object store.
osinode_t *allocate_inode()
{
    struct buf *super_bp;
    int error = 0;

    osinode_t *newinode = malloc(sizeof(osinode_t), M_OBJSNAP, M_WAITOK);
    newinode->i_index = atomic_fetchadd_int(&superblock.super_next, 2);
    allocate_system_block(&newinode->i_treeptr);
    newinode->i_version = 0;
    newinode->i_cnt = 0;

    struct objsnap_vnode *vnode = INDEX_TO_VNODE(newinode->i_index);
    vnode->v_inode = newinode;

    // We must allocate the btree first and place it in our inode structures
    btree_t btree = btree_create();
    vnode->v_tree = vtree_create(btree, &arrops, 0);
    VTREE_INIT(&vnode->v_tree, osdata.os_vp, 
        newinode->i_treeptr, sizeof(obj_diskptr_t));
    printf("Root Inode at %u\n", newinode->i_treeptr.offset);

    // Initialize ondisk root block
    error = bread(osdata.os_vp, DEVICE_BLOCK_NUM(newinode->i_treeptr.offset), 
        BLOCKSIZE, NOCRED, &super_bp);

    // We just dirty, they have created but not checkpointed so don't need to write here.
    bzero(super_bp->b_data, BLOCKSIZE);
    bdirty(super_bp);
    brelse(super_bp);
    LOCK_SUPER();

    if ((error = write_ondisk_inode(newinode)) != 0) {
        newinode->i_index = -1;
        goto allocate_inode_done;
    }
    
    // Increment and write the sister tree
    newinode->i_index += 1;

    if ((error = write_ondisk_inode(newinode)) != 0) {
        newinode->i_index = -1;
        goto allocate_inode_done;
    }

    index_t blk = superblock.super_blk ? 0 : 1;
    // Now we have to write the super block either at 0 or 1
    error = bread(osdata.os_vp, DEVICE_BLOCK_NUM(blk), 
        BLOCKSIZE, NOCRED, &super_bp);
    if (error) {
        printf("Error with writing sister super block %d", error);
        newinode->i_index = -1;
        goto allocate_inode_done;
    }

    // Make sure to update our in-memory copy.
    superblock.super_blk = blk;

    memcpy(super_bp->b_data, &superblock, sizeof(superblock));

    bdirty(super_bp);
    brelse(super_bp);

    flush();

    // Decrement to original index.
    newinode->i_index -= 1;

    vnode->v_magic = OBJMAGIC;
    vnode->v_state = VALID;

allocate_inode_done:

    UNLOCK_SUPER();
    if (error) {
        free(newinode, M_OBJSNAP);
        vnode->v_inode = NULL;
        vnode->v_state = VNULL;
        return NULL;
    }

    return newinode;
}

void
garbage_collect(size_t numblocks)
{
	oa_gc(&alloc.alloc_impl, numblocks);
}
