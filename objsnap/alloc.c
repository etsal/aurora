#include <sys/param.h>
#include <sys/condvar.h>
#include <sys/kernel.h>
#include <sys/limits.h>
#include <sys/lock.h>
#include <sys/lockmgr.h>
#include <sys/malloc.h>
#include <sys/mutex.h>
#include <sys/uuid.h>
#include <sys/buf.h>
#include <machine/atomic.h>

#include <vm/vm.h>
#include <vm/uma.h>
#include <vm/vm_object.h>

#include "alloc.h"
#include "objsnap_internal.h"

struct allocator alloc;

void
allocator_init()
{
	alloc.alloc_size_total_blocks = (superblock.super_size / BLOCKSIZE);
	alloc.alloc_bsize = BLOCKSIZE;
	alloc.alloc_next_block = superblock.super_max_inodes + 2;
};

diskptr_t allocate_block()
{
    return (diskptr_t)atomic_fetchadd_64(&alloc.alloc_next_block, 1);
}

static int 
init_ondisk_inode(osinode_t *inode)
{
    int error;
    struct buf *ino_bp;

    // We don't need to hold the locks for VCHR vnodes.
    error = bread(osdata.os_vp, DEVICE_BLOCK_NUM(inode->i_index), 
        BLOCKSIZE, NOCRED, &ino_bp);
    if (error) {
        printf("Error retrieving bread 1 %d\n", error);
        return error;
    }
    memcpy(ino_bp->b_data, ino_bp, sizeof(osinode_t));
    bwrite(ino_bp);

    return (0);
}

// We allocate in groups of two to support copy on write transactions,
// our copy on write system always goes to the sister tree to push
// changes that way we never change data in place. The same is
// done with superblocks.
//
// It should be noted that we assume object creation is rare and 
// designing it this way means we keep things flat in the object store.
int allocate_inode(osinode_t *newinode)
{
    struct buf *super_bp;
    int error = 0;
    newinode->i_index = atomic_fetchadd_64(&superblock.super_next, 2);
    newinode->i_treeptr = NULLDISKPTR;

    LOCK_SUPER();

    if ((error = init_ondisk_inode(newinode)) != 0) {
        newinode->i_index = -1;
        goto allocate_inode_done;
    }
    
    // Increment and write the sister tree
    newinode->i_index += 1;

    if ((error = init_ondisk_inode(newinode)) != 0) {
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

    bwrite(super_bp);

    // Make sure to update our in-memory copy.
    superblock.super_blk = blk;

    // Decrement to original index.
    newinode->i_index -= 1;

allocate_inode_done:

    UNLOCK_SUPER();

    return (error);
}