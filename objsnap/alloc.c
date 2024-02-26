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

#include "alloc.h"
#include "objsnap_internal.h"
#include "vtree.h"
#include "btree.h"

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
    bdirty(ino_bp);
    brelse(ino_bp);

    return (0);
}

int 
flush() {
    struct buf *bp, *nbp;
    struct bufobj *bo = &osdata.os_vp->v_bufobj;

    BO_LOCK(bo);

    TAILQ_FOREACH_SAFE(bp, &bo->bo_dirty.bv_hd, b_bobufs, nbp) {

        if (BUF_LOCK(bp, LK_EXCLUSIVE | LK_NOWAIT, NULL) == 0)
            BO_UNLOCK(bo);
        if (bp->b_flags & B_DELWRI) {
            bremfree(bp);
        }
        bwrite(bp);
        BO_LOCK(bo);
    }

    BO_UNLOCK(bo);

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
    newinode->i_treeptr = allocate_block();
    newinode->i_version = 0;
    newinode->i_cnt = 0;

    struct objsnap_vnode *vnode = INDEX_TO_VNODE(newinode->i_index);
    vnode->v_inode = newinode;

    // We must allocate the btree first and place it in our inode structures
    btree_t btree = malloc(sizeof(struct btree), M_OBJSNAP, M_WAITOK);
    vnode->v_tree = vtree_create(btree, &btreeops, 0);
    VTREE_INIT(&vnode->v_tree, osdata.os_vp, 
        newinode->i_treeptr, sizeof(diskptr_t));

    // Initialize ondisk root block
    error = bread(osdata.os_vp, DEVICE_BLOCK_NUM(newinode->i_treeptr), 
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