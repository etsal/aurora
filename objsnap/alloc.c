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

#include "objsnap_internal.h"
#include "alloc.h"
#include "vtree.h"
#include "btree.h"

struct allocator alloc;

// We need to allocate inodes and jazz in a SSD block size (256 MB), or rather 
// the WALs should definately be allocated serially
void
allocator_init()
{
    bzero(&alloc, sizeof(struct allocator));
    alloc.alloc_size_total_blocks = superblock.super_size;
    mtx_init(&alloc.alloc_lk, "Objsnap Syncer Lock", NULL, MTX_DEF);
    alloc.alloc_bsize = BLOCKSIZE;

    // Start the offset after inodes and wal thread list
    uint32_t offset = superblock.super_max_inodes + MAX_WAL_ENTRIES + 2;
    uint32_t left = alloc.alloc_size_total_blocks - offset;

    ba_init(&alloc.alloc_impl, offset, left);

    alloc.alloc_base = superblock.super_max_inodes + 2;
    alloc.alloc_walptr_head = 0; 
    alloc.alloc_walptr_tail = 0;
};

void 
allocator_destroy()
{
    ba_destroy(&alloc.alloc_impl);
}

diskptr_t
objsnap_blkalloc_wal()
{
    uint64_t before;
    int check_behind;
    diskptr_t ptr;

    mtx_lock(&alloc.alloc_lk);
    size_t curhead = alloc.alloc_walptr_head;
    size_t curtail = alloc.alloc_walptr_tail;
    check_behind = ((curhead + 1) % MAX_WAL_ENTRIES) == curtail;
    while (check_behind) {
    	OS_START(LOCKANDCOPY, &before);
    	mtx_unlock(&alloc.alloc_lk);
	pause_sbt("combiner wait", 10 * SBT_1US, 0 ,0);
    	mtx_lock(&alloc.alloc_lk);
        curhead = alloc.alloc_walptr_head;
        curtail = alloc.alloc_walptr_tail;
        check_behind = ((curhead + 1) % MAX_WAL_ENTRIES) == curtail;
    	OS_STOP(LOCKANDCOPY, &before);
    }
    
    ptr.offset = alloc.alloc_walptr_head;
    ptr.size = 1; 

    alloc.alloc_walptr_head = (alloc.alloc_walptr_head + 1) % MAX_WAL_ENTRIES;

    mtx_unlock(&alloc.alloc_lk);

    ptr.offset += alloc.alloc_base;

    return ptr;
}

diskptr_t 
allocate_block(int i)
{
    diskptr_t ptr;
    int error;
    error = ba_alloc(&alloc.alloc_impl, i, &ptr);
    if (error) {
        panic("Problem allocating!");
    }

    return ptr;
}

void 
free_block(diskptr_t ptr)
{
    ba_free(&alloc.alloc_impl, ptr);
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
flush(void) {
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
    newinode->i_treeptr = allocate_block(1);
    newinode->i_version = 0;
    newinode->i_cnt = 0;

    struct objsnap_vnode *vnode = INDEX_TO_VNODE(newinode->i_index);
    vnode->v_inode = newinode;

    // We must allocate the btree first and place it in our inode structures
    btree_t btree = btree_create();
    vnode->v_tree = vtree_create(btree, &btreeops, 0);
    VTREE_INIT(&vnode->v_tree, osdata.os_vp, 
        newinode->i_treeptr, sizeof(diskptr_t));

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

