#include <sys/types.h>
#include <sys/malloc.h>
#include <sys/param.h>
#include <sys/buf.h>
#include <sys/bufobj.h>
#include <sys/vnode.h>

#include "vtree.h"
#include "slos_vbtree.h"
#include "slos.h"
#include "slsfs.h"

#define BINARY_SEARCH_CUTOFF (64)

MALLOC_DEFINE(M_SLOS_VTREE, "vtree", "Vtree interface");

struct vtreeops btreeops;
struct vtreeops *defaultops;

static int
binary_search(kvp* arr, size_t size, uint64_t key)
{

  /* In many cases linear search is faster then binary as it
   * can take advantage of streaming prefetching so have a cut
   * off where we switch to linear search */
  if (size <= BINARY_SEARCH_CUTOFF) {
    for (int i = 0; i < size; i++) {
      if (arr[i].key >= key) {
        return i;
      }
    }

    return size;
  }

  size_t low = 0;
  size_t high = size;
  while (low < high) {
    size_t mid = low + (high - low) / 2;
    if (arr[mid].key >= key) {
      high = mid;
    } else {
      low = mid + 1;
    }
  }
  if (low >= size) {
    return size;
  } else {
    return low;
  }
}

void
vtree_empty_wal(vtree* tree)
{
  kvp kv;
  int error;

  if (tree->v_flags & VTREE_WITHWAL) {
    /* Checkpoint should also clear out the wal hopefully before this point */
    if (tree->v_flags & VTREE_WALBULK) {
      VTREE_BULKINSERT(tree, tree->v_wal, tree->v_cur_wal_idx);
    } else {
      for (int i = 0; i < tree->v_cur_wal_idx; i++) {
        kv = tree->v_wal[i];
        error = VTREE_INSERT(tree, kv.key, kv.data);
        KASSERT(error == 0, ("Bulk inserting failing"));
      }
    }

    tree->v_cur_wal_idx = 0;
  }
}

int
vtree_create(struct vtree *vtree, struct vtreeops* ops, 
    diskptr_t root, size_t ks, uint32_t v_flags)
{
  struct vnode *vp;
  int error;

  KASSERT(root.size == VTREE_BLKSZ, ("Wrong size node for tree"));

  vtree->v_flags = v_flags;

	error = getnewvnode("SLSFS Fake VNode", slos.slsfs_mount,
	    &slsfs_vnodeops, &vp);

	if (error) {
		panic("Problem getting fake vnode for device\n");
	}

	/* Set up the necessary backend state to be able to do IOs to the
	 * device. */
	vp->v_bufobj.bo_ops = &bufops_slsfs;
	vp->v_bufobj.bo_bsize = slos.slos_sb->sb_bsize;
	vp->v_type = VCHR;
	vp->v_data = vp;
	vp->v_vflag |= VV_SYSTEM;


  if (v_flags & VTREE_WITHWAL) {
    vtree->v_wal = (kvp*)malloc(VTREE_WALSIZE, M_SLOS_VTREE, M_WAITOK | M_ZERO);
  } else {
    vtree->v_wal = NULL;
  }
  vtree->v_ops = ops;
  vtree->v_cur_wal_idx = 0;
  vtree->v_vp = vp;
  VTREE_INIT(vtree, root, ks);
  KASSERT(((btree_t)vtree->v_tree)->tr_ptr.size == VTREE_BLKSZ, ("Wrong size node for tree"));
  
	lockinit(&vtree->bt_lock, PVFS, "Vtree Lock", 0, LK_CANRECURSE);

  return error;
}

static inline void
wal_insert(vtree* tree, size_t keysize, uint64_t key, void* data)
{
  int idx = binary_search(tree->v_wal, tree->v_cur_wal_idx, key);
  int num_to_move = tree->v_cur_wal_idx - idx;
  if (tree->v_wal[idx].key == key) {
    memcpy(&tree->v_wal[idx].data, data, keysize);
    return;
  }

  if (num_to_move > 0) {
    memmove(
      &tree->v_wal[idx + 1], &tree->v_wal[idx], num_to_move * sizeof(kvp));
  }

  tree->v_wal[idx].key = key;
  memcpy(&tree->v_wal[idx].data, data, keysize);
  tree->v_cur_wal_idx += 1;
}

int
vtree_insert(vtree* tree, uint64_t key, void* value)
{
  size_t ks = VTREE_GETKEYSIZE(tree);
  if (tree->v_flags & VTREE_WITHWAL) {
    /* Checkpoint should also clear out the wal hopefully before this point */
    if (tree->v_cur_wal_idx == VTREE_MAXWAL) {
      vtree_empty_wal(tree);
    }

    wal_insert(tree, ks, key, value);
    return 0;
  }

  return VTREE_INSERT(tree, key, value);
}

int
vtree_bulkinsert(vtree* tree, kvp* keyvalues, size_t len)
{
  return VTREE_BULKINSERT(tree, keyvalues, len);
}

int
vtree_delete(vtree* tree, uint64_t key, void* value)
{
  return VTREE_DELETE(tree, key, value);
}

int
vtree_find(vtree* tree, uint64_t key, void* value)
{
  return VTREE_FIND(tree, key, value);
}

int
vtree_ge(vtree* tree, uint64_t* key, void* value)
{
  return VTREE_GE(tree, key, value);
}

int
vtree_rangequery(vtree* tree,
                 uint64_t key_low,
                 uint64_t key_max,
                 kvp* results,
                 size_t results_max)
{
  return VTREE_RANGEQUERY(tree, key_low, key_max, results, results_max);
}

diskptr_t
vtree_checkpoint(vtree* tree)
{
  vtree_empty_wal(tree);
  return VTREE_CHECKPOINT(tree);
}

size_t
vtree_dirty_cnt(vtree *tree)
{
  return tree->v_vp->v_bufobj.bo_dirty.bv_cnt;
}

void
vtree_free(vtree *tree)
{
  printf("VTree free [%p]\n", tree);
	tree->v_vp->v_data = NULL;
	vnode_destroy_vobject(tree->v_vp);
	tree->v_vp->v_op = &dead_vnodeops;

  if (tree->v_flags & VTREE_WITHWAL) {
    free(tree->v_wal, M_SLOS_VTREE);
  }
}

void
vtree_interface_init()
{
  btreeops.vtree_init = &btree_init;
  btreeops.vtree_insert = &btree_insert;
  btreeops.vtree_bulkinsert = &btree_bulkinsert;
  btreeops.vtree_delete = &btree_delete;

  btreeops.vtree_find = &btree_find;
  btreeops.vtree_ge = &btree_greater_equal;
  btreeops.vtree_rangequery = &btree_rangequery;

  btreeops.vtree_checkpoint = &btree_checkpoint;
  btreeops.vtree_getkeysize = &btree_getkeysize;

  /* Set the default */
  defaultops = &btreeops;
}
