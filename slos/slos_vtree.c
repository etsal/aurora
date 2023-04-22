#include <sys/types.h>
#include <sys/param.h>
#include <sys/buf.h>
#include <sys/bufobj.h>
#include <sys/vnode.h>

#include "vtree.h"

#define BINARY_SEARCH_CUTOFF (64)

static MALLOC_DEFINE(M_SLOS_VTREE, "vtree", "SLOS Virtual Tree Interface");

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

struct vtree
vtree_create(void* tree, struct vtreeops* ops, uint32_t v_flags)
{
  struct vtree vtree;
  struct vnode *vp;
  int error;

  vtree.v_tree = tree;
  vtree.v_flags = v_flags;

	error = getnewvnode("SLSFS Fake VNode", slos.slsfs_mount,
	    &dead_vnodeops, &vp);

	if (error) {
		panic("Problem getting fake vnode for device\n");
	}

	/* Set up the necessary backend state to be able to do IOs to the
	 * device. */
	vp->v_bufobj.bo_ops = &bufops_vtree;
	vp->v_bufobj.bo_bsize = slos.slos_sb->sb_bsize;
	vp->v_type = VCHR;
	vp->v_data = vp;
	vp->v_vflag |= VV_SYSTEM;


  if (v_flags & VTREE_WITHWAL) {
    vtree.v_wal = (kvp*)malloc(VTREE_WALSIZE, M_SLOS_VTREE, M_WAITOK | M_ZERO);
  }
  vtree.v_ops = ops;
  vtree.v_cur_wal_idx = 0;
  vtree.v_vp = vp;

  return vtree;
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
