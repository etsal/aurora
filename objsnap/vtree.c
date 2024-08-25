#include <sys/param.h>
#include <sys/bitstring.h>
#include <sys/condvar.h>
#include <sys/fcntl.h>
#include <sys/file.h>
#include <sys/filedesc.h>
#include <sys/lock.h>
#include <sys/mutex.h>
#include <sys/proc.h>
#include <sys/queue.h>
#include <sys/sdt.h>
#include <sys/stat.h>
#include <sys/syscallsubr.h>
#include <sys/sysctl.h>
#include <sys/vnode.h>
#include <sys/buf.h>

#include "vtree.h"


#define BINARY_SEARCH_CUTOFF (64)

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
  int error = 0;

  if (tree->v_flags & VTREE_WITHWAL) {
    /* Checkpoint should also clear out the wal hopefully before this point */
    if (tree->v_flags & VTREE_WALBULK) {
      VTREE_BULKINSERT(tree, tree->v_wal, tree->v_cur_wal_idx);
    } else {
      for (int i = 0; i < tree->v_cur_wal_idx; i++) {
        kv = tree->v_wal[i];
        error = VTREE_INSERT(tree, kv.key, kv.data);
      }
    }

    tree->v_cur_wal_idx = 0;
  }
}

vtree
vtree_create(void* tree, struct vtreeops* ops, uint32_t v_flags)
{
  vtree vtree;
  vtree.v_tree = tree;
  vtree.v_flags = v_flags;
  if (v_flags & VTREE_WITHWAL) {
    vtree.v_wal = (kvp*)malloc(VTREE_WALSIZE, M_OBJSNAP, M_WAITOK);
  }
  vtree.v_ops = ops;
  vtree.v_cur_wal_idx = 0;

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

obj_diskptr_t
vtree_checkpoint(vtree* tree)
{
  vtree_empty_wal(tree);
  return VTREE_CHECKPOINT(tree);
}
