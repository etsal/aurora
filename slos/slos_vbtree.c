#include "slos_vbtree.h"

#include <sys/types.h>
#include <sys/param.h>
#include <sys/buf.h>
#include <sys/rwlock.h>
#include <sys/bufobj.h>
#include <sys/vnode.h>
#include <sys/mount.h>

#include "slos.h"
#include "slos_alloc.h"
#include "vtree.h"

#define INDEX_NULL ((uint16_t)-1)
//#define DEBUG (1)

#define BT_ISCOW(node) ((node)->n_epoch < slos.slos_sb->sb_epoch)
#define BT_DONT_COW(node) ((node)->n_epoch == slos.slos_sb->sb_epoch)
#define BT_COW_DISABLED(node) ((node)->n_tree->tr_flags & VTREE_NOCOW)

typedef struct bpath
{
  uint64_t p_len;
  uint16_t p_indexes[BT_MAX_PATH_SIZE];
  btnode p_nodes[BT_MAX_PATH_SIZE];
  uint8_t p_cur;
} bpath;

typedef bpath* bpath_t;

#define BINARY_SEARCH_CUTOFF (64)

static int
binary_search(uint64_t* arr, size_t size, uint64_t key)
{
  /* In many cases linear search is faster then binary as it
   * can take advantage of streaming prefetching so have a cut
   * off where we switch to linear search */
  if (size <= BINARY_SEARCH_CUTOFF) {
    for (int i = 0; i < size; i++) {
      if (arr[i] >= key) {
        return i;
      }
    }

    return size;
  }

  size_t low = 0;
  size_t high = size;
  while (low < high) {
    size_t mid = low + (high - low) / 2;
    if (arr[mid] >= key) {
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

static void
btnode_print(btnode_t node)
{
  if (node->n_len > BT_MAX_KEYS) {
    printf("Corrupted node\n");
    node->n_len = BT_MAX_KEYS;
  }

  printf("\nNode %lx %u\n", node->n_ptr.offset, node->n_len);
  for (int i = 0; i < node->n_len; i += 10) {
    printf("%d: ", i);
    for (int j = i; j < i + 10 && j < node->n_len; j++) {
      printf(" %lx |", node->n_keys[j]);
    }
    printf("\n");
  }
  if (BT_ISINNER(node)) {
    printf("\n====CHILDREN=====\n");
    for (int i = 0; i < (node->n_len + 1); i += 10) {
      printf("%d: ", i);
      for (int j = i; j < i + 10 && (j < (node->n_len + 1)); j++) {
        printf(" %lx |", ((diskptr_t*)(&node->n_ch[j]))->offset);
      }
      printf("\n");
    }
  }
  printf("\n==================\n");
}

static inline void
btnode_dirty(btnode_t node)
{
  bdirty(node->n_bp);
}

static void
path_print(bpath_t path)
{
  for (int i = 0; i < path->p_len; i++) {
    btnode_print(&path->p_nodes[i]);
  }
}

static void
btnode_init(btnode_t node, btree_t tree, diskptr_t ptr, int lk_flags)
{
  struct buf* bp;
  int error;
  KASSERT(ptr.size == VTREE_BLKSZ, ("Incorrect size for init"));
  KASSERT(ptr.offset != 0, ("Should be an offset"));

  error = bread(tree->tr_vp, ptr.offset, ptr.size, curthread->td_ucred, &bp);
  MPASS(error == 0);
  MPASS(bp->b_bcount == ptr.size);

  node->n_bp = bp;
  node->n_data = (btdata_t)bp->b_data;
  node->n_tree = tree;
  node->n_ptr = ptr;
  node->o_flags = 0;
}

/* Node is locked exclusively on create */
static void
btnode_create(btnode_t node, btree_t tree, uint8_t type)
{
  diskptr_t ptr;
  struct buf *bp = NULL;
  int error;

  KASSERT(tree != NULL, ("Tree should never be null\n"));
  error = slos_blkalloc(&slos, VTREE_BLKSZ, &ptr);
  MPASS(error == 0);
#ifdef DEBUG
  printf("BTnode create %lu %lu\n", ptr.offset, ptr.size);
#endif
  bp = getblk(tree->tr_vp, ptr.offset, VTREE_BLKSZ, 0, 0, 0);
  MPASS(bp != NULL);

  MPASS(bp->b_bcount == ptr.size);

  vfs_bio_clrbuf(bp);
  node->n_bp = bp;
  /* Data and bp always setup first as all other field point into it */
  node->n_data = (btdata_t)bp->b_data;
  node->n_epoch = ptr.epoch;
  node->n_tree = tree;
  node->n_ptr = ptr;
  node->n_type = type;
  node->n_len = 0;
  node->o_flags = 0;
}

/* Caller must check to see if parent */
static uint16_t
path_getindex(bpath_t path)
{
  return path->p_indexes[path->p_cur];
}

static inline btnode_t
path_getcur(bpath_t path)
{
  return &path->p_nodes[path->p_cur];
}

/*
 * Will iterater through the path and perform COW on all entries within the path
 */
static void
path_cow(bpath_t path, uint64_t epoch)
{
  btnode tmp;
  btnode_t parent = NULL;
  struct buf *oldbp;
  int oldlength = 0;

  int idx;
  btree_t tree = path_getcur(path)->n_tree;
  /* 
   * We hold all the locks of the path exclusively so we can change the parent
   */
  for (int i = 0; i < path->p_len; i++) {
    /* Check if node is not already COWed */
    tmp = path->p_nodes[i];
    if (!IS_SCANNED(&tmp) && (tmp.n_epoch < epoch)) {

      /* Grab our index in our parent */
      if (i > 0) {
        parent = &path->p_nodes[i - 1];
        idx = path->p_indexes[i];
      }

      oldbp  = tmp.n_bp;
      oldlength = tmp.n_len;
      KASSERT(oldlength < BT_MAX_KEYS, ("Too large!"));

      btnode_create(&path->p_nodes[i], tmp.n_tree, tmp.n_type);

      /* 
       * Update to proper epoch etc, stored in the buffer (bp), so we dont clobber it when 
       * we copy 
       */

      KASSERT(tmp.n_epoch < path->p_nodes[i].n_epoch, ("NEW PTR SHOULD BE IN LARGER EPOCH"));

      tmp.n_epoch = path->p_nodes[i].n_epoch;

      KASSERT(tmp.n_epoch >= epoch, ("NEW PTR SHOULD BE IN LARGER EPOCH 1"));
      /* Perform the copy of data or however we choose to transfer it over */
      KASSERT(path->p_nodes[i].n_bp->b_data != oldbp->b_data, ("Data buffers should be different"));

      memcpy(path->p_nodes[i].n_bp->b_data, oldbp->b_data, VTREE_BLKSZ);

      KASSERT(path->p_nodes[i].n_epoch >= epoch, ("NEW PTR SHOULD BE IN LARGER EPOCH 2"));
      KASSERT(oldlength == path->p_nodes[i].n_len, ("Should be the same number of entries!"));
      KASSERT(path->p_nodes[i].n_ptr.offset != tmp.n_ptr.offset, ("Different locations!"));
      KASSERT(path->p_nodes[i].n_ptr.offset != 0, ("Should not equal zero!"));

      /* Update our parent to know of the change */
      if (i > 0) {
        KASSERT(path->p_nodes[i].n_ptr.offset != 0, ("Do not copy a bad ptr"));
        KASSERT(((diskptr_t *)&parent->n_ch[idx])->offset == oldbp->b_lblkno, ("Replacing the correct one"));
        memcpy(&parent->n_ch[idx], &path->p_nodes[i].n_ptr, sizeof(diskptr_t));
      } else {
        /* Make sure we update our root ptr in our main tree datastructure */
        tree->tr_ptr = path->p_nodes[i].n_ptr;
      }

      /* Write any pending writes and then release the buffer */
      oldbp->b_flags |= B_INVAL;
      brelse(oldbp);

      /* Turn of cow on the node and dirty the node */
      btnode_dirty(&path->p_nodes[i]);
      MARK_SCANNED(&path->p_nodes[i]);
    }
  }

  for (int i = 0; i < path->p_len; i++) {
      CLR_SCAN(&path->p_nodes[i]);
  }

}

static inline void
path_add(bpath_t path, btree_t tree, diskptr_t ptr, uint16_t cidx, int lk_flags)
{
  btnode_init(&path->p_nodes[path->p_len], tree, ptr, lk_flags);
  path->p_indexes[path->p_len] = cidx;
  path->p_cur = path->p_len;
  path->p_len += 1;
}

static inline void
path_unacquire(bpath_t path, int acquire_as)
{
  struct buf *bp;
  for (int i = 0; i < path->p_len; i++) {
    bp = path->p_nodes[i].n_bp;
    if (bp->b_flags & B_DELWRI)
      bawrite(bp);
    else
      bqrelse(bp);
  }
}

static inline void
path_backtrack(bpath_t path)
{
  if (path->p_cur > 0) {
    path->p_cur -= 1;
  }
}

static inline btnode_t
path_fixup_cur_parent(bpath_t path, btnode_t parent)
{
  int num_to_move = BT_MAX_PATH_SIZE - path->p_cur - 1;
  memmove(&path->p_nodes[path->p_cur + 1],
          &path->p_nodes[path->p_cur],
          num_to_move * sizeof(btnode));
  memcpy(&path->p_nodes[path->p_cur], parent, sizeof(btnode));
  path->p_cur += 1;
  path->p_len += 1;
  return path_getcur(path);
}

static inline void
path_copy(bpath_t dst, bpath_t src)
{
  memcpy(dst->p_nodes, src->p_nodes, src->p_len * sizeof(btnode));
}

static btnode_t
btnode_go_deeper(bpath_t path, uint64_t key, int acquire_as)
{
  int idx = 0;
  int cidx;

  btnode_t cur = path_getcur(path);

  idx = binary_search(cur->n_keys, cur->n_len, key);
  /* Larger then every element */
  if (idx == cur->n_len) {
    cidx = cur->n_len;
  } else {
    uint64_t keyflag = cur->n_keys[idx];
    if (key > keyflag) {
      cidx = idx + 1;
    } else {
      cidx = idx;
    }
  }

  KASSERT(cidx < (cur->n_len + 1), ("Too large of child index"));
  diskptr_t ptr = *(diskptr_t*)&cur->n_ch[cidx];
  if (ptr.offset == 0) {
    printf("TRYING TO ACCESS CHILD %d\n", cidx);
    path_print(path);
    KASSERT(false, ("Corruption?"));
  }

  path_add(path, cur->n_tree, ptr, cidx, acquire_as);

  return path_getcur(path);
}

/*
 * Finds the node which should hold param KEY.
 */
static btnode_t
btnode_find_child(bpath_t path, uint64_t key, int acquire_as)
{
  btnode_t cur = path_getcur(path);
  while (BT_ISINNER(cur)) {
    cur = btnode_go_deeper(path, key, acquire_as);
  };

  return cur;
}

/*
 * Within a node, find the key thats greater than or equal
 * to value in param KEY.
 * Function will overwrite key with any key that is found
 * so call should ensure to save the real key before calling
 * and check after
 */
static int
btnode_find_ge(btree_t tree, uint64_t* key, void* value, int acquire_as)
{
  btnode_t node;
  int idx;
  bpath path;
  path.p_len = 0;
  path_add(&path, tree, tree->tr_ptr, INDEX_NULL, acquire_as);

  node = btnode_find_child(&path, *key, acquire_as);

  if (node->n_len > BT_MAX_KEYS) {
    path_print(&path);
    KASSERT(false, ("Bad node"));
  }

  idx = binary_search(node->n_keys, node->n_len, *key);
  /* Is there no key here */
  if (idx >= node->n_len) {
    path_unacquire(&path, acquire_as);
    return -1;
  }

#ifdef DEBUG
  printf("[Find] %lu in %lu\n", *key, node->n_ptr.offset);
#endif

  *key = node->n_keys[idx];
  // key is greater than all elements in the array
  memcpy(value, &node->n_ch[idx + 1], tree->tr_vs);

  path_unacquire(&path, acquire_as);

  return 0;
}

static inline btnode_t
path_parent(bpath_t path)
{
  if (path->p_cur == 0) {
    return NULL;
  }

  return &path->p_nodes[path->p_cur - 1];
}

static void
btnode_inner_insert(btnode_t node, int idx, uint64_t key, diskptr_t value)
{
  KASSERT(BT_ISINNER(node), ("Node should be an inner node"));
  KASSERT(((diskptr_t *)&node->n_ch[0])->offset != 0, ("BAD OFFSET BEFORE INSERT"));
  if (node->n_len) {
    int num_to_move = node->n_len - idx;
    memmove(
      &node->n_keys[idx + 1], &node->n_keys[idx], num_to_move * sizeof(key));
    memmove(&node->n_ch[idx + 2],
            &node->n_ch[idx + 1],
            num_to_move * BT_MAX_VALUE_SIZE);
  }

  node->n_keys[idx] = key;
  memcpy(&node->n_ch[idx + 1], &value, sizeof(value));
  node->n_len += 1;
  KASSERT(((diskptr_t *)&node->n_ch[0])->offset != 0, ("BAD OFFSET AFTER INSERT"));

  btnode_dirty(node);
}

static void
btnode_split(bpath_t path)
{
  int idx;
  btnode_t node = path_getcur(path);
  btnode_t pptr = path_parent(path);
  btnode parent;
  btnode right_child;

#ifdef DEBUG
  printf("[Split]\n");
#endif
  if (!BT_ISLEAF(node)) {
    KASSERT(((diskptr_t *)&node->n_ch[0])->offset != 0, ("BAD OFFSET BEFORE COPY"));
  }

  /* We are the root */
  if (pptr == NULL) {
    btnode_create(&parent, node->n_tree, BT_INNER);
    /* Set our current node to the child of our new parent */
    memcpy(&parent.n_ch[0], &node->n_ptr, sizeof(diskptr_t));
    KASSERT(((diskptr_t *)&parent.n_ch[0])->offset != 0, ("BAD OFFSET PARENT"));

    node = path_fixup_cur_parent(path, &parent);

    /* Fixup root parent ptr in the tree */
    node->n_tree->tr_ptr = parent.n_ptr;

    idx = 0;
  } else {
    parent = *pptr;
    KASSERT(((diskptr_t *)&parent.n_ch[0])->offset != 0, ("BAD OFFSET GOGO PARENT"));
    idx = path_getindex(path);
  }

  btnode_create(&right_child, node->n_tree, node->n_type);

  right_child.n_len = SPLIT_KEYS;

  if (BT_ISLEAF(node)) {
    node->n_len = SPLIT_KEYS;
  } else {
    node->n_len = SPLIT_KEYS - 1;
  }

  uint64_t split_key = node->n_keys[SPLIT_KEYS - 1];

  memcpy(&right_child.n_keys[0],
         &node->n_keys[SPLIT_KEYS],
         SPLIT_KEYS * sizeof(uint64_t));
  if (BT_ISLEAF(node))
    memcpy(&right_child.n_ch[0],
           &node->n_ch[SPLIT_KEYS],
           (SPLIT_KEYS + 1) * BT_MAX_VALUE_SIZE);
  else {
    memcpy(&right_child.n_ch[0],
           &node->n_ch[SPLIT_KEYS],
           (SPLIT_KEYS + 1) * BT_MAX_VALUE_SIZE);
    KASSERT(((diskptr_t *)&node->n_ch[0])->offset != 0, ("BAD OFFSET NODE"));
    KASSERT(((diskptr_t *)&right_child.n_ch[0])->offset != 0, ("BAD OFFSET RIGHT CHILD"));
  }

  /* Setting the pivot key here, with SPLIT_KEYS - 1, means elements to the
   * right must be strictly greater
   */
  btnode_inner_insert(&parent, idx, split_key, right_child.n_ptr);

  /* Unlock the right child and dirty the children*/
  btnode_dirty(&right_child);
  btnode_dirty(node);

  bqrelse(right_child.n_bp);

  if (parent.n_len == BT_MAX_KEYS) {
    KASSERT(((diskptr_t *)&parent.n_ch[0])->offset != 0, ("BAD OFFSET PARENT SPLIT"));
    path_backtrack(path);
    btnode_split(path);
  }
}

static void
btnode_leaf_insert(btnode_t node, int idx, uint64_t key, void* value)
{
  KASSERT(BT_ISLEAF(node), ("Node should be a leaf"));
  int num_to_move = node->n_len - idx;
  if (num_to_move > 0) {
    memmove(
      &node->n_keys[idx + 1], &node->n_keys[idx], num_to_move * sizeof(key));
  }

  if (num_to_move > 0) {
    memmove(&node->n_ch[idx + 2],
            &node->n_ch[idx + 1],
            num_to_move * BT_MAX_VALUE_SIZE);
  }

#ifdef DEBUG
  printf("[Insert] %lu at %d in node %lu\n", key, idx, node->n_ptr.offset);
#endif

  node->n_keys[idx] = key;
  memcpy(&node->n_ch[idx + 1], value, BT_VALSZ(node));
  node->n_len += 1;

  btnode_dirty(node);
}

static void
btnode_leaf_update(btnode_t node, int idx, void* value)
{
  KASSERT(BT_ISLEAF(node), ("Node should be a leaf"));
  memcpy(&node->n_ch[idx + 1], value, BT_VALSZ(node));
  btnode_dirty(node);

  /* False update - key value is actually a new key
   * This captures the case where the key is 0 for example
   */
  if (idx == node->n_len) {
    node->n_len += 1;
  }
}

static int
btnode_insert(bpath_t path, uint64_t key, void* value)
{
  int idx;

  btnode_find_child(path, key, LK_EXCLUSIVE);
  btnode_t node = path_getcur(path);
  idx = binary_search(node->n_keys, node->n_len, key);

  /*
   * If node is COW'd this means the entire path leading
   * to this node must be COW'd
   * */
  if (BT_ISCOW(node) && !BT_COW_DISABLED(node)) {
    path_cow(path, slos.slos_sb->sb_epoch);
  }

  /* Update over insert */
  if (node->n_keys[idx] == key) {
    btnode_leaf_update(node, idx, value);
  } else {
    btnode_leaf_insert(node, idx, key, value);
  }

  if (node->n_len == BT_MAX_KEYS) {
    btnode_split(path);
  }

  return 0;
}
static void
btnode_leaf_delete(btnode_t node, int idx, void* value)
{
  KASSERT(BT_ISLEAF(node), ("Should be a leaf"));
  int num_to_move = node->n_len - idx;

  if (value != NULL)
    memcpy(value, &node->n_ch[idx + 1], BT_VALSZ(node));

  if (num_to_move > 0) {
    memmove(&node->n_keys[idx],
            &node->n_keys[idx + 1],
            num_to_move * sizeof(uint64_t));
  }

  if (num_to_move > 0) {
    memmove(&node->n_ch[idx + 1],
            &node->n_ch[idx + 2],
            num_to_move * BT_MAX_VALUE_SIZE);
  }
  node->n_len -= 1;
}

static void
btnode_inner_collapse(bpath_t path)
{
  diskptr_t* ptr;

  btnode_t node = path_getcur(path);
  btnode_t parent = path_parent(path);

  /* We are the root, and if we've gotten to this point that means
   * We collapsed the last child into the parent so the parent need
   * to become a leaf again */
  if (parent == NULL) {
    /* Ensure we are a leaf now */
    node->n_type = BT_LEAF;
    return;
  }

  if (node->n_len == 0) {
    return;
  }

  /* Find our index */
  int idx;
  for (idx = 0; idx < parent->n_len + 1; idx++) {
    ptr = (diskptr_t*)&parent->n_ch[idx];
    if (memcmp(ptr, &node->n_ptr, sizeof(diskptr_t)) == 0)
      break;
  }

  int num_to_move = parent->n_len - idx;
  memmove(&parent->n_keys[idx],
          &parent->n_keys[idx + 1],
          (num_to_move) * sizeof(uint64_t));
  memmove(&parent->n_ch[idx],
          &parent->n_ch[idx + 1],
          num_to_move * BT_MAX_VALUE_SIZE);

  parent->n_len -= 1;
  if (parent->n_len == 0) {
    path_backtrack(path);
    btnode_inner_collapse(path);
  }
}

static int
btnode_delete(bpath_t path, uint64_t key, void* value)
{
  btnode_t node;
  int idx;

  node = btnode_find_child(path, key, LK_EXCLUSIVE);
  idx = binary_search(node->n_keys, node->n_len, key);
  if (node->n_keys[idx] != key) {
    return -1;
  }

  btnode_leaf_delete(node, idx, value);
  btnode_dirty(node);

  /* Delete the node from the parent */
  if (node->n_len == 0) {
    btnode_inner_collapse(path);
  }

  return 0;
}

static int
btnode_leaf_bulkinsert(btnode_t node,
                       kvp* keyvalues,
                       size_t* len,
                       int64_t max_key)
{
  KASSERT(BT_ISLEAF(node), ("Node should be a leaf"));
  int keys_i = 0;
  int node_i = 0;
  int inserted = 0;
  for (;;) {
    if (node->n_len == BT_MAX_KEYS)
      break;
    /* No more keys! */
    if (node_i >= BT_MAX_KEYS)
      break;

    /* No more keys! */
    if (*len == 0)
      break;

    /* Cannot insert more into this node */
    if (keyvalues[keys_i].key > max_key)
      break;

    if (keyvalues[keys_i].key == node->n_keys[node_i]) {
      btnode_leaf_update(node, node_i, &keyvalues[keys_i].data);
      keys_i += 1;
      inserted += 1;
      *len -= 1;
      continue;
    }

    if (keyvalues[keys_i].key < node->n_keys[node_i]) {
      btnode_leaf_insert(
        node, node_i, keyvalues[keys_i].key, &keyvalues[keys_i].data);
      keys_i += 1;
      inserted += 1;
      *len -= 1;
      continue;
    }

    /* Last element */
    if (node_i == node->n_len) {
      btnode_leaf_insert(
        node, node_i, keyvalues[keys_i].key, &keyvalues[keys_i].data);
      keys_i += 1;
      inserted += 1;
      *len -= 1;
      continue;
    }

    node_i += 1;
  }

  return inserted;
}

int
btree_delete(void* treep, uint64_t key, void* value)
{
  btree_t tree = (btree_t)treep;

  int ret;
  bpath path;
  path.p_len = 0;
  path_add(&path, tree, tree->tr_ptr, INDEX_NULL, LK_EXCLUSIVE);

  ret = btnode_delete(&path, key, value);

  path_unacquire(&path, LK_EXCLUSIVE);

  return ret;
}

#define BULK_DONE (0)
#define BULK_SPLIT (1)
#define BULK_CONTINUE (2)
#define BULK_MAX ((uint64_t)(-1))

static int
btnode_bulkinsert(bpath_t path, kvp** keyvalues, size_t* len, uint64_t max_key)
{
  int idx;
  btnode_t next;
  kvp* kvs = *keyvalues;
  int inserted;

  btnode_t cur = path_getcur(path);
  if (*len == 0)
    return BULK_DONE;

  /* If we are at a leaf the remaining keys can go here */
  if (BT_ISLEAF(cur)) {

    /* Function will update len for us and tell us by how much
     * through the returned inserted variable */
    if (BT_ISCOW(cur) && !BT_COW_DISABLED(cur)) {
      path_cow(path, slos.slos_sb->sb_epoch);
    }

    inserted = btnode_leaf_bulkinsert(cur, kvs, len, max_key);
    /* Update our pointer to further along the list */
    *keyvalues = &kvs[inserted];

    if (cur->n_len == BT_MAX_KEYS) {
      btnode_split(path);
      return BULK_SPLIT;
    }

    return BULK_CONTINUE;
  }

  next = btnode_go_deeper(path, kvs[0].key, LK_EXCLUSIVE);
  cur = path_parent(path);
  /* What is our index */
  idx = path_getindex(path);
  /* It is the last child */
  if (idx != cur->n_len) {
    /* Change our max key */
    max_key = path_parent(path)->n_keys[idx];
  }

  return btnode_bulkinsert(path, keyvalues, len, max_key);
}

/* Assume keyvalues list is sorted */
int
btree_bulkinsert(void* treep, kvp* keyvalues, size_t len)
{
  btree_t tree = (btree_t)treep;

  int ret;
  bpath path;
  path.p_len = 0;

  path_add(&path, tree, tree->tr_ptr, INDEX_NULL, LK_EXCLUSIVE);
  ret = btnode_bulkinsert(&path, &keyvalues, &len, BULK_MAX);
  while (ret != BULK_DONE) {
    /* We got some amount of keys done */
    path_unacquire(&path, LK_EXCLUSIVE);
    /* Reset our path */
    path.p_len = 0;
    path_add(&path, tree, tree->tr_ptr, INDEX_NULL, LK_EXCLUSIVE);
    ret = btnode_bulkinsert(&path, &keyvalues, &len, BULK_MAX);
  }

  path_unacquire(&path, LK_EXCLUSIVE);

  return 0;
}

int
btree_init(void* tree_ptr, struct vnode *vp, diskptr_t ptr, size_t value_size, uint32_t flags)
{
  btree_t tree = (btree_t)tree_ptr;

  KASSERT(value_size <= BT_MAX_VALUE_SIZE, ("Value size too large"));
#ifdef DEBUG
  printf("[Btree Init] %lu\n", ptr.size);
#endif
  tree->tr_ptr = ptr;
  tree->tr_vs = value_size;
  tree->tr_vp = vp;
  tree->tr_flags = flags;

  return 0;
}

int
btree_insert(void* treep, uint64_t key, void* value)
{
  btree_t tree = (btree_t)treep;

  int ret;
  bpath path;
  path.p_len = 0;
#ifdef DEBUG
  printf("[Insert] %lu\n", key);
#endif

  path_add(&path, tree, tree->tr_ptr, INDEX_NULL, LK_EXCLUSIVE);

  ret = btnode_insert(&path, key, value);

  path_unacquire(&path, LK_EXCLUSIVE);

  return (ret);
}

int
btree_greater_equal(void* treep, uint64_t* key, void* value)
{
  btree_t tree = (btree_t)treep;
  uint64_t possible_key = *key;
  int error;

  error = btnode_find_ge(tree, &possible_key, value, LK_SHARED);
  if (error) {
    return error;
  }

  *key = possible_key;

  return 0;
}

int
btree_find(void* treep, uint64_t key, void* value)
{
  btree_t tree = (btree_t)treep;
  uint64_t possible_key = key;
  int error;
#ifdef DEBUG
  printf("[Find] %lu\n", key);
#endif

  error = btnode_find_ge(tree, &possible_key, value, LK_SHARED);
  if (error) {
    return (error);
  }

  if (possible_key != key) {
    return (-1);
  }

  return 0;
}

static int
btree_sync_buf(struct vnode *vp, int waitfor)
{
	struct buf *bp, *nbp;
	struct bufobj *bo;
	struct mount *mp;
	int error, maxretry;

	error = 0;
	maxretry = 10000;     /* large, arbitrarily chosen */
	mp = NULL;
	bo = &vp->v_bufobj;
	BO_LOCK(bo);
loop1:
	/*
	 * MARK/SCAN initialization to avoid infinite loops.
	 */
  TAILQ_FOREACH(bp, &bo->bo_dirty.bv_hd, b_bobufs) {
		bp->b_vflags &= ~BV_SCANNED;
		bp->b_error = 0;
	}

	/*
	 * Flush all dirty buffers associated with a vnode.
	 */
loop2:
	TAILQ_FOREACH_SAFE(bp, &bo->bo_dirty.bv_hd, b_bobufs, nbp) {
		if ((bp->b_vflags & BV_SCANNED) != 0)
			continue;
		bp->b_vflags |= BV_SCANNED;
		if (BUF_LOCK(bp, LK_EXCLUSIVE | LK_NOWAIT, NULL)) {
			if (waitfor != MNT_WAIT)
				continue;
			if (BUF_LOCK(bp,
			    LK_EXCLUSIVE | LK_INTERLOCK | LK_SLEEPFAIL,
			    BO_LOCKPTR(bo)) != 0) {
				BO_LOCK(bo);
				goto loop1;
			}
			BO_LOCK(bo);
		}
		BO_UNLOCK(bo);
		KASSERT(bp->b_bufobj == bo,
		    ("bp %p wrong b_bufobj %p should be %p",
		    bp, bp->b_bufobj, bo));
		if ((bp->b_flags & B_DELWRI) == 0) {
			panic("fsync: not dirty");
		} else {
			bremfree(bp);
			bawrite(bp);
		}
		if (maxretry < 1000)
			pause("dirty", hz < 1000 ? 1 : hz / 1000);
		BO_LOCK(bo);
		goto loop2;
	}

	/*
	 * If synchronous the caller expects us to completely resolve all
	 * dirty buffers in the system.  Wait for in-progress I/O to
	 * complete (which could include background bitmap writes), then
	 * retry if dirty blocks still exist.
	 */
	if (waitfor == MNT_WAIT) {
		bufobj_wwait(bo, 0, 0);
		if (bo->bo_dirty.bv_cnt > 0) {
			/*
			 * If we are unable to write any of these buffers
			 * then we fail now rather than trying endlessly
			 * to write them out.
			 */
			TAILQ_FOREACH(bp, &bo->bo_dirty.bv_hd, b_bobufs)
				if ((error = bp->b_error) != 0)
					break;
			if ((mp != NULL && mp->mnt_secondary_writes > 0) ||
			    (error == 0 && --maxretry >= 0))
				goto loop1;
			if (error == 0)
				error = EAGAIN;
		}
	}
	BO_UNLOCK(bo);
	if (error != 0)
		vn_printf(vp, "fsync: giving up on dirty (error = %d) ", error);

	return (error);
}

diskptr_t
btree_checkpoint(void* treep)
{
  btree_t tree = (btree_t)treep;
  diskptr_t ptr = tree->tr_ptr;

#ifdef DEBUG
  printf("[Checkpoint]\n");
#endif

  btree_sync_buf(tree->tr_vp, 0);

  return (ptr);
}

/*
 * Btree rangequery gives all results such that
 * low_key <= result < key_max
 */
int
btree_rangequery(void* treep,
                 uint64_t key_low,
                 uint64_t key_max,
                 kvp* results,
                 size_t results_max)
{
  btree_t tree = (btree_t)treep;

  int idx;
  bpath path;
  btnode_t node;
  int cur_res_idx = 0;

  for (;;) {

    /* Start querying */
    if (cur_res_idx == results_max)
      return cur_res_idx;

    path.p_len = 0;
    path_add(&path, tree, tree->tr_ptr, INDEX_NULL, LK_SHARED);

    node = btnode_find_child(&path, key_low, LK_SHARED);

    idx = binary_search(node->n_keys, node->n_len, key_low);
    /* Did not find the minimum key at all */
    if (idx == node->n_len) {
      path_unacquire(&path, LK_SHARED);
      return cur_res_idx;
    }

    while (idx < node->n_len) {
      /* Found a key */
      if (node->n_keys[idx] >= key_max) {
        path_unacquire(&path, LK_SHARED);
        return cur_res_idx;
      }

      if (node->n_keys[idx] >= key_low) {
        results[cur_res_idx].key = node->n_keys[idx];
        memcpy(&results[cur_res_idx].data, &node->n_ch[idx + 1], tree->tr_vs);
        cur_res_idx += 1;
        /*
         * Update our key low to the current key we just added + 1, so we
         * can traverse forward
         */
        key_low = node->n_keys[idx] + 1;
      }

      idx += 1;
    }

    path_unacquire(&path, LK_SHARED);
  }

  return 0;
}

size_t
btree_getkeysize(void* treep)
{
  btree_t tree = (btree_t)treep;
  return tree->tr_vs;
}

diskptr_t
btree_getroot(void* treep)
{
  btree_t tree = (btree_t)treep;
  return tree->tr_ptr;
}


