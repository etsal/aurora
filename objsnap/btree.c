#include <assert.h>

#include "btree.h"
#include "buf.h"

#define INDEX_NULL ((uint16_t)-1)

typedef struct bpath
{
  uint64_t p_len;
  uint16_t p_indexes[BT_MAX_PATH_SIZE];
  btnode p_nodes[BT_MAX_PATH_SIZE];
  uint8_t p_cur;
} bpath;

typedef bpath* bpath_t;

static int num_splits = 0;

#define BINARY_SEARCH_CUTOFF (64)

int
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
btnode_wrap_bp(btnode_t node, btree_t tree, struct buf* bp)
{
  diskptr_t ptr;

  ptr.size = BLKSZ;
  ptr.offset = bp->bp_lblkno;

  node->n_bp = bp;
  node->n_data = (btdata_t)bp->bp_data;
  node->n_tree = tree;
  node->n_ptr = ptr;
}

static void
btnode_init(btnode_t node, btree_t tree, diskptr_t ptr, int lk_flags)
{
  struct buf* bp = getblk(ptr.offset, ptr.size * PBLKSZ, lk_flags);
  node->n_bp = bp;
  node->n_data = (btdata_t)bp->bp_data;
  node->n_tree = tree;
  node->n_ptr = ptr;
}

/* Node is locked exclusively on create */
static void
btnode_create(btnode_t node, btree_t tree, uint8_t type)
{
  diskptr_t ptr = allocate_blk(BLKSZ);
  btnode_init(node, tree, ptr, LK_EXCLUSIVE);
  node->n_type = type;
  node->n_len = 0;
}

/* Caller must check to see if parent */
static uint16_t
path_getindex(bpath_t path)
{
  return path->p_indexes[path->p_cur];
}

/*
 * Will iterater through the path and perform COW on all entries within the path
 */
static void
path_cow(bpath_t path)
{
  btnode tmp;
  btnode_t parent = NULL;
  int idx;
  /* We hold all the locks of the path exclusively so we can change the parent
   */
  for (int i = 0; i < path->p_len; i++) {
    /* Check if node is not already COWed */
    tmp = path->p_nodes[i];
    if (!BT_ALREADY_COW(&tmp)) {

      /* Grab our index in our parent */
      if (i > 0) {
        parent = &path->p_nodes[i - 1];
        idx = path->p_indexes[i];
        /* We are the root so lets update our own parent ptr as well as save the
         * old tree */
      } else {
        /* Init the old tree to be passed back to the user */
        /* TODO: Update any consumer that this root has changed */
      }

      btnode_create(&path->p_nodes[i], tmp.n_tree, tmp.n_type);
      /* Perform the copy of data or however we choose to transfer it over */
      memcpy(path->p_nodes[i].n_data, tmp.n_data, BLKSZ);

      /* Update our parent to know of the change */
      if (i > 0) {
        memcpy(&parent->n_ch[idx], &path->p_nodes[i].n_ptr, sizeof(diskptr_t));
      } else {
        /* Make sure we update our root ptr in our main tree datastructure */
        path->p_nodes[i].n_tree->tr_ptr = path->p_nodes[i].n_ptr;
      }

      buf_unlock(tmp.n_bp, LK_EXCLUSIVE);

      /* We must invalidate the buffer to insure it never writes */
      bclean(tmp.n_bp);

      /* Turn of cow on the node and dirty the node */
      BT_FRESH_COW(&path->p_nodes[i]);
      btnode_dirty(&path->p_nodes[i]);
    }
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

static inline btnode_t
path_getcur(bpath_t path)
{
  return &path->p_nodes[path->p_cur];
}

static inline void
path_unacquire(bpath_t path, int acquire_as)
{
  for (int i = 0; i < path->p_len; i++) {
    buf_unlock(path->p_nodes[i].n_bp, acquire_as);
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

  diskptr_t ptr = *(diskptr_t*)&cur->n_ch[cidx];
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
  assert(BT_ISINNER(node));
  if (node->n_len) {
    int num_to_move = node->n_len - idx + 1;
    memmove(
      &node->n_keys[idx + 1], &node->n_keys[idx], num_to_move * sizeof(key));
    memmove(&node->n_ch[idx + 2],
            &node->n_ch[idx + 1],
            num_to_move * BT_MAX_VALUE_SIZE);
  }

  node->n_keys[idx] = key;
  memcpy(&node->n_ch[idx + 1], &value, sizeof(value));
  node->n_len += 1;

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

  /* We are the root */
  if (pptr == NULL) {
    btnode_create(&parent, node->n_tree, BT_INNER);
    /* Set our current node to the child of our new parent */
    memcpy(&parent.n_ch[0], &node->n_ptr, sizeof(diskptr_t));

    node = path_fixup_cur_parent(path, &parent);

    /* Fixup root parent ptr in the tree */
    node->n_tree->tr_ptr = parent.n_ptr;

    idx = 0;
  } else {
    parent = *pptr;
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
  else
    memcpy(&right_child.n_ch[0],
           &node->n_ch[SPLIT_KEYS],
           (SPLIT_KEYS + 1) * BT_MAX_VALUE_SIZE);

  /* Setting the pivot key here, with SPLIT_KEYS - 1, means elements to the
   * right must be strictly greater
   */
  btnode_inner_insert(&parent, idx, split_key, right_child.n_ptr);

  /* Unlock the right child and dirty the children*/
  btnode_dirty(&right_child);
  btnode_dirty(node);

  buf_unlock(right_child.n_bp, LK_EXCLUSIVE);

  if (parent.n_len == BT_MAX_KEYS) {
    printf("DOUBLE SPLIT\n");
    path_backtrack(path);
    btnode_split(path);
  }
}

static void
btnode_leaf_insert(btnode_t node, int idx, uint64_t key, void* value)
{
  assert(BT_ISLEAF(node));
  assert(!BT_ISCOW(node));
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

  bdirty(node->n_bp);
}

static void
btnode_leaf_update(btnode_t node, int idx, void* value)
{
  assert(BT_ISLEAF(node));
  assert(!BT_ISCOW(node));
  memcpy(&node->n_ch[idx + 1], value, BT_VALSZ(node));
  bdirty(node->n_bp);
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
  if (BT_ISCOW(node)) {
    path_cow(path);
  }

  /* Update over insert */
  if (node->n_keys[idx] == key) {
    btnode_leaf_update(node, idx, value);
  } else {
    btnode_leaf_insert(node, idx, key, value);
    if (node->n_len == BT_MAX_KEYS) {
      btnode_split(path);
    }
  }

  return 0;
}
static void
btnode_leaf_delete(btnode_t node, int idx, void* value)
{
  assert(BT_ISLEAF(node));
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

  assert(memcmp(ptr, &node->n_ptr, sizeof(diskptr_t)) == 0);
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

static inline void
btnode_mark_cow(btnode_t node)
{
  node->n_hdr.hdr_flags = BT_COW;
}

static int
btnode_leaf_bulkinsert(btnode_t node,
                       kvp* keyvalues,
                       size_t* len,
                       int64_t max_key)
{
  assert(BT_ISLEAF(node));
  assert(!BT_ISCOW(node));
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

int
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
    if (BT_ISCOW(cur)) {
      path_cow(path);
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
  kvp* kvs = keyvalues;

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
btree_init(void* tree_ptr, diskptr_t ptr, size_t value_size)
{
  btree_t tree = (btree_t)tree_ptr;

  assert(value_size <= BT_MAX_VALUE_SIZE);

  tree->tr_ptr = ptr;
  tree->tr_vs = value_size;

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

diskptr_t
btree_checkpoint(void* treep)
{
  btree_t tree = (btree_t)treep;
  size_t size;
  struct buf** ds = get_dirty_set(&size);
  btnode node;
  diskptr ptr = tree->tr_ptr;

#ifdef DEBUG
  printf("[Checkpoint]\n");
#endif

  for (size_t i = 0; i < size; i++) {
    /* Wrap our node */
    btnode_wrap_bp(&node, tree, ds[i]);

    /* Node is dead - clean up */
    if (node.n_len == 0) {
      bclean(ds[i]);
    }

    btnode_mark_cow(&node);
    bawrite(ds[i]);
  }

  /* TODO: Barrier writes or wait for all buffers to flush on the new root node
   */
  free(ds);
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

static size_t
btree_getkeysize(void* treep)
{
  btree_t tree = (btree_t)treep;
  return tree->tr_vs;
}

struct vtreeops btreeops = { .vtree_init = &btree_init,

                             .vtree_insert = &btree_insert,
                             .vtree_bulkinsert = &btree_bulkinsert,
                             .vtree_delete = &btree_delete,

                             .vtree_find = &btree_find,
                             .vtree_ge = &btree_greater_equal,
                             .vtree_rangequery = &btree_rangequery,

                             .vtree_checkpoint = &btree_checkpoint,
                             .vtree_getkeysize = &btree_getkeysize };
