#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "radix.h"

#define RDX_MARK_LEAF(ptr) ((ptr)->rdx_ptr.flags |= DPTR_RDX_LEAF)
#define RDX_IS_LEAF(ptr) ((ptr)->rdx_ptr.flags & DPTR_RDX_LEAF)
#define RDX_HDR(node) (node->rdx_data->d_hdr)

static size_t current_block_no = 1;
static const uint64_t l0_mask = 0xFF80000000000000;
static const uint64_t l1_mask = 0x007FF00000000000;
static const uint64_t l2_mask = 0x00000FFE00000000;
static const uint64_t l3_mask = 0x00000001FFC00000;
static const uint64_t l4_mask = 0x00000000003FF800;
static const uint64_t l5_mask = 0x00000000000007FF;

static const uint64_t masks[6] = { l0_mask, l1_mask, l2_mask,
                                   l3_mask, l4_mask, l5_mask };

static rdxnode_t
get_node(rdxtree_t tree, diskptr_t ptr)
{
  struct buf* bp = alloc_blk(BLKSZ);
  rdxnode_t node = malloc(sizeof(rdxnode));
  node->rdx_bp = bp;
  node->rdx_tree = tree;
  node->rdx_ptr = ptr;
  node->rdx_data = bp->bp_data;
  node->rdx_data->d_hdr.depth = MAX_ARR;

  return node;
}

static rdxnode_t
create_node(rdxtree_t tree, int isleaf)
{
  struct buf* bp = alloc_blk(BLKSZ);
  rdxnode_t node = malloc(sizeof(rdxnode));
  node->rdx_bp = bp;
  node->rdx_tree = tree;
  node->rdx_data = bp->bp_data;
  if (isleaf)
    RDX_MARK_LEAF(node);
  return node;
}

// For prefix compressed radix trees the algorithm for checking, is
// take your key, shift by DEPTH bytes to the left, compare mask with
// that amount of bytes specific by prefix_len with prefix_arr, and check if 0
// If 0, it means the key follow the key located at keys byte depth + prefix_len
// else the key is not there.
static inline int
leaf_matches(uint64_t key, rdx_node_header* header)
{
  uint64_t mask = 0;
  for (uint8_t i = 0; i < header->prefix_len; i++) {
    mask |= ((uint64_t)header->prefix_arr[i])
            << ((MAX_ARR - i) * RADIX_BIT_WIDTH);
  }
  return (mask & key) == mask;
}

static inline int
check_prefix(uint64_t key, rdx_node_header* header)
{
  uint64_t mask = 0;

  int num_matches = 0;
  for (uint8_t i = 0; i < header->prefix_len; i++) {
    mask = ((uint64_t)header->prefix_arr[i])
           << ((MAX_ARR - i - header->depth) * RADIX_BIT_WIDTH);
    if ((mask & key) == mask) {
      num_matches += 1;
    }
  }

  return num_matches;
}

static inline int
rdx_get_index(uint64_t key)
{
  /* 000100000000... */
  uint64_t mask = 1 << (RADIX_BIT_WIDTH + 1);
  /* 000011111111... */
  mask -= 1;
  return key & mask;
}

static inline uint64_t
determine_key(rdxnode_t node, int idx)
{
  uint64_t mask = 0;
  rdx_node_header* header = &RDX_HDR(node);

  for (uint8_t i = 0; i < header->prefix_len; i++) {
    mask |= ((uint64_t)header->prefix_arr[i])
            << ((MAX_ARR - i - header->depth) * RADIX_BIT_WIDTH);
  }

  mask |= idx;

  return mask;
}

static rdx_value_cont*
rdx_find_keydata_pair(rdxnode_t node, uint64_t key)
{
  int idx = rdx_get_index(key);
  rdx_node_data* data = (rdx_node_data*)node->rdx_data;
  return &data->d_values[idx];
}

void
rdx_tree_init(rdxtree_t tree, diskptr_t ptr, size_t value_size)
{
  tree->tree_root = NULL;
  tree->value_size = value_size;
  tree->tree_ptr = ptr;
}

int
rdx_find(rdxtree_t tree, uint64_t key, void* value)
{
  int error = 00;
  rdxnode_t current;
  void* found;
  rdx_value_cont* cont;

  if (tree->tree_root == NULL) {
    return -1;
  }

  current = tree->tree_root;
  for (;;) {
    if (RDX_IS_LEAF(current)) {
      cont = rdx_find_keydata_pair(current, key);
      if (cont->v_nullflag) {
        memcpy(value, cont->v_data, sizeof(tree->value_size));
        return 0;
      }
      return -1;
    }
  }
}

static void
set_prefix(rdxnode_t node, uint64_t key)
{
  RDX_HDR(node).prefix_len = RDX_HDR(node).depth;
  for (uint8_t i = 0; i < RDX_HDR(node).depth; i++) {
    RDX_HDR(node).prefix_arr[i] = key & masks[i];
  }
}

int
_rdx_insert(rdxnode_t previous, rdxnode_t node, uint64_t key, void* value)
{
  uint64_t container_key;
  rdxnode_t current;
  rdxnode_t new_parent;
  rdx_value_cont* cont;

  int error = 0;
  int idx = 0;
  rdxtree_t tree = node->rdx_tree;

  current = node;

  for (;;) {
    /* Check is the current node is a leaf */
    if (RDX_IS_LEAF(current)) {

      /* Grab key value pair */
      cont = rdx_find_keydata_pair(current, key);

      /* Check if the key is present in the tree */
      if (cont->v_nullflag) {
        idx = rdx_get_index(key);
        /* Check if the key given the prefix is OUR key */
        container_key = determine_key(current, idx);
        if (container_key == key) {
          memcpy(cont->v_data, value, tree->value_size);
          return 0;
        }

        /* Key is different, we need to split */
        new_parent = create_node(tree, 0);
        RDX_HDR(new_parent).depth = RDX_HDR(current).depth - 1;
        set_prefix(new_parent,
        return 0;
      }

      if (!RDX_HDR(current).num_keys) {
        RDX_HDR(current).num_keys++;
        set_prefix(current, key);
      }

      cont->v_nullflag = 1;
      memcpy(cont->v_data, value, tree->value_size);
      break;
    }

    if (RDX_HDR(current).prefix_len) {
    }
  }

  return (error);
}

int
rdx_insert(rdxtree_t tree, uint64_t key, void* value)
{
  if (tree->tree_root == NULL) {
    tree->tree_root = get_node(tree, tree->tree_ptr);
    // KASSERT RDX_IS_LEAF(tree->tree_root)
  }

  return _rdx_insert(NULL, tree->tree_root, key, value);
}
