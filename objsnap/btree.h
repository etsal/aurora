#ifndef _BTREE_H_
#define _BTREE_H_
/*
 *
 * COW version, and write optimized
 * B+Tree which is designed to follow the FreeBSD kernel buffer
 * cache semantics
 *
 * The general design is such that it uses the underlying buffer cache to keep
 * track of nodes (meaning no volatile in memory pointers to other children).
 * Each operation keeps a path of nodes access, locking respectively as
 * it traverses the tree.
 *
 * Having the buffer cache keep track of memory makes the implementation
 * cleaner and easier
 */

#include <sys/types.h>
#include <sys/buf.h>
#include <sys/vnode.h>

#include "vtree.h"
#include "binaryalloc.h"

#define BT_MAX_KEY_SIZE (8L)
#define BT_MAX_HDR_SIZE (64L)
#define BT_MAX_PATH_SIZE (10L)

#define BT_LEAF (0)
#define BT_INNER (1)

/* The number of keys is
 * (BLKSZ - BT_MAX_HDR_SIZE - BT_MAX_VALUE_SIZE) /
 *  (BT_MAX_KEY_SIZE + BT_MAX_VALUE_SIZE)
 */
#define TOP (BLOCKSIZE - BT_MAX_HDR_SIZE - BT_MAX_VALUE_SIZE)
#define BOT (BT_MAX_KEY_SIZE + BT_MAX_VALUE_SIZE)
#define PADDING (10)
#define BT_MAX_KEYS ((TOP / BOT))
#define SPLIT_KEYS (BT_MAX_KEYS / 2)

#define BT_ISLEAF(node) ((node)->n_type == BT_LEAF)
#define BT_ISINNER(node) ((node)->n_type == BT_INNER)
#define BT_VALSZ(node) ((node)->n_tree->tr_vs)
#define BT_COWCHECK(node) ((node)->n_hdr.hdr_version < (node)->n_tree->tr_version)
#define BT_BUMPVERSION(node) ((node)->n_hdr.hdr_version = (node)->n_tree->tr_version)

/* Header object that is apart of every on disk node */
typedef struct btnodehdr
{
  uint32_t hdr_len;
  uint8_t hdr_type;
  uint64_t hdr_version;
} btnodehdr;

typedef btnodehdr* btnodehdr_t;

/* Container for holding the values for Btree */
typedef struct child_cont
{
  unsigned char vdata[BT_MAX_VALUE_SIZE];
} ct;

/* Data representing the on disk btree node */
typedef struct btdata
{
  btnodehdr bt_hdr;
  uint64_t bt_keys[BT_MAX_KEYS];

  /* Make sure to add one child for inner nodes */
  diskptr_t bt_children[BT_MAX_KEYS + 1];
} btdata;

typedef btdata* btdata_t;

struct btree;
typedef struct btree* btree_t;

/* In memory btnode */
typedef struct btnode
{
  struct buf* n_bp;
  btdata_t n_data;
  btree_t n_tree;
  diskptr_t n_ptr;
#define n_id n_bp->bp_lblkno
#define n_hdr n_data->bt_hdr
#define n_keys n_data->bt_keys
#define n_ch n_data->bt_children
#define n_len n_data->bt_hdr.hdr_len
#define n_version n_data->bt_hdr.hdr_version
#define n_type n_data->bt_hdr.hdr_type
} btnode;

typedef btnode* btnode_t;

typedef struct btree
{
  // tr_ptr must always be on top so the virtual tree can acquire it
  diskptr_t tr_ptr;
  size_t tr_vs;
  uint64_t tr_version;
  struct vnode *tr_vp;

	struct arraylist tr_freeme;
	struct arraylist tr_deadlist;
} btree;

btree_t btree_create(void);
void btree_destroy(btree_t tree);

int
btree_init(void* tree, struct vnode *vp, diskptr_t ptr, size_t value_size);
int
btree_insert(void* tree, uint64_t key, void* value);
int
btree_bulkinsert(void* tree, kvp* keyvalues, size_t len);

int
btree_delete(void* tree, uint64_t key, void* value);

int
btree_find(void* tree, uint64_t key, void* value);
int
btree_greater_equal(void* tree, uint64_t* key, void* value);

void
btree_bumpversion(void* tree);

uint64_t
btree_getversion(void* tree);

int
btree_rangequery(void* tree,
                 uint64_t key_low,
                 uint64_t key_max,
                 kvp* results,
                 size_t results_max);

diskptr_t
btree_checkpoint(void* tree);

extern struct vtreeops btreeops;

#endif
