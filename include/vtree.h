#ifndef _VTREE_H_
#define _VTREE_H_

#include <sys/types.h>
#include <sys/malloc.h>
#include <sys/param.h>
#include <sys/buf.h>
#include <sys/bufobj.h>
#include <sys/vnode.h>

#define VTREE_BLKSZ (MAXBCACHEBUF)

#define DPTR_COW (1)

/* Physical extent on-disk pointer */
struct slos_diskptr {
	uint64_t
	    offset;    /* The block offset of the first block of the region. */
	uint64_t size; /* The size of the region in bytes. */
	uint64_t epoch;
  uint8_t flags;
};
typedef struct slos_diskptr diskptr_t;

/*
 * Virtual Tree Interface
 */

/* Max value size for tree in bytes */
#define BT_MAX_VALUE_SIZE (32)

typedef struct kvp
{
  uint64_t key;
  int error;
  unsigned char data[BT_MAX_VALUE_SIZE];
} kvp;

typedef int (*vtree_init_t)(void* tree, struct vnode *vp, diskptr_t key, size_t value_size, uint32_t flags);

/* Write ops */
typedef int (*vtree_insert_t)(void* tree, uint64_t key, void* value);
typedef int (*vtree_bulkinsert_t)(void* tree, kvp* keyvalues, size_t len);
typedef int (*vtree_delete_t)(void* tree, uint64_t key, void* value);

/* Query Ops */
typedef int (*vtree_find_t)(void* tree, uint64_t key, void* value);
typedef int (*vtree_ge_t)(void* tree, uint64_t* key, void* value);
typedef int (*vtree_rangequery_t)(void* tree,
                                  uint64_t keylow,
                                  uint64_t keymax,
                                  kvp* results,
                                  size_t results_max);

typedef diskptr_t (*vtree_checkpoint_t)(void* tree);
typedef size_t (*vtree_getkeysize)(void* tree);
typedef diskptr_t (*vtree_getroot)(void* tree);

struct vtreeops
{
  vtree_init_t vtree_init;

  vtree_insert_t vtree_insert;
  vtree_bulkinsert_t vtree_bulkinsert;
  vtree_delete_t vtree_delete;

  vtree_find_t vtree_find;
  vtree_ge_t vtree_ge;
  vtree_rangequery_t vtree_rangequery;

  vtree_checkpoint_t vtree_checkpoint;

  vtree_getkeysize vtree_getkeysize;
  vtree_getroot   vtree_getroot;
};

#define VTREE_WALSIZE (64UL * 1024)
#define VTREE_MAXWAL (VTREE_WALSIZE / sizeof(kvp))

#define VTREE_WITHWAL (0x1)
#define VTREE_WALBULK (0x2)
#define VTREE_NOCOW (0x4)

#define VTREE_BASETREE (128)
#define VTREE_LOCK(tree, flags) (lockmgr(&(tree)->bt_lock, flags, 0))

typedef int (*vtree_rc_t)(void* ctx, diskptr_t newroot);

typedef struct vtree
{
  unsigned char v_tree[VTREE_BASETREE];

  uint32_t v_flags;
  kvp* v_wal;
  struct vnode *v_vp;
  int v_cur_wal_idx;

  struct vtreeops* v_ops;

  vtree_rc_t    v_callback;
  void          *v_ctx;

	struct lock bt_lock;
} vtree;

#define VTREE_INIT(tree, ptr, keysize, flags)                                         \
  ((tree)->v_ops->vtree_init((void *)(tree)->v_tree, (tree)->v_vp, ptr, keysize, flags))

#define VTREE_INSERT(tree, key, value)                                         \
  ((tree)->v_ops->vtree_insert((void *)(tree)->v_tree, key, value))

#define VTREE_BULKINSERT(tree, kvp, len)                                       \
  ((tree)->v_ops->vtree_bulkinsert((void *)(tree)->v_tree, kvp, len))

#define VTREE_DELETE(tree, key, value)                                         \
  ((tree)->v_ops->vtree_delete((void *)(tree)->v_tree, key, value))

#define VTREE_FIND(tree, key, value)                                           \
  ((tree)->v_ops->vtree_find((void *)(tree)->v_tree, key, value))

#define VTREE_GE(tree, key, value)                                             \
  ((tree)->v_ops->vtree_ge((void *)(tree)->v_tree, key, value))

#define VTREE_RANGEQUERY(tree, keylow, keymax, results, results_max)           \
  ((tree)->v_ops->vtree_rangequery(                                            \
    (tree)->v_tree, keylow, keymax, results, results_max))

#define VTREE_CHECKPOINT(tree) ((tree)->v_ops->vtree_checkpoint((tree)->v_tree))

#define VTREE_GETKEYSIZE(tree) ((tree)->v_ops->vtree_getkeysize((tree)->v_tree))
#define VTREE_GETROOT(tree) ((tree)->v_ops->vtree_getroot((tree)->v_tree))

int
vtree_create(struct vtree *vtree, struct vtreeops* ops, 
    diskptr_t root, size_t ks, uint32_t v_flags, vtree_rc_t rc, void *ctx);

int
vtree_insert(vtree* tree, uint64_t key, void* value);

int
vtree_bulkinsert(vtree* tree, kvp* keyvalues, size_t len);

int
vtree_delete(vtree* tree, uint64_t key, void* value);

int
vtree_find(vtree* tree, uint64_t key, void* value);

int
vtree_ge(vtree* tree, uint64_t* key, void* value);

int
vtree_rangequery(vtree* tree,
                 uint64_t key_low,
                 uint64_t key_max,
                 kvp* results,
                 size_t results_max);

diskptr_t
vtree_checkpoint(vtree* tree);

void
vtree_empty_wal(vtree* tree);

size_t
vtree_dirty_cnt(vtree *tree);

void
vtree_free(vtree *tree);

void
vtree_interface_init(void);

extern struct vtreeops btreeops;
extern struct vtreeops *defaultops;
#endif
