#include <sys/param.h>
#include <objsnap_ioctl.h>

#include "objsnap_internal.h"
#include "arraylist.h"
#include "binaryalloc.h"
#include "btree.h"
#include "alloc.h"


static uint64_t arr_version = 0;
static obj_diskptr_t *arr_pv;

static int
arr_init(void *arr, struct vnode *vp, obj_diskptr_t ptr, size_t value_size)
{
	KASSERT(sizeof(*arr_pv) == value_size, ("initing with invalid value"));
	arr_pv = malloc(sizeof(*arr_pv) * superblock.super_size, M_TEMP, M_ZERO | M_WAITOK);
	return (0);
}

static int
arr_insert(void *arr, uint64_t key, void *value)
{
	if (arr_pv[key].offset != 0)
  		free_block_data(arr_pv[key]);

	arr_pv[key] = *(obj_diskptr_t *) value;

	return (0);
}

static int
arr_bulkinsert(void *arr, kvp* keyvalues, size_t len)
{
	panic("unimplemented");
	return (0);
}

static int
arr_delete(void *arr, uint64_t key, void *value)
{
	panic("unimplemented");
	return (0);
}

static int
arr_find(void *arr, uint64_t key, void *value)
{
	panic("unimplemented");
	return (0);
}

static int
arr_greater_equal(void *arr, uint64_t *key, void *value)
{
	panic("unimplemented");
	return (0);
}

static int
arr_rangequery(void *arr, uint64_t low, uint64_t max, kvp* results, size_t results_max)
{
	panic("unimplemented");
	return (0);
}

static obj_diskptr_t
arr_checkpoint(void *arr)
{
	atomic_add_64(&arr_version, 1);
	panic("unimplemented");
}

static size_t
arr_getkeysize(void* treep)
{
	return (sizeof(*arr_pv));
}


struct vtreeops arrops = { 
	.vtree_init = &arr_init,

        .vtree_insert = &arr_insert,
        .vtree_bulkinsert = &arr_bulkinsert,
        .vtree_delete = &arr_delete,

        .vtree_find = &arr_find,
        .vtree_ge = &arr_greater_equal,
        .vtree_rangequery = &arr_rangequery,

        .vtree_checkpoint = &arr_checkpoint,
        .vtree_getkeysize = &arr_getkeysize
};
