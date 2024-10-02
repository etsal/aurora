#ifndef _ARRAY_H_
#define _ARRAY_H_

//int arr_init(void *, struct vnode *, obj_diskptr_t, size_t);
//int arr_insert(void *, uint64_t, void *);
//int arr_bulkinsert(void *, kvp *, size_t);
//int arr_delete(void *, uint64_t, void *);
//int arr_find(void *, uint64_t, void *);
//int arr_greater_equal(void *, uint64_t *, void *);
//void arr_bumpversion(void *);
//
//uint64_t arr_getversion(void *);

//int arr_rangequery(void * tree, uint64_t, uint64_t, kvp *, size_t)
//
//obj_diskptr_t
//arr_checkpoint(void * tree);

extern struct vtreeops arrops;
#endif /* _ARRAY_H_ */
