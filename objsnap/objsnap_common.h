#ifndef _OBJSNAP_COMMON_H_
#define _OBJSNAP_COMMON_H_

#define MAXTHREADS (64)
#define MAXDRTYCNT (64)

struct objsnap_txn {
	int d_cnt;	/* Size of the working set in disk blocks. */
	vm_page_t d_page[MAXDRTYCNT];
	vm_pindex_t d_index[MAXDRTYCNT];
	vm_offset_t d_offset[MAXDRTYCNT];
	vm_offset_t d_inode[MAXDRTYCNT];
	obj_diskptr_t d_ptr; /* Backing disk pointer. */
} __attribute__((aligned(64))) ;

extern struct objsnap_txn tpgs[MAXTHREADS];

void objsnap_checkpoint_txn(int);
void objsnap_create_inode(int *);

#endif /* _OBJSNAP_COMMON_H_ */ 

