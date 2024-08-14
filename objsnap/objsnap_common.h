#ifndef _OBJSNAP_COMMON_H_
#define _OBJSNAP_COMMON_H_

#define MAXTHREADS (64)
#define MAXDRTYCNT (64)

struct pageset {
	vm_object_t obj;
	vm_pindex_t pindex;
	vm_offset_t offset;
	index_t inode;
};

struct blockset {
	uint64_t blkoff; 
	uint64_t objoff;
	index_t objino;
};

enum objsnap_txn_type {
	OBJTXN_PAGE,
	OBJTXN_BLOCK,
	OBJTXN_MSNP,
};

struct objsnap_txn {
	int d_cnt;	/* Size of the working set in disk blocks. */
	union {
		struct pageset d_pg[MAXDRTYCNT];
		struct blockset d_blk[MAXDRTYCNT];
		vm_page_t d_msnp[MAXDRTYCNT];
	};
	diskptr_t d_ptr; /* Backing disk pointer. */
	enum objsnap_txn_type d_type; /* Transaction data format. */
};

extern struct objsnap_txn tpgs[MAXTHREADS];

void objsnap_checkpoint_txn(int, enum objsnap_txn_type type);

#endif /* _OBJSNAP_COMMON_H_ */ 

