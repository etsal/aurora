#ifndef __CHUNKALLOC_H__
#define __CHUNKALLOC_H__

#define CA_CHUNKSZ (1UL * 1024 * 1024)
#define CA_SECOBJS (64)
#define CA_MAXSEC (CA_CHUNKSZ / BLOCKSIZE)

enum ca_chunk_state {
	CA_ACTIVE,
	CA_EMPTYING,
	CA_FULL,
};

struct ca_objid {
	uint32_t cao_ino;
	uint32_t cao_off;
};

struct ca_sector {
	uint64_t 	cas_bmap;
	struct ca_objid cas_objs[CA_SECOBJS];
};

/* XXX Static assert that CA_MAXSEC < 256 */
struct ca_chunk {
	struct mtx 		cac_mtx;
	int 			cac_index;
	diskptr_t 		cac_ptr;

	struct ca_sector 	cac_map[CA_MAXSEC];

	uint64_t 		cac_used;
	uint64_t 		cac_freed;

	/* Fields that are only valid when the chunk is not free. */
	enum ca_chunk_state 	cac_state;
	uint8_t 		cac_sec_free;
	uint8_t 		cac_sec_max;
	uint32_t 		cac_txn_size;
};

/* XXX Implement high-pressure state. */
struct chunkallocator {
	struct mtx  		ca_mtx;

	uint32_t		ca_txnsz_blk;

	struct ca_chunk		*ca_chunks;
	uint64_t 		ca_chunk_cnt;

	struct ca_chunk		**ca_old;
	int			ca_old_cnt;

	struct ca_chunk		**ca_next;
	int			ca_next_cnt;

	struct ca_chunk		**ca_cand;
	struct ca_chunk		**ca_cand_old;
	uint64_t 		ca_cand_cnt;

	uint64_t 		ca_num_chunks;
	uint64_t 		ca_num_used;

	bool			ca_exiting;	/* Should the allocator exit? */
	/*
	 * XXX Add stats back.
	 */
};

void ca_init(struct chunkallocator *ba);
int ca_alloc(struct chunkallocator *ba, int numblocks, diskptr_t *ptr);
void ca_free(struct chunkallocator *ba, diskptr_t tofree);
void ca_destroy(struct chunkallocator *ba);
void ca_print(struct chunkallocator *ba);

#endif /* __CHUNKALLOCATOR_H_ */
