#ifndef __CHUNKALLOC_H__
#define __CHUNKALLOC_H__

#define CA_CHUNKSZ (1UL * 1024 * 1024)
#define CA_SECOBJS (64)
#define CA_MAXSEC (CA_CHUNKSZ / BLOCKSIZE)

enum ca_chunk_state {
	CH_FREE,
	CH_ACTIVE,
	CH_EMPTYING,
	CH_FULL,
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
	obj_diskptr_t 		cac_ptr;

	struct ca_sector 	cac_map[CA_MAXSEC];
	enum ca_chunk_state 	cac_state;

	/* Fields that are only valid when the chunk is not free. */
	uint64_t 		cac_blocks_used;
	uint32_t 		cac_sec_free;
	uint32_t 		cac_sec_max;
	uint32_t 		cac_txn_size;
};

/* XXX Implement high-pressure state. */
struct chunkallocator {
	struct mtx  		ca_mtx;

	uint32_t		ca_txnsz_blk;

	struct ca_chunk		*ca_chunks;
	uint64_t 		ca_chunk_cnt;

	struct ca_chunk		**ca_free;
	uint64_t		ca_free_cnt;

	struct ca_chunk		**ca_hot;
	uint64_t		ca_hot_cnt;

	struct ca_chunk		**ca_cold;
	uint64_t		ca_cold_cnt;

	uint64_t 		ca_used_cnt;

	/*
	 * XXX Add stats back.
	 */
};

void ca_init(struct chunkallocator *ca, uint64_t offset, size_t numblocks);
int ca_alloc(struct chunkallocator *ca, int numblocks, obj_diskptr_t *ptr);
void ca_free(struct chunkallocator *ca, obj_diskptr_t tofree);
void ca_destroy(struct chunkallocator *ca);
void ca_print(struct chunkallocator *ca);

#endif /* __CHUNKALLOCATOR_H_ */
