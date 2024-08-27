#ifndef __CHUNKALLOC_H__
#define __CHUNKALLOC_H__

#define CA_CHUNKSZ (1UL * 1024 * 1024)
#define CA_BLOCKS (CA_CHUNKSZ / BLOCKSIZE)

struct ca_objid {
	uint32_t cao_ino;
	uint32_t cao_off;
};

struct ca_sector {
};

struct ca_chunk {
	struct mtx 		cac_mtx;
	int 			cac_index;
	obj_diskptr_t 		cac_ptr;

	struct ca_objid		cac_backmap[CA_BLOCKS];

	uint64_t 		cac_blocks_used;
	uint64_t 		cac_alloc_index;
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
