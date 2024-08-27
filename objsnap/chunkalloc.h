#ifndef __CHUNKALLOC_H__
#define __CHUNKALLOC_H__

#define CA_CHUNKSZ (1UL * 1024 * 1024)
#define CA_BLOCKS (CA_CHUNKSZ / BLOCKSIZE)
#define CA_COLD_BUCKETS (8)

#define CA_SETALL(size) ((size) == 64 ? UINT64_MAX : ((1ULL << (size)) - 1))
#define CA_TXNSIZE (64)
#define CA_CRITICAL_WATERMARK (4)
/* 
 * This knob determines the size of the hot list. The larger the ratio, the more
 * larger the hot list ends up becoming because we require more pages to be used
 * before we start laundering hot pages into the cold list.
 */
#define CA_TOTAL_VS_FREE_RATIO (4)
/*
 * Maximum amount of pages we can launder during a single transaction.
 */
#define CA_MAXHOT_TO_COLD (8)
struct ca_objid {
	uint32_t cao_ino;
	uint32_t cao_off;
};

struct ca_chunk {
	struct mtx 		cac_mtx;
	int 			cac_index;
	obj_diskptr_t 		cac_ptr;

	struct ca_objid		cac_backmap[CA_BLOCKS];

	uint64_t 		cac_blocks_used;
	uint64_t 		cac_alloc_index;
};

struct chunkallocator {
	struct mtx  		ca_mtx;

	uint32_t		ca_txnsz_blk;

	struct ca_chunk		*ca_chunks;
	uint64_t 		ca_chunk_cnt;

	struct ca_chunk		**ca_free;
	uint64_t		ca_free_cnt;

	struct ca_chunk		**ca_hot;
	uint64_t		ca_hot_start;
	uint64_t		ca_hot_end;

	struct ca_chunk		**ca_cold[CA_COLD_BUCKETS];
	uint64_t		ca_cold_cnt[CA_COLD_BUCKETS];

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
