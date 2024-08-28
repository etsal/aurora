#ifndef __CHUNKALLOC_H__
#define __CHUNKALLOC_H__

#define CA_CHUNKSZ (1UL * 1024 * 1024)
#define CA_BLOCKS (CA_CHUNKSZ / BLOCKSIZE)
#define CA_COLD_BUCKETS (8)
#define CA_SYSTEM_INO (0xFFFFFFFF)

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


enum ca_state {
	CA_FREE,
	CA_HOT,
	CA_COLD,
	CA_SYSTEM,
	CA_SYSTEM_FULL,
};


struct ca_objid {
	uint32_t cao_ino;
	uint32_t cao_off;
};

TAILQ_HEAD(ca_system_list, ca_chunk);

struct ca_chunk {
	struct mtx 		cac_mtx;
	int 			cac_index;
	obj_diskptr_t 		cac_ptr;

	struct ca_objid		cac_backmap[CA_BLOCKS];

	uint64_t 		cac_blocks_used;
	uint64_t 		cac_alloc_index;
	enum ca_state		cac_state;
	TAILQ_ENTRY(ca_chunk)	ca_next;
};

struct chunkallocator {
	struct mtx  		ca_mtx;

	uint32_t		ca_txnsz_blk;

	struct ca_chunk		*ca_chunks;
	uint64_t 		ca_chunk_cnt;

	/* Free chunks for servicing allocations. */
	struct ca_chunk		**ca_free;
	uint64_t		ca_free_cnt;

	/* FIFO queue for recently written data. */
	struct ca_chunk		**ca_hot;
	uint64_t		ca_hot_start;
	uint64_t		ca_hot_end;

	/* Chunks that hold less recently accessed data. */
	struct ca_chunk		**ca_cold[CA_COLD_BUCKETS];
	uint64_t		ca_cold_cnt[CA_COLD_BUCKETS];

	/* Chunk lists for system blocks (used for inodes/bnodes). */
	struct ca_system_list		ca_system_alloc;
	struct ca_system_list		ca_system_full;

	/*
	 * XXX Add stats back.
	 */
};

void ca_init(struct chunkallocator *ca, uint64_t offset, size_t numblocks);
void ca_free(struct chunkallocator *ca, obj_diskptr_t tofree);
void ca_destroy(struct chunkallocator *ca);
void ca_print(struct chunkallocator *ca);
int ca_alloc_txn(struct chunkallocator *ca, struct objsnap_txn *txn);
int ca_alloc_system(struct chunkallocator *ca, obj_diskptr_t *ptr);
#endif /* __CHUNKALLOCATOR_H_ */
