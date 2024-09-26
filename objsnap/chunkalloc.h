#ifndef __CHUNKALLOC_H__
#define __CHUNKALLOC_H__

#define CA_CHUNKSZ (8UL * 1024 * 1024)
#define CA_BLOCKS (2048)
#define CA_COLD_BUCKETS (1)
#define CA_NOBUCKET (-1)

#define CA_SYSTEM_INO (0xFFFFFFFF)
#define CA_COLD_LOAD_THRESHOLD (3 * CA_BLOCKS / 10)
#define CA_FREESLOTS (8)
#define CA_HOT_CHUNKS_PERCENT (4)

/* Dummy TID for the GC move thread. */
#define CA_GC_TID (MAXTHREADS - 1)

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
	CA_NOQUEUE,
	CA_FREE,
	CA_HOT,
	CA_COLD,
	CA_SYSTEM,
	CA_SYSTEM_FULL,
	CA_LAUNDER,
	CA_BLKALLOC,
	CA_STATES,
};

struct ca_objid {
	uint32_t cao_ino;
	uint32_t cao_off;
	enum ca_state cao_state;
};

TAILQ_HEAD(ca_system_list, ca_chunk);

struct ca_chunk {
	struct mtx 		cac_mtx;
	int 			cac_index;
	obj_diskptr_t 		cac_ptr;

	struct ca_objid		cac_backmap[CA_BLOCKS];

	uint64_t 		cac_blocks_used;
	uint64_t 		cac_alloc_index;
	uint64_t 		cac_clean_index;
	enum ca_state		cac_state;
	uint8_t			cac_cold_bucket;
	TAILQ_ENTRY(ca_chunk)	cac_next;
	vm_page_t		cac_launder[CA_BLOCKS];
	size_t			cac_movable;
	int			cac_laundered;
};

struct ca_stats {
	uint64_t 		cs_pop_from_free;
	uint64_t 		cs_free_to_hot;
	uint64_t 		cs_free_to_launder;
	uint64_t 		cs_hot_to_free;
	uint64_t 		cs_hot_to_cold;
	uint64_t 		cs_hot_to_launder;
	uint64_t 		cs_cold_to_launder;
	uint64_t 		cs_cold_to_free;
	uint64_t 		cs_launder_to_cold;
	uint64_t 		cs_free_to_system;
	uint64_t 		cs_system_to_full;
	uint64_t 		cs_full_to_system;
	uint64_t 		cs_op_alloc_fast[CA_FREESLOTS];
	uint64_t 		cs_op_aging;
	uint64_t 		cs_op_move;
	uint64_t 		cs_op_alloc_fast_fail;
	uint64_t 		cs_page_alloc_fast[CA_FREESLOTS];
	uint64_t 		cs_page_moves;
	uint64_t 		cs_hot_blocks_used;
	uint64_t 		cs_op_free;
	uint64_t 		cs_op_gc_failed;
	uint64_t 		cs_reclaimed_to_free;
	uint64_t 		cs_op_age_io;
	uint64_t 		cs_page_alloc;
	uint64_t 		cs_page_free;
	uint64_t 		cs_page_launder;
	uint64_t 		cs_page_reclaim;
	uint64_t 		cs_op_cleanup_successful;
};

struct chunkallocator {
	struct mtx  		ca_mtx;

	uint64_t		ca_startoff;

	struct ca_chunk		*ca_chunks;
	uint64_t 		ca_chunk_cnt;

	/* Free slots for quick sharded allocations. */
	struct ca_chunk		*ca_free_slots[CA_FREESLOTS];
	struct ca_chunk		*ca_launder_slots[CA_FREESLOTS];
	struct mtx		ca_slot_mtx[CA_FREESLOTS];

	/* Free chunks for servicing allocations. */
	struct ca_chunk		**ca_free;
	uint64_t		ca_free_cnt;

	/* FIFO queue for recently written data. */
	struct ca_chunk		**ca_hot;
	uint64_t		ca_hot_start;
	uint64_t		ca_hot_end;

	/* Chunks that hold less recently accessed data. */
	struct ca_system_list	ca_cold[CA_COLD_BUCKETS];
	uint64_t		ca_cold_cnt[CA_COLD_BUCKETS + 1];

	/* Chunk lists for system blocks (used for inodes/bnodes). */
	struct ca_system_list		ca_system_alloc;
	struct ca_system_list		ca_system_full;

	struct ca_system_list		ca_launder;
	uint64_t			ca_launder_cnt;
	struct ca_chunk			*ca_launder_dst;

	struct ca_stats			ca_stats;
};

void ca_init(struct chunkallocator *ca, uint64_t offset, size_t numblocks);
void ca_free(struct chunkallocator *ca, obj_diskptr_t tofree);
void ca_destroy(struct chunkallocator *ca);
void ca_print(struct chunkallocator *ca);
int ca_alloc_txn(struct chunkallocator *ca, int tid, struct objsnap_txn *txn);
int ca_alloc_system(struct chunkallocator *ca, obj_diskptr_t *ptr);
void ca_gc(struct chunkallocator *ca, size_t numblocks);

void ca_integrity_check(void);

#endif /* __CHUNKALLOCATOR_H_ */
