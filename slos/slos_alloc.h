#ifndef _SLOS_ALLOC_H_
#define _SLOS_ALLOC_H_

#define OTREE(slos) (&((slos)->slsfs_alloc.a_offset->sn_tree))
#define STREE(slos) (&((slos)->slsfs_alloc.a_size->sn_tree))

int slos_allocator_init(struct slos *slos);
int uint64_t_comp(const void *k1, const void *k2);
int slos_allocator_uninit(struct slos *slos);
int slos_allocator_sync(struct slos *slos, struct slos_sb *newsb);
int slos_blkalloc(struct slos *slos, size_t bytes, diskptr_t *ptr);

#endif