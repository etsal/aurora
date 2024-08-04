#ifndef __BINARYALLOC_H__
#define __BINARYALLOC_H__

#include <cstdint>
#include <pthread.h>
#include <stdlib.h>

#define NULLDISKPTR ((diskptr_t)-1)
#define BADINDEX ((index_t)(-1))

typedef struct diskptr
{
  uint32_t offset;
  uint32_t size;
} diskptr_t;

struct allocatorstats
{
  uint64_t list[64];
  uint64_t numStats;
};

#define malloc(a, b, c) malloc(a)
#define free(a, b) free(a)

struct arraylist
{
  diskptr_t* list;
  int cnt;
  int max;
};

void
initlist(struct arraylist* al, int max);
void
destroylist(struct arraylist* al);
void
addlist(struct arraylist* al, int at, diskptr_t value);
void
removelist(struct arraylist* al, int index);
void
reinitlist(struct arraylist* al, int to);
void
appendlist(struct arraylist* al, diskptr_t ptr);
void
movelist(struct arraylist* a, struct arraylist* b);

// Remember its the 2^(x) * PAGE_SIZE
// Or rather its how many continguous page blocks are there.
#define INITLISTSIZE (2048)

struct binaryallocator
{
  pthread_mutex_t ba_lock;
  struct arraylist ba_flists[MAXPOWEROFTWO + 1];
};

void
ba_init(struct binaryallocator* ba);
int
ba_alloc(struct binaryallocator* ba, int numblocks, diskptr_t* ptr);
void
ba_free(struct binaryallocator* ba, diskptr_t tofree);
void
ba_destroy(struct binaryallocator* ba);
void
ba_print(struct binaryallocator* ba);
#endif
