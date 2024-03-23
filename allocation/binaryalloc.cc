#include "binaryalloc.h"

#include <stdio.h>
#include <errno.h>

#include <cassert>
#include <cstring>

#ifdef BINARY_ALLOCATOR_DEBUG 
#define BADEBUG(...) BADEBUG(__VA_ARGS__)
#else
#define BADEBUG(...) 
#endif

void 
ba_print(struct binaryallocator *ba)
{
    printf("Binary Allocator State:\n");
    for (int i = 0; i < (MAXPOWEROFTWO + 1); i++) {
        printf("[%d, %u] %d\n", i, 1 << i, ba->ba_flists[i].cnt);
    }
}

void 
ba_init(struct binaryallocator *ba)
{
    struct arraylist *f;
	pthread_mutex_init(&ba->ba_lock, NULL);
    for (int i = 0; i < (MAXPOWEROFTWO + 1); i++) {
        f = &ba->ba_flists[i];
        initlist(f, INITLISTSIZE);
    }
}

void 
ba_destroy(struct binaryallocator *ba)
{
    struct arraylist *f;

    for (int i = 0; i < MAXPOWEROFTWO + 1; i++) {
        f = &ba->ba_flists[i];
        destroylist(f);
    }
}

static int 
determine_bucket(int numblocks)
{
    int i = 1;
    int shift;
    for (shift = 0; shift <= MAXPOWEROFTWO; shift++) {
        if (numblocks <= (i << shift)) {
            return (shift);
        }
    }

    assert(false);
}

static int 
allocate_from_bucket(struct arraylist *f, diskptr_t *ptr)
{
    if (f->cnt) {
        *ptr = f->list[f->cnt - 1];
        f->cnt -= 1;

        return (0);
    }

    return ENOSPC;
}


static int
split_above(struct binaryallocator *ba, int bucket) 
{
    struct arraylist *from, *into;
    diskptr_t ptr;
    int error;
    int splitbucket = bucket + 1;

    if (bucket == MAXPOWEROFTWO + 1) {
        return ENOSPC;
    }

    from = &ba->ba_flists[splitbucket];
    into = &ba->ba_flists[bucket];

    assert(into->cnt == 0);


    if (from->cnt == 0) {
        error = split_above(ba, splitbucket);
        if (error) {
            return (error);
        }
        assert(from->cnt == 2);
    }

    // Only ever going up 1 power of two so split it into two
    if (!allocate_from_bucket(from, &ptr)) {
        uint32_t splitinto = ptr.size / 2; 
        diskptr_t tmpptr;
        assert(splitinto == (1U << bucket));
        for (int i = 0; i < 2; i++) {
            tmpptr.offset = ptr.offset + (splitinto * i);
            tmpptr.size = splitinto;
            into->list[into->cnt] = tmpptr;
            into->cnt += 1;
        }
        return (0);
    } 

    assert(false);
}

int
ba_alloc(struct binaryallocator *ba, int numblocks, diskptr_t *ptr)
{
    struct arraylist *f;

    pthread_mutex_lock(&ba->ba_lock);
    int bucket = determine_bucket(numblocks);
    BADEBUG("[BA] Allocation asked of size %d decided on bucket %d\n", numblocks, bucket);

    f = &ba->ba_flists[bucket];
    if (!allocate_from_bucket(f, ptr)) {
        pthread_mutex_unlock(&ba->ba_lock);
        if (numblocks < ptr->size) {
            diskptr_t tmp;
            tmp.offset = ptr->offset + numblocks;
            tmp.size = ptr->size - numblocks;
            ba_free(ba, tmp);
        }

        ptr->size = numblocks;
        BADEBUG("[BA] Successfully allocated ptr at %u of size %u\n", ptr->offset, ptr->size);
        return (0);
    }

    // Were not able to find a ptr in our bucket, now
    // need to split our boys
    if (split_above(ba, bucket)) {
        pthread_mutex_unlock(&ba->ba_lock);

        // We are as small as we can get.
        if (numblocks == 1) {;
            return ENOSPC;
        }

        // Try an allocate a smaller amount;
        return ba_alloc(ba, numblocks >> 1, ptr);
    }

    // We were successful in finding a split so
    // try allocation again.
    if (!allocate_from_bucket(f, ptr)) {
        pthread_mutex_unlock(&ba->ba_lock);
        BADEBUG("[BA with split] Successfully allocated ptr at %u of size %u\n",
            ptr->offset, ptr->size);
        if (numblocks < ptr->size) {
            diskptr_t tmp;
            tmp.offset = ptr->offset + numblocks;
            tmp.size = ptr->size - numblocks;
            ba_free(ba, tmp);
        }

        ptr->size = numblocks;
        return (0);
    }

    assert(false);

    return ENOSPC;
}

static void 
ba_free_unlocked(struct binaryallocator *ba, diskptr_t tofree)
{
    struct arraylist *f;
    int bucket = determine_bucket(tofree.size);
    f = &ba->ba_flists[bucket];
    int next;
    if (tofree.size < (1U << bucket)) {
        diskptr_t tmp;
        tmp.offset = tofree.offset;
        tmp.size = 1U << (bucket - 1);
        ba_free_unlocked(ba, tmp);
        tmp.offset = tofree.offset + tmp.size;
        tmp.size = tofree.size - tmp.size;
        return ba_free_unlocked(ba, tmp);
    }

    // Edge case of empty list
    if (f->cnt == 0) {
        f->list[0] = tofree;
        f->cnt = 1;
        return;
    }

    // MERGE!
    if ((tofree.offset + tofree.size) == f->list[next].offset && 
        (bucket != MAXPOWEROFTWO)) {
        assert(f->list[next].size == tofree.size);
        tofree.size = tofree.size * 2;            
        removelist(f, next);
        ba_free_unlocked(ba, tofree);

        return;
    }

    if (f->cnt == f->max) {
        reinitlist(f, f->max * 2);
    }

    addlist(f, next, tofree);
}

void
ba_free(struct binaryallocator *ba, diskptr_t tofree)
{
    pthread_mutex_lock(&ba->ba_lock);
    ba_free_unlocked(ba, tofree);
    pthread_mutex_unlock(&ba->ba_lock);
}

void 
initlist(struct arraylist *al, int max)
{
    al->cnt = 0;
    al->max = max;
    al->list = (diskptr_t *)malloc(sizeof(diskptr_t) * max, M_ARRAY, M_WAITOK);
}

void 
destroylist(struct arraylist *al)
{
    free(al->list, M_ARRAY);
}

void 
addlist(struct arraylist *al, int at, diskptr_t value) 
{
    if (al->cnt == al->max)
        reinitlist(al, al->max * 2);

    memmove(&al->list[at + 1], &al->list[at], 
        sizeof(diskptr_t) * (al->cnt - at));
    al->list[at] = value;
    al->cnt += 1;
}

void 
removelist(struct arraylist *al, int index) 
{
    memmove(&al->list[index], &al->list[index + 1], 
        sizeof(diskptr_t) * (al->cnt - index - 1));
    al->cnt -= 1;
}

void
appendlist(struct arraylist *al, diskptr_t ptr)
{
    if (al->cnt == al->max)
        reinitlist(al, al->max * 2);

    al->list[al->cnt] = ptr;
    al->cnt += 1;
}


void
reinitlist(struct arraylist *f, int to)
{
    diskptr_t *newlist = (diskptr_t *)malloc(sizeof(diskptr_t) * to, 
        M_ARRAY, M_WAITOK);
    int amount = to < f->cnt ? to : f->cnt;
    memcpy(newlist, f->list, amount * sizeof(diskptr_t));
    f->max = to;
    free(f->list, M_ARRAY);
    f->list = newlist;
}


void 
movelist(struct arraylist *dst, struct arraylist *src)
{
    free(dst->list, M_ARRAY);
    dst->list = src->list;
    dst->max = src->max;
    dst->cnt = src->cnt;
    src->list = (diskptr_t *)malloc(sizeof(diskptr_t) * dst->max, 
        M_ARRAY, M_WAITOK);
    src->cnt = 0;
}