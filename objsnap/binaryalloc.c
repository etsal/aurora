#include <sys/param.h>
#include <sys/bitstring.h>
#include <sys/condvar.h>
#include <sys/fcntl.h>
#include <sys/kernel.h>
#include <sys/lock.h>
#include <sys/mutex.h>
#include <sys/proc.h>
#include <sys/queue.h>
#include <sys/sdt.h>
#include <sys/stat.h>
#include <sys/syscallsubr.h>
#include <sys/sysctl.h>
#include <sys/vnode.h>
#include <sys/taskqueue.h>
#include <sys/errno.h>

#include "objsnap_internal.h"

#include "binaryalloc.h"

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
	mtx_init(&ba->ba_lock, "Binary allocator Lock", NULL, MTX_DEF);
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

    panic("Bucket could not be determined %d", numblocks);
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

    KASSERT(into->cnt == 0, ("Bucket should be empty!"));


    if (from->cnt == 0) {
        error = split_above(ba, splitbucket);
        if (error) {
            panic("Out of space\n");
        }

        KASSERT(from->cnt == 2, ("Should be two available after split"));
    }

    // Only ever going up 1 power of two so split it into two
    if (!allocate_from_bucket(from, &ptr)) {
        uint32_t splitinto = ptr.size / 2; 
        diskptr_t tmpptr;
        KASSERT(splitinto == (1 << bucket), ("Incorrect splitting\n"));
        for (int i = 0; i < 2; i++) {
            tmpptr.offset = ptr.offset + (splitinto * i);
            tmpptr.size = splitinto;
            into->list[into->cnt] = tmpptr;
            into->cnt += 1;
        }
        return (0);
    } 

    panic("Should not reach here");
}

int
ba_alloc(struct binaryallocator *ba, int numblocks, diskptr_t *ptr)
{
    struct arraylist *f;

    mtx_lock(&ba->ba_lock);
    int bucket = determine_bucket(numblocks);
    BADEBUG("[BA] Allocation asked of size %d decided on bucket %d\n", numblocks, bucket);

    f = &ba->ba_flists[bucket];
    if (!allocate_from_bucket(f, ptr)) {
        mtx_unlock(&ba->ba_lock);
        BADEBUG("[BA] Successfully allocated ptr at %u of size %u\n", ptr->offset, ptr->size);
        return (0);
    }

    // Were not able to find a ptr in our bucket, now
    // need to split our boys
    if (split_above(ba, bucket)) {
        mtx_unlock(&ba->ba_lock);
        return ENOSPC;
    }

    // We were successful in finding a split so
    // try allocation again.
    if (!allocate_from_bucket(f, ptr)) {
        mtx_unlock(&ba->ba_lock);
        BADEBUG("[BA with split] Successfully allocated ptr at %u of size %u\n",
            ptr->offset, ptr->size);
        return (0);
    }

    panic("Allocation should have been successful!");

    return ENOSPC;
}

static void 
ba_free_unlocked(struct binaryallocator *ba, diskptr_t tofree)
{
    struct arraylist *f;
    int bucket = determine_bucket(tofree.size);
    f = &ba->ba_flists[bucket];
    int next;
    KASSERT(tofree.size == (1 << bucket), ("Pointer does not belong in this bucket\n"));

    // Edge case of empty list
    if (f->cnt == 0) {
        f->list[0] = tofree;
        f->cnt = 1;
        return;
    }

    // We go through the list to determine 
    // TODO: We need to order from largest to smalled so we can
    // pop off the tail easily when allocating
    for (next = 0; next < f->cnt; next++) {
        if (f->list[next].offset == tofree.offset) {
            panic("Value already found?\n");
        }

        if (f->list[next].offset > tofree.offset) {
            break;
        }
    }

    // MERGE!
    if ((tofree.offset + tofree.size) == f->list[next].offset && 
        (bucket != MAXPOWEROFTWO)) {
        KASSERT(f->list[next].size == tofree.size, 
            ("We want to merge, they are different sizes\n"));
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
    mtx_lock(&ba->ba_lock);
    ba_free_unlocked(ba, tofree);
    mtx_unlock(&ba->ba_lock);
}
