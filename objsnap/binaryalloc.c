#include <sys/param.h>
#include <sys/bitstring.h>
#include <sys/condvar.h>
#include <sys/fcntl.h>
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

MALLOC_DEFINE(M_OBJSNAP_BA, "binary allocator", "binary allocator");

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
        printf("[%d, %u] %d\n", i, 1 << i, ba->ba_flists[i].f_cnt);
    }
}

void 
ba_init(struct binaryallocator *ba)
{
    struct freelist *f;
	mtx_init(&ba->ba_lock, "Binary allocator Lock", NULL, MTX_DEF);
    for (int i = 0; i < (MAXPOWEROFTWO + 1); i++) {
        f = &ba->ba_flists[i];
        f->f_cnt = 0;
        f->f_max = INITLISTSIZE;
        f->f_lists = malloc(sizeof(diskptr_t) * INITLISTSIZE, M_OBJSNAP_BA, M_WAITOK);
    }
}

void 
ba_destroy(struct binaryallocator *ba)
{
    struct freelist *f;

    for (int i = 0; i < MAXPOWEROFTWO + 1; i++) {
        f = &ba->ba_flists[i];
        free(f->f_lists, M_OBJSNAP_BA);
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
allocate_from_bucket(struct freelist *f, diskptr_t *ptr)
{
    if (f->f_cnt) {
        *ptr = f->f_lists[f->f_cnt - 1];
        f->f_cnt -= 1;

        return (0);
    }

    return ENOSPC;
}

static void
expand_freelist(struct freelist *f)
{
    diskptr_t *newlist = malloc(sizeof(diskptr_t) * (f->f_max * 2), M_OBJSNAP_BA, M_WAITOK);
    memcpy(newlist, f->f_lists, f->f_max * sizeof(diskptr_t));
    f->f_max = f->f_max * 2;
    free(f->f_lists, M_OBJSNAP_BA);
    f->f_lists = newlist;
}

static int
split_above(struct binaryallocator *ba, int bucket) 
{
    struct freelist *from, *into;
    diskptr_t ptr;
    int error;
    int splitbucket = bucket + 1;

    if (bucket == MAXPOWEROFTWO + 1) {
        return ENOSPC;
    }

    from = &ba->ba_flists[splitbucket];
    into = &ba->ba_flists[bucket];

    KASSERT(into->f_cnt == 0, ("Bucket should be empty!"));


    if (from->f_cnt == 0) {
        error = split_above(ba, splitbucket);
        if (error) {
            panic("Out of space\n");
        }

        KASSERT(from->f_cnt == 2, ("Should be two available after split"));
    }

    // Only ever going up 1 power of two so split it into two
    if (!allocate_from_bucket(from, &ptr)) {
        uint32_t splitinto = ptr.size / 2; 
        diskptr_t tmpptr;
        KASSERT(splitinto == (1 << bucket), ("Incorrect splitting\n"));
        for (int i = 0; i < 2; i++) {
            tmpptr.offset = ptr.offset + (splitinto * i);
            tmpptr.size = splitinto;
            into->f_lists[into->f_cnt] = tmpptr;
            into->f_cnt += 1;
        }
        return (0);
    } 

    panic("Should not reach here");
}

int
ba_alloc(struct binaryallocator *ba, int numblocks, diskptr_t *ptr)
{
    struct freelist *f;

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
    struct freelist *f;
    int bucket = determine_bucket(tofree.size);
    f = &ba->ba_flists[bucket];
    int next;
    KASSERT(tofree.size == (1 << bucket), ("Pointer does not belong in this bucket\n"));

    // Edge case of empty list
    if (f->f_cnt == 0) {
        f->f_lists[0] = tofree;
        f->f_cnt = 1;
        return;
    }

    // We go through the list to determine 
    // TODO: We need to order from largest to smalled so we can
    // pop off the tail easily when allocating
    for (next = 0; next < f->f_cnt; next++) {
        if (f->f_lists[next].offset == tofree.offset) {
            panic("Value already found?\n");
        }

        if (f->f_lists[next].offset > tofree.offset) {
            break;
        }
    }

    // MERGE!
    if ((tofree.offset + tofree.size) == f->f_lists[next].offset && 
        (bucket != MAXPOWEROFTWO)) {
        KASSERT(f->f_lists[next].size == tofree.size, 
            ("We want to merge, they are different sizes\n"));
        tofree.size = tofree.size * 2;            
        memmove(&f->f_lists[next], &f->f_lists[next + 1], 
            sizeof(diskptr_t) * (f->f_cnt - next - 1));
        f->f_cnt -= 1;
        ba_free_unlocked(ba, tofree);

        return;
    }

    if (f->f_cnt == f->f_max) {
        expand_freelist(f);
    }

    memmove(&f->f_lists[next + 1], &f->f_lists[next], 
        sizeof(diskptr_t) * (f->f_cnt - next));
    f->f_lists[next] = tofree;
    f->f_cnt += 1;
}

void
ba_free(struct binaryallocator *ba, diskptr_t tofree)
{
    mtx_lock(&ba->ba_lock);
    ba_free_unlocked(ba, tofree);
    mtx_unlock(&ba->ba_lock);
}
