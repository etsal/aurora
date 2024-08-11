#include <sys/param.h>
#include <sys/kernel.h>
#include <sys/malloc.h>

#include "arraylist.h"
#include "objsnap_internal.h"

MALLOC_DEFINE(M_ARRAY, "Array allocator", "array allocator");


void 
initlist(struct arraylist *al, int max)
{
    al->cnt = 0;
    al->max = max;
    al->list = malloc(sizeof(diskptr_t) * max, M_ARRAY, M_WAITOK);
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
    diskptr_t *newlist = malloc(sizeof(diskptr_t) * to, 
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
    src->list = malloc(sizeof(diskptr_t) * dst->max, 
        M_ARRAY, M_WAITOK);
    src->cnt = 0;
}
