#ifndef __ARRAYLIST_H__
#define __ARRAYLIST_H__

#include "vtree.h"

struct arraylist {
  obj_diskptr_t *list;
  int cnt;
  int max;
};

void initlist(struct arraylist *al, int max);
void destroylist(struct arraylist *al);
void addlist(struct arraylist *al, int at, obj_diskptr_t value);
void removelist(struct arraylist *al, int index);
void reinitlist(struct arraylist *al, int to);
void appendlist(struct arraylist *al, obj_diskptr_t ptr);
void movelist(struct arraylist *a, struct arraylist *b);

// Remember its the 2^(x) * PAGE_SIZE
// Or rather its how many continguous page blocks are there.
#define INITLISTSIZE (2048)

#endif /* _ARRAYLIST_H_ */
