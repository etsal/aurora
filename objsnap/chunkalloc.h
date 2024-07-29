#ifndef __BINARYALLOC_H__
#define __BINARYALLOC_H__

struct chunkallocator {
    struct mtx  ca_lock;
};

void ca_init(struct chunkallocator *ba);
int ca_alloc(struct chunkallocator *ba, int numblocks, diskptr_t *ptr);
void ca_free(struct chunkallocator *ba, diskptr_t tofree);
void ca_destroy(struct chunkallocator *ba);
void ca_print(struct chunkallocator *ba);

#endif /* __CHUNKALLOCATOR_H_ */
