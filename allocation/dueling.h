#ifndef __DUELING_H__
#define __DUELING_H__
#include <cstdint>
#include <map>
#include <vector>
#include <set>

#include "binaryalloc.h"

struct size_type {
    std::vector<uint64_t>::iterator iter;
    uint64_t size;
};

struct duelingtrees {
    std::map<uint64_t, uint64_t> off_to_size;
    std::map<uint64_t, std::set<uint64_t>> size_to_off;
};

void  dt_init(struct duelingtrees *dt, size_t disksize);
int dt_alloc(struct duelingtrees *dt, struct transaction *txns, uint32_t numblocks);
int dt_free(struct duelingtrees *dt, diskptr_t ptr);
#endif