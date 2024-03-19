#include <stdio.h>
#include <unordered_map>
#include <set>
#include <random>
#include <mutex>
#include <iostream>
#include <cassert>
#include <chrono>

#include "binaryalloc.h"
#include "chunkalloc.h"

#define GinBlocks ((1024UL * 1024UL * 1024UL) / (4096))

std::random_device rd;
std::mt19937 gen;

using namespace std;
using namespace chrono;

struct binaryallocator ba;
struct chunkallocator ca;

std::function<void(diskptr_t *, int cnt)> txn_func;

enum AllocType {
    BinaryAllocator = 0,
    ChunkAllocator = 1,
};

double allocs = 0;
double allocs_cnt = 0;
double frees = 0;
double frees_cnt = 0;

void
printallocstats() {
    printf("Allocs - Total(%f) Count(%f) - %fus/a\n", allocs, allocs_cnt, allocs / allocs_cnt);
    printf("Frees - Total(%f) Count(%f) - %fus/f\n", frees, frees_cnt, frees / frees_cnt);
    allocs = 0;
    allocs_cnt = 0;
    frees = 0;
    frees_cnt = 0;
}

void
doalloc(void *allocator, AllocType type, int size, diskptr_t *ptr) {
    auto start = high_resolution_clock::now();
    switch (type) {
        case AllocType::BinaryAllocator:
            ba_alloc((struct binaryallocator *)allocator, size, ptr);
            break;
        case AllocType::ChunkAllocator:
            ca_alloc((struct chunkallocator *)allocator, size, ptr);
            break;
    }
    allocs += duration_cast<microseconds>(high_resolution_clock::now() - start).count();
    allocs_cnt += 1;
}

void
dofree(void *allocator, AllocType type, diskptr_t ptr) {
    auto start = high_resolution_clock::now();
    switch (type) {
        case AllocType::BinaryAllocator:
            ba_free((struct binaryallocator *)allocator, ptr);
            break;
        case AllocType::ChunkAllocator:
            ca_free((struct chunkallocator *)allocator, ptr);
            break;
    }
    frees += duration_cast<microseconds>(high_resolution_clock::now() - start).count();
    frees_cnt += 1;
}


struct stats {
    int total_allocations;    
    int total_frees;
    int total_extrafrees;
    int total_newvalues;
    int total_overallocations;
    int total_blocks;
};


int
writethis(std::set<uint32_t> &write_set, stats &st, 
    std::vector<diskptr_t> &allocation_map,
    std::mutex &mtx,
    void *allocator, AllocType type)
{
    // Allocate enough space for it
    auto start = high_resolution_clock::now();
    do {
        diskptr_t ptr;
        doalloc(allocator, type, write_set.size(), &ptr);
        st.total_allocations++;
        if (ptr.size > write_set.size()) {
            st.total_overallocations++;
        } 

        // We now need to free anything that we overwrite
        uint32_t i;
        for (i = 0; i < ptr.size; i++) {
            if (write_set.size() == 0) {
                break;
            }

            // Pop element off list
            uint32_t element = *write_set.begin();
            write_set.erase(write_set.begin());

            diskptr_t tmp = ptr;
            tmp.offset = ptr.offset + i;
            tmp.size = 1;
            mtx.lock();
            // See if we have a previous allocation there
            if (allocation_map[element].size != (uint32_t)(-1)) {
                // We do so free it; 
                st.total_frees++;
                dofree(allocator, type, allocation_map[element]);

                // Then we update our value with a new value
                allocation_map[element] = tmp;
            } else {
                // Add the value in
                allocation_map[element] = tmp;
                st.total_newvalues++;
            }
            mtx.unlock();
        }

        // We have left over allocated amount
        if (i < ptr.size) {
            diskptr_t tmp = ptr;
            tmp.offset = ptr.offset + i;
            tmp.size = ptr.size - i;
            st.total_frees++;
            st.total_extrafrees++;
            dofree(allocator, type, tmp);
        }
    } while (write_set.size());
    return duration_cast<milliseconds>(high_resolution_clock::now() - start).count();
}

int dowrite(stats &st, std::mutex &mtx, 
    std::vector<diskptr_t> &allocation_map, 
    void *allocator, AllocType type) {
    int max = GinBlocks;
    int max_writes = 16; // 64 KiB write
    std::uniform_int_distribution<> dis{0, max};
    std::uniform_int_distribution<> writes{10, max_writes};
    std::set<uint32_t> write_set;
    // Generate a random write set
    for (int i = 0; i < writes(gen); i++) {
        write_set.emplace(dis(gen));
    }
    st.total_blocks += write_set.size();

    return writethis(write_set, st, allocation_map, mtx, allocator, type);
}

void printstats(stats &st) {
    printf("========== Stats =========\n");
    printf("Total allocations: %d\n", st.total_allocations);
    printf("Total frees: %d\n", st.total_frees);
    printf("Total extra Frees: %d\n", st.total_extrafrees);
    printf("Total new values: %d\n", st.total_newvalues);
    printf("Total over allocations: %d\n", st.total_overallocations);
    printf("Total blocks: %d\n", st.total_blocks);
    printf("Total blocks / total alloctions: %f\n", (double)st.total_blocks / (double)st.total_allocations);
}

// We have to imitate a random write workload, so we collect a write set and decide what frees to do.
stats dowork(void *allocator, AllocType type, size_t disksize) {
    stats st{};
    size_t blocks = disksize / BLOCKSIZE;
    std::vector<diskptr_t> allocation_map;
    for (size_t i = 0; i < blocks; i++) {
        diskptr_t tmp;
        tmp.offset = -1;
        tmp.size = (uint32_t)-1;
        allocation_map.push_back(tmp);
    }
    std::mutex mtx;
    int maxTransactions = 100000;

    if (type == AllocType::ChunkAllocator)  {
        txn_func = [&](diskptr_t *writeset, int cnt) {
            std::set<uint32_t> writes;
            for (int i = 0; i < cnt; i++) {
                int t = 0;
                for (auto k : allocation_map) {
                    if (k.offset == writeset[i].offset) {
                        assert(k.size == writeset[i].size);
                        writes.emplace(t);
                    }
                    t++;
                }
            }
            writethis(writes, st, allocation_map, mtx, allocator, type);

            return 0;
        };
    }
    auto start = high_resolution_clock::now();
    double sum = 0;
    for (int i = 0; i < maxTransactions; i++) {
        if ((i != 0) && (i % 10000) == 0) {
            auto duration = duration_cast<milliseconds>(high_resolution_clock::now() - start);
            printf("Transactions done - %d - %ldms - %f\n", i, duration.count(), sum / 10000);
            printallocstats();
            sum = 0;
            start  = high_resolution_clock::now();
        }
        sum += dowrite(st, mtx, allocation_map, allocator, type);
    }

    return st;
};

int main() {
    stats s;
    gen = std::mt19937{rd()};
    uint64_t disksize = 1024UL * 1024UL * 1024UL * 4;

    // ba_init(&ba);
    // diskptr_t ptr;
    // ptr.offset = 0;
    // ptr.size = disksize / BLOCKSIZE;
    // ba_free(&ba, ptr);
    // s = dowork(&ba, AllocType::BinaryAllocator, disksize);
    // ba_destroy(&ba);
    // printf("Binary Allocation\n");
    // printstats(s);


    ca_init(&ca, 0, disksize, 16);
    s = dowork(&ca, AllocType::ChunkAllocator, disksize);
    printf("Chunk Allocation\n");
    printstats(s);
    ca_destroy(&ca);
    return 0;
};