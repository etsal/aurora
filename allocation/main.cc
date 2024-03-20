#include <stdio.h>
#include <unordered_map>
#include <set>
#include <random>
#include <mutex>
#include <iostream>
#include <cassert>
#include <chrono>
#include <thread>

#include "binaryalloc.h"
#include "chunkalloc.h"

#define GinBlocks ((1024UL * 1024UL * 1024UL) / (4096))

std::random_device rd;
std::mt19937 gen;

using namespace std;
using namespace chrono;

struct binaryallocator ba;
struct chunkallocator ca;

std::function<void(struct transaction *, int)> txn_func;

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
doalloc(void *allocator, AllocType type, struct transaction *txns, int size, int flag) {
    auto start = high_resolution_clock::now();
    switch (type) {
        case AllocType::BinaryAllocator:
            //ba_alloc((struct binaryallocator *)allocator, size, ptr);
            break;
        case AllocType::ChunkAllocator:
            ca_alloc((struct chunkallocator *)allocator, txns, size, flag);
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
    void *allocator, AllocType type, int flag)
{
    struct transaction txns[64];
    uint32_t txn_cnt = 0;
    while (write_set.size()) {
        uint32_t element = *write_set.begin();
        txns[txn_cnt].inode = 0;
        txns[txn_cnt].offset = element; 
        write_set.erase(write_set.begin());
        txn_cnt += 1;
    }

    // Allocate enough space for it
    auto start = high_resolution_clock::now();
    doalloc(allocator, type, txns, txn_cnt, flag);
    st.total_allocations++;

    // We now need to free anything that we overwrite
    uint32_t i;
    for (i = 0; i < txn_cnt; i++) {
        // Pop element off list
        uint32_t element = txns[i].offset;
        mtx.lock();
        // See if we have a previous allocation there
        if (allocation_map[element].size != (uint32_t)(-1)) {
            // We do so free it; 
            st.total_frees++;
            dofree(allocator, type, allocation_map[element]);

            // Then we update our value with a new value
            allocation_map[element] = txns[i].ptr;
        } else {
            // Add the value in
            allocation_map[element] = txns[i].ptr;
            st.total_newvalues++;
        }
        mtx.unlock();
    }
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(50us);

    return duration_cast<microseconds>(high_resolution_clock::now() - start).count();
}

int dowrite(stats &st, std::mutex &mtx, 
    std::vector<diskptr_t> &allocation_map, 
    void *allocator, AllocType type) {
    int max = GinBlocks;
    int max_writes = 16; // 64 KiB write
    std::uniform_int_distribution<> dis{0, max};
    std::uniform_int_distribution<> writes{15, max_writes};
    std::set<uint32_t> write_set;
    // Generate a random write set
    for (int i = 0; i < writes(gen); i++) {
        write_set.emplace(dis(gen));
    }
    st.total_blocks += write_set.size();

    return writethis(write_set, st, allocation_map, mtx, allocator, type, 0);
}

void printstats(stats &st) {
    printf("========== Stats =========\n");
    printf("Total allocations: %d\n", st.total_allocations);
    printf("Total frees: %d\n", st.total_frees);
    printf("Total extra Frees: %d\n", st.total_extrafrees);
    printf("Total new values: %d\n", st.total_newvalues);
    printf("Total over allocations: %d\n", st.total_overallocations);
    printf("Total blocks: %d\n", st.total_blocks);
    printf("Total Written: %luMiB\n", (st.total_blocks * 4096UL) / (1024 * 1024));
    printf("Total blocks / total alloctions: %f\n", (double)st.total_blocks / (double)st.total_allocations);
}

// We have to imitate a random write workload, so we collect a write set and decide what frees to do.
stats dowork(std::vector<diskptr_t> &allocation_map, std::mutex &mtx, void *allocator, AllocType type, size_t disksize) {
    stats st{};
    int maxTransactions = 1000000;

    if (type == AllocType::ChunkAllocator)  {
        txn_func = [&](struct transaction *writeset, int cnt) {
            std::set<uint32_t> writes;
            for (int i = 0; i < cnt; i++) {
                writes.emplace(writeset[i].offset);
            }
            writethis(writes, st, allocation_map, mtx, allocator, type, 1);
            return 0;
        };
    }

    auto start = high_resolution_clock::now();
    double sum = 0;
    for (int i = 0; i < maxTransactions; i++) {
        if ((i != 0) && (i % 10000) == 0) {
            auto duration = duration_cast<milliseconds>(high_resolution_clock::now() - start);
            printf("Transactions done - %d - %ldus - %f\n", i, duration.count(), sum / 10000);
            ca_print((struct chunkallocator *)allocator);
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
    std::vector<diskptr_t> allocation_map;
    std::mutex mtx;
    uint64_t disksize = 1024UL * 1024UL * 1024UL * 4;

    size_t blocks = disksize / BLOCKSIZE;
    for (size_t i = 0; i < blocks; i++) {
        diskptr_t tmp;
        tmp.offset = -1;
        tmp.size = (uint32_t)-1;
        allocation_map.push_back(tmp);
    }

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
    s = dowork(allocation_map, mtx, &ca, AllocType::ChunkAllocator, disksize);
    printf("Chunk Allocation\n");
    printstats(s);
    ca_print(&ca);
    ca_destroy(&ca);
    uint64_t inmap = 0;
    for (auto k : allocation_map) {
        if (k.size != (uint32_t)(-1)) {
            inmap += k.size;
        }
    }
    printf("Blocks allocated %luMiB\n", (inmap * BLOCKSIZE) / (1024 * 1024));
    return 0;
}