#include <stdio.h>
#include <unordered_map>
#include <set>
#include <random>
#include <mutex>
#include <iostream>
#include <cassert>
#include <chrono>
#include <thread>
#include <unistd.h>
#include <cstring>
#include <sstream>
#include <fstream>

#include "binaryalloc.h"
#include "chunkalloc.h"
#include "dueling.h"

#define GinBlocks ((1024UL * 1024UL * 1024UL) / (4096))

std::random_device rd;
std::mt19937 gen;

using namespace std;
using namespace chrono;

std::vector<uint64_t> heat_map;

struct binaryallocator ba;
struct duelingtrees dt;
struct chunkallocator ca;

#define GIB (1024UL * 1024UL * 1024UL)

int maxTransactions = 2000000; // Number of txns to do
int max_writes = 64; // 64 KiB write
int min_writes = 16; // 4096 
uint64_t disksize = GIB * 64;
uint64_t max_obj_size = 48 * GinBlocks; // Max object size
uint64_t size_of_hotset = 8 * GinBlocks; // Max object size


std::function<void(struct transaction *, int)> txn_func;

enum AllocType {
    BinaryAllocator = 0,
    ChunkAllocator = 1,
    DuelingTrees = 2,
};
AllocType type = AllocType::ChunkAllocator;

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
convert_to_ba_alloc(struct binaryallocator *allocator, struct transaction *txns, int size)
{
    int cur = 0;
    while (cur < size) {
        diskptr_t ptr;
        ba_alloc((struct binaryallocator *)allocator, size, &ptr);
        for (uint32_t i = 0; i < ptr.size; i++) {
            txns[cur + i].ptr.offset = ptr.offset + i;
            txns[cur + i].ptr.size = 1;
        }
        cur += ptr.size;
    }

}


void
doalloc(void *allocator, AllocType type, struct transaction *txns, int size, int flag) {
    auto start = high_resolution_clock::now();
    switch (type) {
        case AllocType::BinaryAllocator:
            convert_to_ba_alloc((struct binaryallocator *)allocator, txns, size);
            break;
        case AllocType::ChunkAllocator:
            ca_alloc((struct chunkallocator *)allocator, txns, size, flag);
            break;
        case AllocType::DuelingTrees:
            dt_alloc((struct duelingtrees *)allocator, txns, size);
            break;
    }
    allocs += duration_cast<microseconds>(high_resolution_clock::now() - start).count();
    allocs_cnt += 1;

}
struct allocatorstats 
getstat(void *allocator, AllocType type) 
{
    struct allocatorstats null{};
    switch (type) {
        case AllocType::BinaryAllocator:
            return null;
        case AllocType::ChunkAllocator:
            return ca_stat((struct chunkallocator *)allocator);
        case AllocType::DuelingTrees:
            return null;
    }

    return null;
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
        case AllocType::DuelingTrees:
            dt_free((struct duelingtrees *)allocator, ptr);
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
        assert(txns[i].ptr.size == 1);
        // See if we have a previous allocation there
        if (allocation_map[element].size != (uint32_t)(-1)) {
            // We do so free it; 
            st.total_frees++;
            dofree(allocator, type, allocation_map[element]);
            // Then we update our value with a new value
            allocation_map[element] = txns[i].ptr;
            heat_map[txns[i].ptr.offset] += 1;
        } else {
            // Add the value in
            allocation_map[element] = txns[i].ptr;
            heat_map[txns[i].ptr.offset] += 1;
            st.total_newvalues++;
        }
        mtx.unlock();
    }
    using namespace std::chrono_literals;
    //std::this_thread::sleep_for(50us);

    return duration_cast<microseconds>(high_resolution_clock::now() - start).count();
}

int HOTPERCENTAGE = 90;
int dowrite(stats &st, std::mutex &mtx, 
    std::vector<diskptr_t> &allocation_map, 
    void *allocator, AllocType type) {
    std::uniform_int_distribution<uint64_t> dis_hot{0, size_of_hotset};
    std::uniform_int_distribution<uint64_t> dis_nothot{size_of_hotset, max_obj_size};
    std::uniform_int_distribution<uint64_t> writes{min_writes, max_writes};
    std::uniform_int_distribution<uint64_t> hot_probability{1, 100};
    std::set<uint32_t> write_set;
    // Generate a random write set
    for (int i = 0; i < writes(gen); i++) {
        int s;

        bool isHot = hot_probability(gen) < HOTPERCENTAGE;
        if (isHot)
            s = int(dis_hot(gen));
        else
            s = int(dis_nothot(gen));

        if (s < 0) {
            s = 0;
        }

        if (s > max_obj_size) {
            s = max_obj_size;
        }

        write_set.emplace(s);
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

uint64_t
check_allocated(std::vector<diskptr_t> &allocation_map) {
    uint64_t count = 0;
    for (const auto &k: allocation_map) {
        if (k.size != (uint32_t)(-1)) {
            count += 1;
        }
    }
    return count;
}

uint64_t 
print_allocation_map(std::vector<diskptr_t> &allocation_map, int per_x_blocks) {
    uint64_t inmap = 0;
    for (auto k : allocation_map) {
        if (k.size != (uint32_t)(-1)) {
            assert(k.size == 1);
            inmap += k.size;
        }
    }
    printf("Blocks allocated %luMiB\n", (inmap * BLOCKSIZE) / (1024 * 1024));
    printf("Heatmap per %d blocks, %lu\n", per_x_blocks, heat_map.size());
    auto iter = heat_map.begin();
    std::vector<uint64_t> heat;
    uint64_t non_zero = 0;
    while(iter != heat_map.end()) {
        uint64_t sum = 0;
        for (int i = 0; i < per_x_blocks; i++) {
            sum += *iter;
            iter++;
        }
        if (sum > 0)
            non_zero++;

        heat.push_back(sum);
    }

    for (auto k: heat) {
        printf("%lu,", k);
    }
    printf("\n");
    return inmap;
}

// We have to imitate a random write workload, so we collect a write set and decide what frees to do.
stats dowork(std::vector<diskptr_t> &allocation_map, std::mutex &mtx, void *allocator, AllocType type) {
    stats st{};

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
    auto print_per = 10000;
    double sum = 0;
    std::stringstream fn;

    fn << min_writes << "min-" << max_writes << "max-" << disksize / GIB << "ds-" << max_obj_size / GinBlocks;
    fn << "objsize-" << size_of_hotset / GinBlocks << "hot-" << maxTransactions << "-" << enable_old_chunks << ".csv";
    std::ofstream outfile(fn.str(), std::ios::out);

    for (int i = 0; i < maxTransactions; i++) {
        if ((i != 0) && (i % print_per) == 0) {
            std::stringstream ss;
            auto duration = duration_cast<milliseconds>(high_resolution_clock::now() - start);
            ss << i << ", " << st.total_blocks << ", " << st.total_allocations;
            ss << ", " << sum / print_per;
            sum = 0;
            start  = high_resolution_clock::now();
            auto allocstats = getstat(allocator, type);
            for (int i = 0; i < allocstats.numStats; i++) {
                ss << ", " << allocstats.list[i];
            }

            ss << std::endl;
            outfile << ss.str();
            printf(ss.str().c_str());
            auto size = check_allocated(allocation_map);
            if (allocstats.list[allocstats.numStats - 1] != (allocation_map.size() - size)) {
                printf("Listed Free Blocks: %lu\n", allocstats.list[allocstats.numStats - 1]);
                printf("Real Free Blocks: %lu\n", (allocation_map.size() - size));
                printf("List Allocated: %lu\n", allocstats.list[allocstats.numStats - 4]);
                printf("Real Allocated: %lu\n", size);
                assert(false);
            }

        }
        sum += dowrite(st, mtx, allocation_map, allocator, type);
    }

    return st;
};



static void 
reset(std::vector<diskptr_t> &allocation_map, uint64_t blocks) {
    allocation_map.clear();
    heat_map.clear();
    for (size_t i = 0; i < blocks; i++) {
        diskptr_t tmp;
        tmp.offset = (uint32_t)-1;
        tmp.size = (uint32_t)-1;
        allocation_map.push_back(tmp);
        heat_map.push_back(0);
    }
}

void usage()
{
    printf("Usage: allocator tester tool\n");
    printf("  -h\t\tPrint usage instructions\n");
    printf("  -x\t\tNumber of transaction to perform\n");
    printf("  -t\t\tType of allocator: dt = dueling trees, chunk = chunk allocator\n");
    printf("  -l\t\tMin number of writes in a transaction\n");
    printf("  -m\t\tMax number of writes in a transaction\n");
    printf("  -s\t\tSize of disk in GiB\n");
    printf("  -o\t\tSize of object in GiB\n");
    printf("  -e\t\tSize of the hot set in GiB (Will span 2 std deviations of the object)\n");
    printf("  -c\t\tEnable old chuck allocation (Chunk Allocator only)\n");
}

int main(int argc, char *argv[]) {
    int opt;
    stats s;
    gen = std::mt19937{rd()};
    std::vector<diskptr_t> allocation_map;
    std::mutex mtx;

    while ((opt = getopt(argc, argv, "x:t:l:m:s:o:he:c")) != -1) {
        switch (opt) {
            case 't':
                if (strcmp(optarg, "dt")) {
                    type = AllocType::DuelingTrees;
                } else {
                    type = AllocType::ChunkAllocator;
                }
                break; 
            case 'x':
                maxTransactions = strtoull(optarg, NULL, 10);
                break;
            case 'm':
                max_writes = atoi(optarg);
                break;
            case 'l':
                min_writes = atoi(optarg);
                break;
            case 's':
                disksize = GIB * strtoull(optarg, NULL, 10);
                break;
            case 'o':
                max_obj_size = GinBlocks * atoi(optarg);
                break;
            case 'e':
                size_of_hotset = strtoull(optarg, NULL, 10) * GinBlocks;
                break;
            case 'c':
                enable_old_chunks = 1;
                break;
            case 'h':
                usage();
                exit(0);
        }
    }

    uint64_t blocks = disksize / (uint64_t)BLOCKSIZE;
    if (blocks < max_obj_size) {
        printf("Disk size too small! Reduce max object size or increase disksize\n");
        return -1;
    }

    reset(allocation_map, blocks);
    assert(allocation_map.size() == blocks);
    switch (type) {
        case AllocType::ChunkAllocator: {
            ca_init(&ca, 0, disksize, 64);
            s = dowork(allocation_map, mtx, &ca, AllocType::ChunkAllocator);
            ca_destroy(&ca);
            return (0);
        }
        case AllocType::BinaryAllocator: {
            ba_init(&ba);
            diskptr_t ptr;
            ptr.offset = 0;
            ptr.size = disksize / BLOCKSIZE;
            ba_free(&ba, ptr);
            s = dowork(allocation_map, mtx, &ba, AllocType::BinaryAllocator);
            ba_destroy(&ba);
            printf("Binary Allocation\n");
            printstats(s);
            return (0);
        }
        case AllocType::DuelingTrees: {
            dt_init(&dt, disksize);
            s = dowork(allocation_map, mtx, &dt, AllocType::DuelingTrees);
            printstats(s);
            print_allocation_map(allocation_map, 256 * 1024);
            return (0);
        }
    }
    return 0;
}