#include "dueling.h"
#include "chunkalloc.h"
#include <cassert>
#include <errno.h>
#include <sys/types.h>

static int
determine_bucket(uint64_t numblocks)
{
  int i = 1;
  int shift;
  for (shift = 0; shift <= 31; shift++) {
    if (numblocks <= (i << shift)) {
      return (shift);
    }
  }
  assert(false);
}
static void
partition(struct duelingtrees* dt, diskptr_t ptr)
{
  uint32_t off = ptr.offset;
  uint32_t blocks = ptr.size;
  while (blocks) {
    int bucket = determine_bucket(blocks);
    if (blocks < (1 << bucket)) {
      bucket--;
    }

    dt->size_to_off[1 << bucket].insert(off);
    dt->off_to_size[off] = (1 << bucket);
    blocks -= (1 << bucket);
    off += (1 << bucket);
  }
}

void
dt_init(struct duelingtrees* dt, size_t disksize)
{
  uint64_t blocks = disksize / BLOCKSIZE;
  diskptr_t ptr;
  ptr.size = blocks;
  ptr.offset = 0;
  partition(dt, ptr);
}

int
dt_alloc(struct duelingtrees* dt, struct transaction* txns, uint32_t numblocks)
{
  diskptr_t ptr;
  ptr.size = -1;
  uint32_t original_numblocks = numblocks;
  uint32_t cur = 0;
  while (numblocks && (cur < original_numblocks)) {
    // Try to find our value
    auto iter = dt->size_to_off.begin();
    for (auto k = dt->size_to_off.begin(); k != dt->size_to_off.end(); k++) {
      if (k->first >= numblocks && k->second.size()) {
        iter = k;
      }
    }

    if (iter != dt->size_to_off.end() && iter->second.size()) {
      auto offset = iter->second.begin();
      iter->second.erase(iter->second.begin());
      ptr.size = iter->first;
      ptr.offset = *offset;
      assert(dt->off_to_size[*offset] == iter->first);
      dt->off_to_size.erase(*offset);
    } else {
      numblocks = numblocks << 1;
      continue;
    }

    // We allocated too much so free the extra
    if ((cur + ptr.size) > original_numblocks) {
      diskptr_t tmp;
      auto used = original_numblocks - cur;
      tmp.size = cur + ptr.size - original_numblocks;
      tmp.offset = ptr.offset + used;
      // Update our allocated pointer to indicate to use
      // only the size we need
      ptr.size = used;
      dt_free(dt, tmp);
    }
    // We found an allocation. So lets set it
    for (uint32_t i = 0; i < ptr.size; i++) {
      txns[cur + i].ptr.offset = ptr.offset + i;
      txns[cur + i].ptr.size = 1;
    }
    cur += ptr.size;
  }

  // for (uint32_t i = 0; i < original_numblocks; i++) {
  //     printf("%u %u %u %u\n",i, txns[i].ptr.size, original_numblocks, cur);
  //     assert(txns[i].ptr.size == 1);
  // }

  if (cur != original_numblocks) {
    // We have to refree our allocations, this should not happen though in our
    // testing.
    return (ENOSPC);
  }

  // for (auto k: dt->off_to_size) {
  //     auto v = std::find(dt->size_to_off[k.second].begin(),
  //         dt->size_to_off[k.second].end(), k.first);
  //     assert(v != dt->size_to_off[k.second].end());
  // }

  return (0);
}

int
dt_free(struct duelingtrees* dt, diskptr_t ptr)
{
  // auto iter_before = dt->off_to_size.upper_bound(ptr.offset);
  // // The ptr before us can be merged with the freed ptr;
  // if ((iter_before->first + iter_before->second) == ptr.offset) {
  //     // Remove the element from the size list
  //     dt->size_to_off[iter_before->second].erase(iter_before->first);
  //     iter_before->second += ptr.size;
  //     dt->size_to_off[iter_before->second].insert(iter_before->first);
  //     return (0);
  // }
  // auto iter_next = dt->off_to_size.find(ptr.offset + ptr.size);
  // if (iter_next != dt->off_to_size.end()) {
  //     ptr.size += iter_next->second;
  //     dt->size_to_off[iter_next->second].erase(iter_next->first);
  //     dt->off_to_size.erase(iter_next);
  // }

  partition(dt, ptr);
  // Insert our value
  // dt->size_to_off[ptr.size].insert(ptr.offset);
  // dt->off_to_size[ptr.offset] = ptr.size;

  return (0);
}
