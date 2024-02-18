#include <sys/types.h>

#include "buf.h"

#define BYTE (8)
#define BLKSZ ((unsigned long)64 * 1024)
#define MAX_VALUE_SIZE (31)

/* Subtract 1 for the header */
#define RDX_NUM_ELEMS (2047)
/* Number of bits required to hold 2047 elements - 11 */
#define RADIX_BIT_WIDTH (11)
#define MAX_ARR (5)

// We use 32 bit to hold our MAX ARR
// 5 entries to hold bits of prefix (9 + 11 + 11 + 11 + 11), 11 bits of last
typedef struct rdx_node_header
{
  uint8_t depth;
  uint8_t prefix_len;
  uint16_t num_keys;
  uint16_t prefix_arr[MAX_ARR];
} rdx_node_header;

typedef struct rdx_value_cont
{
  uint8_t v_nullflag;
  unsigned char v_data[MAX_VALUE_SIZE];
} rdx_value_cont;

typedef struct rdx_node_data
{
  rdx_node_header d_hdr;
  rdx_value_cont d_values[];
} rdx_node_data;

struct rdxtree;
typedef struct rdxtree* rdxtree_t;

typedef struct rdxnode
{
  struct buf* rdx_bp;
  rdxtree_t rdx_tree;
  diskptr_t rdx_ptr;
  rdx_node_data* rdx_data;
#define rdx_blkn rdx_bp->bp_lblkno
} rdxnode;

typedef rdxnode* rdxnode_t;

typedef struct rdxtree
{
  rdxnode_t tree_root;
  diskptr_t tree_ptr;
  size_t value_size;
} rdxtree;

void
rdx_tree_init(rdxtree_t tree, diskptr_t ptr, size_t value_size);
int
rdx_insert(rdxtree_t tree, uint64_t key, void* value);
int
rdx_find(rdxtree_t tree, uint64_t key, void* value);
