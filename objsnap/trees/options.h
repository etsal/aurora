#ifndef _OPTIONS_H_
#define _OPTIONS_H_

#define NS (1e9)
#define KB (1UL * 1024)
#define MB (1UL * 1024 * KB)
#define GB (1UL * 1024 * MB)

/* Size of LRU */
#define LRU_CAPACITY (10000)

/*
 * Throughput of underlying simulated device, used to calculate
 * latency of a buffer cache read when used with DISK_LATENCY on
 */
#define THROUGHPUT (2UL * GB)

/*
 * On LRU cache miss induce disk latency
 */
// #define DISK_LATENCY (1)

/*
 * This feature is not enabled be default as it will
 * hurt our LRU cache and induce increased latency
 */
// #define CHECK_OLD_TREE (1)

#endif
