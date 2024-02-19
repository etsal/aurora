#ifndef __UTIL_TIME_H
#define __UTIL_TIME_H

/*
 * rdtsc.h
 *
 * Various utilities for helping with the proper benchmarking
 *
 * References:
 * 1.
 * https://www.intel.de/content/dam/www/public/us/en/documents/white-papers/ia-32-ia-64-benchmark-code-execution-paper.pdf
 *
 */

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>

#define cycles_to_ns(cycles, clock_freq)                                       \
  ((double)((double)(cycles) / (clock_freq / 1e9)))

#define cycles_to_us(cycles, clock_freq)                                       \
  ((double)((double)(cycles) / (clock_freq / 1e6)))

#define cycles_to_ms(cycles, clock_freq)                                       \
  ((double)((double)(cycles) / (clock_freq / 1e3)))

#define cycles_to_s(cycles, clock_freq)                                        \
  ((double)((double)(cycles) / (clock_freq)))

#define rdtsc() (rdtsc_cycles())
#define rdtscp() (rdtscp_cycles())

/*
 * Read System Clock
 */
static __attribute__((always_inline)) __inline__ uint64_t
rdtsc_cycles(void)
{
#if defined(_i386_)
  uint64_t ret;
  asm __volatile__("rdtsc" : "=A"(ret));
#elif defined(__x86_64__) || defined(_M_X64)
  unsigned hi, lo;
  asm __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  uint64_t ret = ((uint64_t)lo) | (((uint64_t)hi) << 32);
#else
  _Static_assert(false, "Unsupported architecture");
#endif

  return ret;
}

/*
 * Prevents out-of-order execution, this can be done with CPUID instruction as
 * well but at a much higher cost for the instruction itself
 */
static __attribute__((always_inline)) __inline__ uint64_t
rdtscp_cycles()
{
#if defined(_i386_)
  _Static_assert(false, "Unsupported architecture");
#elif defined(__x86_64__) || defined(_M_X64)
  unsigned hi, lo;
  asm __volatile__("rdtscp" : "=a"(lo), "=d"(hi));
  uint64_t ret = ((uint64_t)lo) | (((uint64_t)hi) << 32);
#else
  _Static_assert(false, "Unsupported architecture");
#endif

  return ret;
}

double
rdtsc_average()
{
  uint64_t iterations = 10000000;
  uint64_t cost1, cost2;
  double cost = 0;
  for (uint64_t i = 0; i < iterations; i++) {
    cost1 = rdtsc();
    cost2 = rdtsc();
    cost += ((double)(cost2 - cost1));
  }

  return cost / iterations;
}

double
rdtscp_average()
{
  uint64_t iterations = 10000000;
  uint64_t cost1, cost2;
  double cost = 0;
  for (uint64_t i = 0; i < iterations; i++) {
    cost1 = rdtscp();
    cost2 = rdtscp();
    cost += ((double)(cost2 - cost1));
  }

  return cost / iterations;
}

/*
 * https://www.microbe.cz/docs/CPUID.pdf
 *
 * I think there is a way to do this with CPUID, but Intel suggests
 * this way.
 *
 * Because this requires a sleep for 1 second, obviously place this as a
 * constant at the start of a program initialization phase and just reuse this
 * constant throughout.
 */
uint64_t
get_clock_speed_sleep()
{
  uint64_t start, end;
  start = rdtscp();
  sleep(1);
  end = rdtscp();
  return end - start;
}

#endif
