#ifndef _MEMSNAP_H_
#define _MEMSNAP_H_

#include <pthread.h>
#include <limits.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define SLS_SAS_INITADDR (0x600000000000ULL)
#define SLS_SAS_MAXADDR (0x700000000000ULL)
#define MAX_SAS_SIZE (20UL * 1024 * 1024 * 1024)

int slsfs_sas_create(char *path, size_t size);
int slsfs_sas_map(int fd, void **addrp);

int sas_trace_start(int fd);
int sas_trace_end(int fd);
int sas_trace_abort(int fd);
int sas_trace_commit(int fd);
int sas_refresh_protection(int fd);

#ifdef __cplusplus
}
#endif

#endif /* _MEMSNAP_H_ */
