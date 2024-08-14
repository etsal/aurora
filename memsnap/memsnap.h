#ifndef _MEMSNAP_H_
#define _MEMSNAP_H_

#define SDI_MAXENTRIES (32 * 1024)

struct slos_meta {
	struct mtx sb_mtx;
	uint64_t sb_sas_addr;
	/*
	 * XXX We need some indexing structure for the SAS
	 * objects, since we are addressing them by name.
	 */
};

struct slos_node {
	vm_object_t sn_obj;
	vm_offset_t sn_addr;
	size_t sn_size;
};

extern void (*sls_writefault_hook)(vm_offset_t vaddr, vm_map_t map, vm_page_t m,
    int fault_type);
extern void (*sas_cow_hook)(vm_offset_t vaddr, vm_page_t *m);
void msnp_trace_update(vm_offset_t vaddr, vm_map_t map, vm_page_t m, int fault_type);
void msnp_test_cow(vm_offset_t vaddr, vm_page_t *m);

/* Turns an SLS ID to an identifier suitable for the SLOS. */
#define OIDTOSLSID(OID) ((int)(OID & INT_MAX))

#define SLS_SAS_INITADDR (0x600000000000ULL)
#define SLS_SAS_MAXADDR (0x700000000000ULL)
#define MAX_SAS_SIZE (5UL * 1024 * 1024 * 1024)

#define SLSFS_SAS_INIT _IOWR('N', 104, size_t)
#define SLSFS_SAS_MAP _IOWR('N', 105, void *)
#define SLSFS_SAS_TRACE_START _IO('N', 106)
#define SLSFS_SAS_TRACE_END _IO('N', 107)
#define SLSFS_SAS_TRACE_ABORT _IO('N', 108)
#define SLSFS_SAS_TRACE_COMMIT _IO('N', 109)
#define SLSFS_SAS_REFRESH_PROTECTION _IO('N', 110)

#endif /* _MEMSNAP_H_ */
