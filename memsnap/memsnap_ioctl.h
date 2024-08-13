#ifndef _MEMSNAP_IOCTL_H_
#define _MEMSNAP_IOCTL_H_

struct slsfs_sas_create_args {
	char path[PATH_MAX];
	size_t size;
};

#define SLSFS_SAS_CREATE _IOWR('N', 104, struct slsfs_sas_create_args)
#define SLSFS_SAS_MAP _IOWR('N', 105, void *)
#define SLSFS_SAS_TRACE_START _IO('N', 106)
#define SLSFS_SAS_TRACE_END _IO('N', 107)
#define SLSFS_SAS_TRACE_ABORT _IO('N', 108)
#define SLSFS_SAS_TRACE_COMMIT _IO('N', 109)
#define SLSFS_SAS_REFRESH_PROTECTION _IO('N', 110)

#endif /* _MEMSNAP_IOCTL_H_ */
