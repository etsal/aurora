KMOD	= objfs
DPSRCS = offset.inc

SRCS	= objfs_vfsops.c objfs_vnops.c

CFLAGS	+= -DKDTRACE_HOOKS -DKLD_TIED -DSMP -I../include 
CLEANFILES = .depend*
WITH_CTF = 1

.ifdef PERF
DEBUG_FLAGS += -g3 -O2 -DDB
.endif

.ifdef FASTDBG
DEBUG_FLAGS += -g3 -O0 -DINVARIANTS -DINVARIANT_SUPPORT -DKTR -DDDB
WITH_CTF = 1
.endif

.ifdef SLOWDBG
DEBUG_FLAGS += -g3 -O0 -DINVARIANTS -DINVARIANT_SUPPORT -DDEBUG_LOCKS -DKTR -DWITNESS -DDDB
.endif

.include <bsd.kmod.mk>
