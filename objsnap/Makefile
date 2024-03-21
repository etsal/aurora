KMOD	= objsnap
CLEANFILES = .depend*
DPSRCS 	= offset.inc
SRCS	= binaryalloc.c objsnap_ioctl.c alloc.c btree.c vtree.c vnode_if.h
CFLAGS	+= -DKDTRACE_HOOKS -DSMP -DKLD_TIED -I../include -g
CLEANFILES = .depend*
WITH_CTF = 1

.ifdef PERF
DEBUG_FLAGS += -g3 -O2 -DDDB
.endif

.ifdef FASTDBG
DEBUG_FLAGS += -g3 -O0 -DINVARIANTS -DINVARIANT_SUPPORT -DKTR -DDDB
.endif

.ifdef SLOWDBG
DEBUG_FLAGS += -g3 -O0 -DINVARIANTS -DINVARIANT_SUPPORT -DDEBUG_LOCKS -DKTR -DDDB -DWITNESS
.endif

.include <bsd.kmod.mk>
