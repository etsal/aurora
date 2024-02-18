KMOD	= objsnap
CLEANFILES = .depend*
DPSRCS 	= offset.inc
SRCS	= objsnap_ioctl.c vnode_if.h
CFLAGS	+= -DKDTRACE_HOOKS -DSMP -DKLD_TIED -I../include -g -fPIC
CLEANFILES = .depend*
WITH_CTF = 1

.ifdef SLS_MSG
DEBUG_FLAGS += -DSLS_MSG
.endif

.ifdef SLS_TEST
DEBUG_FLAGS += -DSLS_TEST
.endif

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
