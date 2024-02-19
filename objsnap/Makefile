KMOD	= objsnap
CLEANFILES = .depend*
DPSRCS 	= offset.inc
SRCS	= objsnap_ioctl.c alloc.c vnode_if.h
CFLAGS	+= -DKDTRACE_HOOKS -DSMP -DKLD_TIED -I../include -g
CLEANFILES = .depend*
WITH_CTF = 1

.ifdef FASTDBG
DEBUG_FLAGS += -g3 -O0 -DINVARIANTS -DINVARIANT_SUPPORT -DKTR -DDDB
.endif


.include <bsd.kmod.mk>
