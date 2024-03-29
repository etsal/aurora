#ifndef _SLS_DATA_H_
#define _SLS_DATA_H_

#include <sys/param.h>
#include <sys/selinfo.h>
#include <sys/pcpu.h>
#include <sys/pipe.h>
#include <sys/proc.h>
#include <sys/sbuf.h>
#include <sys/socket.h>
#include <sys/tty.h>
#include <sys/ttycom.h>
#include <sys/un.h>

#include <vm/vm.h>
#include <vm/pmap.h>
#include <vm/vm_map.h>

#include <machine/param.h>
#include <machine/pcb.h>
#include <machine/reg.h>

#include <netinet/in.h>
#include <netinet/in_pcb.h>

#define SLSPROC_ID 0x736c7301
struct slsproc {
	uint64_t magic;		  /* Magic value */
	uint64_t slsid;		  /* Unique object ID */
	size_t nthreads;	  /* Threads in a process */
	pid_t pid;		  /* PID of the process */
	pid_t pgid;		  /* ID of the process group */
	uint64_t pptr;		  /* Unique object ID of the parent proc */
	uint64_t pgrpwait;	  /* Should we wait for the process group
				     to be rebuilt? */
	pid_t sid;		  /* Session ID */
	uint64_t textvp;	  /* ID of the text vnode */
	struct sigacts sigacts;	  /* Signal handling info */
	char name[MAXCOMLEN + 1]; /* Name of the process */
};

#define SLSTHREAD_ID 0x736c7302
struct slsthread {
	uint64_t magic;
	uint64_t slsid;
	struct reg regs;
	struct fpreg fpregs;
	lwpid_t tid;
	sigset_t sigmask;
	sigset_t oldsigmask;
	uint64_t fs_base;
	uint64_t tf_err;
	uint64_t tf_trapno;
};

/* State of the vmspace, but also of its vm_map */
#define SLSVMSPACE_ID 0x736c730a
struct slsvmspace {
	uint64_t magic;
	uint64_t slsid;
	/* State of the vmspace object */
	segsz_t vm_swrss;
	segsz_t vm_tsize;
	segsz_t vm_dsize;
	segsz_t vm_ssize;
	caddr_t vm_taddr;
	caddr_t vm_daddr;
	caddr_t vm_maxsaddr;
	int nentries;
	int has_shm;
};

#define SLSSESSION_ID 0x736c730b
struct slssession {
	uint64_t magic;
	uint64_t slsid;
	uint64_t tty;
	uint64_t sid;
	uint64_t leader;
};

#define SLSVMOBJECT_ID 0x7abc7303
struct slsvmobject {
	uint64_t magic;
	vm_object_t objptr; /* The object pointer itself */
	uint64_t slsid;
	vm_pindex_t size;
	enum obj_type type;
	/* Used for objects that are shadows of others */
	uint64_t backer;
	vm_ooffset_t backer_off;
	uint64_t vnode; /* Backing SLS vnode */
};

#define SLSVNODE_ID 0xbaba9001
struct slsvnode {
	uint64_t magic;
	uint64_t slsid;
	int has_path;
	uint64_t ino;
	char path[PATH_MAX]; /* Filesystem path for vnode VM objects. */
};

#define SLSVMENTRY_ID 0x736c7304
/* State of a vm_map_entry and its backing object */
struct slsvmentry {
	uint64_t magic;
	uint64_t slsid;
	/* State of the map entry */
	vm_offset_t start;
	vm_offset_t end;
	vm_ooffset_t offset;
	vm_eflags_t eflags;
	vm_prot_t protection;
	vm_prot_t max_protection;
	uint64_t obj;
	vm_inherit_t inheritance;
	enum obj_type type;
	uint64_t vp;
};

#define SLSFILE_ID 0x736c7234
#define SLSSTRING_ID 0x72626f72
struct slsfile {
	uint64_t magic;
	uint64_t slsid;

	short type;
	u_int flag;

	off_t offset;
	uint64_t vnode;
	uint64_t ino;
	/*
	 * Let's not bother with this flag from the filedescent struct.
	 * It's only about autoclosing on exec, and we don't really care right
	 * now
	 */
	/* uint8_t fde_flags; */
};

#define SLSFILEDESC_ID 0x736c7233
struct slsfiledesc {
	uint64_t magic;
	uint64_t slsid;

	uint64_t cdir;
	uint64_t rdir;
	/* TODO jdir */

	u_short fd_cmask;
	uint64_t fd_lastfile;
	/* Needs to be last to ease (de)serialization is trivial. */
	uint64_t fd_table[];
};

#define SLSPIPE_ID 0x736c7499
struct slspipe {
	uint64_t magic;		/* Magic value */
	uint64_t slsid;		/* Unique SLS ID */
	uint64_t iswriteend;	/* Is this the write end? */
	uint64_t peer;		/* The SLS ID of the other end */
	struct pipebuf pipebuf; /* The pipe's buffer. */
	/* XXX Valid only at restore time */
	void *data;
};

#define SLSKQUEUE_ID 0x736c7265
struct slskqueue {
	uint64_t magic;
	uint64_t slsid; /* Unique SLS ID */
};

#define SLSKNOTE_ID 0x736c7115
struct slsknote {
	uint64_t magic; /* Magic value */
	uint64_t slsid; /* Unique SLS ID */
	int kn_status;
	struct kevent kn_kevent;
	int kn_sfflags;
	int64_t kn_sdata;
};

#define SLSSOCKET_ID 0x736c7268
struct slssock {
	/* SLS Object options */
	uint64_t magic;
	uint64_t slsid; /* Unique SLS ID */
	uint64_t state; /* Flags */

	/* Socket-wide options */
	int16_t family;
	int16_t type;
	int16_t proto;
	uint32_t options;

	/* UNIX or INET addresses */
	union {
		struct sockaddr_un un;
		struct sockaddr_in in;
	};

	uint64_t unpeer; /* UNIX socket peer */
	uint64_t bound;	 /* Is the socket bound? */

	/* XXX Maybe encapsulate INET state? */
	struct in_conninfo in_conninfo;
	uint64_t backlog;

	uint64_t rcvid; /* SLS ID for receive buffer */
	uint64_t sndid; /* SLS ID for send buffer */

	uint64_t peer_rcvid; /* SLS ID for peer's receive buffer */
	uint64_t peer_sndid; /* SLS ID for peer's send buffer */

	uint64_t vnode; /* Underlying UNIX socket vnode */
};

#define SLSPTS_ID 0x736c7269
struct slspts {
	uint64_t magic;
	uint64_t slsid;

	/*
	 * Find out if it's the master or the
	 * slave side. If it's the slave side
	 * we only need the peer's ID.
	 */
	uint64_t ismaster;
	uint64_t peerid;
	uint64_t flags;

	int drainwait;
	struct termios termios;
	struct winsize winsize;
	unsigned int column;
	unsigned int writepos;
	struct termios termios_init_in;
	struct termios termios_lock_in;
	struct termios termios_init_out;
	struct termios termios_lock_out;
	/* XXX Valid only at restore time */
	size_t inqlen;
	size_t outqlen;
	void *inq;
	void *outq;
};

#define SLSSYSVSHM_ID 0x736c7232
struct slssysvshm {
	uint64_t magic;
	uint64_t slsid;
	uint64_t shm_segsz;
	mode_t mode;
	key_t key;
	int segnum;
	int seq;
};

#define SLSPOSIXSHM_ID 0x736c7230
struct slsposixshm {
	uint64_t magic;
	uint64_t slsid;
	mode_t mode;
	uint64_t object;
	bool is_named;
	char path[PATH_MAX];
};

#define SLSMBUF_ID 0x736c7245
struct slsmbuf {
	uint64_t slsid; /* Unique identifier for the sockbuf mbufs  */
	uint64_t magic; /* Magic for helping debug parsing */
	uint8_t type;	/* Type of mbuf (data, control...) */
	uint64_t flags; /* Data-related flags */
	size_t len;	/* Size of data */
};

#endif /* _SLS_DATA_H_ */
