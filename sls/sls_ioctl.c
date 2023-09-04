#include <sys/param.h>
#include <sys/systm.h>
#include <sys/bitstring.h>
#include <sys/capsicum.h>
#include <sys/conf.h>
#include <sys/file.h>
#include <sys/kernel.h>
#include <sys/kthread.h>
#include <sys/limits.h>
#include <sys/lock.h>
#include <sys/malloc.h>
#include <sys/md5.h>
#include <sys/module.h>
#include <sys/mutex.h>
#include <sys/protosw.h>
#include <sys/queue.h>
#include <sys/rwlock.h>
#include <sys/sbuf.h>
#include <sys/stat.h>
#include <sys/sx.h>
#include <sys/sysctl.h>
#include <sys/time.h>
#include <sys/uio.h>
#include <sys/vnode.h>
#include <sys/wait.h>

#include <vm/vm_page.h>
#include <vm/vm_param.h>

#include "debug.h"
#include "sls_internal.h"
#include "sls_io.h"
#include "sls_ioctl.h"
#include "sls_kv.h"
#include "sls_message.h"
#include "sls_metropolis.h"
#include "sls_pager.h"
#include "sls_prefault.h"
#include "sls_syscall.h"
#include "sls_table.h"
#include "sls_vm.h"

/* XXX Rename to M_SLS. */
MALLOC_DEFINE(M_SLSMM, "sls", "SLS");
MALLOC_DEFINE(M_SLSREC, "slsrec", "SLSREC");

SDT_PROVIDER_DEFINE(sls);

/* Variables set using sysctls. */
extern int sls_tracebuf;
extern int sls_only_flush_deltas;
extern uint64_t sls_successful_restores;

struct sls_metadata slsm;
struct sysctl_ctx_list aurora_ctx;

bool
sls_proc_attached(uint64_t oid, struct proc *p)
{
	struct slskv_iter iter;
	struct slspart *slsp;
	uint64_t slsp_pid;

	SLS_ASSERT_LOCKED();

	slsp = slsp_find_locked(oid);
	if (slsp == NULL)
		return (false);

	KVSET_FOREACH(slsp->slsp_procs, iter, slsp_pid)
	{
		if (p->p_pid == slsp_pid) {
			slsp_deref_locked(slsp);
			return (true);
		}
	}

	slsp_deref_locked(slsp);
	return (false);
}

bool
sls_proc_registered(struct proc *p)
{
	SLS_ASSERT_LOCKED();

	/* Check if we're already in Aurora. */
	return (p->p_auroid != 0);
}

/* Add a process into Aurora. */
void
sls_procadd_unlocked(uint64_t oid, struct proc *p)
{
	SLS_ASSERT_LOCKED();
	PROC_LOCK_ASSERT(p, MA_OWNED);

	/* Check if we're already in Aurora. */
	if (sls_proc_registered(p))
		return;

	p->p_auroid = oid;
	p->p_sysent = &slssyscall_sysvec;

	LIST_INSERT_HEAD(&slsm.slsm_plist, p, p_aurlist);
}

/* Add a process into Aurora. */
void
sls_procadd(uint64_t oid, struct proc *p)
{
	SLS_LOCK();
	PROC_LOCK(p);
	sls_procadd_unlocked(oid, p);
	PROC_UNLOCK(p);
	SLS_UNLOCK();
}

/* Remove a process from Aurora. */
void
sls_procremove(struct proc *p)
{
	SLS_ASSERT_LOCKED();
	PROC_LOCK_ASSERT(p, MA_OWNED);

	/* Check if we're already in Aurora. */
	if (!sls_proc_registered(p))
		return;

	LIST_REMOVE(p, p_aurlist);
	p->p_auroid = 0;

	return;
}

static int
sls_prockillall(void)
{
	struct proc *p, *tmp;

	/*
	 * The children take the SLS lock on exit while holding the process
	 * lock. This means that we cannot hold the SLS lock when we get the
	 * process lock, otherwise we can cause a deadlock. We are traversing
	 * the list using a safe macro, so we are allowed to drop the lock.
	 */
	LIST_FOREACH_SAFE (p, &slsm.slsm_plist, p_aurlist, tmp) {
		PROC_LOCK(p);
		kern_psignal(p, SIGKILL);
		PROC_UNLOCK(p);

		while (sls_proc_registered(p))
			cv_wait(&slsm.slsm_exitcv, &slsm.slsm_mtx);
	}

	/* Ensure all processes are dead. */
	KASSERT(LIST_EMPTY(&slsm.slsm_plist), ("processes still in Aurora"));

	return (0);
}

/*
 * Start checkpointing a partition. If a checkpointing period
 * has been set, then the partition gets periodically
 * checkpointed, otherwise it's a one-off.
 */
int
sls_checkpoint(struct sls_checkpoint_args *args)
{
	struct sls_checkpointd_args *ckptd_args;
	struct proc *p = curproc;
	struct slspart *slsp;
	uint64_t nextepoch;
	int error = 0;

	/* Take another reference for the worker thread. */
	if (sls_startop() != 0)
		return (EBUSY);

	/* Get the partition to be checkpointed. */
	slsp = slsp_find(args->oid);
	if (slsp == NULL) {
		sls_finishop();
		return (EINVAL);
	}

	if (SLSP_NOCKPT(slsp)) {
		slsp_deref(slsp);
		sls_finishop();
		return (EINVAL);
	}

	/* Set up the arguments. */
	ckptd_args = malloc(sizeof(*ckptd_args), M_SLSMM, M_WAITOK);
	ckptd_args->slsp = slsp;
	ckptd_args->pcaller = NULL;
	ckptd_args->recurse = args->recurse;
	ckptd_args->nextepoch = &nextepoch;

	/*
	 * If this is a one-off checkpoint, this thread will wait on the
	 * partition. This causes a deadlock when the daemon tries to force its
	 * process into single threading mode. Get into and out of single
	 * threading mode by itself and let the daemon know.
	 */
	if (slsp->slsp_attr.attr_period == 0) {
		PROC_LOCK(p);
		_PHOLD(p);
		thread_single(p, SINGLE_BOUNDARY);
		PROC_UNLOCK(p);
		ckptd_args->pcaller = p;
	} else {
		/* The checkpoint daemon cannot be waited on. */
		ckptd_args->nextepoch = NULL;
	}

	/* Create the daemon. */
	error = kproc_create((void (*)(void *))sls_checkpointd, ckptd_args, NULL,
	    0, 0, "sls_checkpointd");
	if (error != 0) {
		free(ckptd_args, M_SLSMM);
		goto error;
	}

	mtx_lock(&slsp->slsp_syncmtx);

	/* If it's a periodic daemon, don't wait for it. */
	if (slsp->slsp_attr.attr_period != 0) {
		mtx_unlock(&slsp->slsp_syncmtx);
		return (error);
	}

	error = slsp_waitfor(slsp);
	PROC_LOCK(p);
	thread_single_end(p, SINGLE_BOUNDARY);
	if (slsp->slsp_attr.attr_period == 0)
		_PRELE(p);
	PROC_UNLOCK(p);

	if (error != 0)
		return (error);

	/* Give the next epoch to userspace if it asks for it. */
	if (args->nextepoch != NULL)
		error = copyout(&nextepoch, args->nextepoch, sizeof(nextepoch));

	return (error);

error:
	if (slsp->slsp_attr.attr_period == 0) {
		PROC_LOCK(p);
		thread_single_end(p, SINGLE_BOUNDARY);
		_PRELE(p);
		PROC_UNLOCK(p);
	}

	slsp_deref(slsp);

	return (error);
}

int
sls_restore(struct sls_restore_args *args)
{
	struct sls_restored_args *restd_args;
	struct slspart *slsp;
	int error;

	/* Try to get a reference to the module. */
	if (sls_startop() != 0)
		return (EBUSY);

	/* Get the partition to be checkpointed. */
	slsp = slsp_find(args->oid);
	if (slsp == NULL) {
		error = EINVAL;
		goto error;
	}

	/* Check if the partition is restorable. */
	if (!slsp_restorable(slsp)) {
		error = EINVAL;
		goto error;
	}

	/* Set up the arguments. */
	restd_args = malloc(sizeof(*restd_args), M_SLSMM, M_WAITOK);
	restd_args->slsp = slsp;
	restd_args->rest_stopped = args->rest_stopped;


	/* Create the daemon. */
	error = kthread_add((void (*)(void *))sls_restored, restd_args, curproc,
	    NULL, 0, 0, "sls_restored");
	if (error != 0) {
		free(restd_args, M_SLSMM);
		goto error;
	}

	mtx_lock(&slsp->slsp_syncmtx);

	return (slsp_waitfor(slsp));

error:
	if (slsp != NULL)
		slsp_deref(slsp);

	sls_finishop();

	return (error);
}

/* Add a process to an SLS partition, allowing it to be checkpointed. */
int
sls_attach(struct sls_attach_args *args)
{
	struct proc *p;
	int error;

	/* Check whether the process exists. */
	error = pget(args->pid, PGET_WANTREAD, &p);
	if (error != 0)
		return (error);

	/* Try to add the new process. */
	error = slsp_attach(args->oid, p);

	PRELE(p);
	return (error);
}

static int
sls_socket_receive(struct slspart *slsp)
{
	struct file *fp = (struct file *)slsp->slsp_backend;
	struct thread *td = curthread;
	int error;

	if (!fhold(fp))
		return (EBADF);

	if (sls_startop()) {
		fdrop(fp, td);
		return (EBUSY);
	}

	slsp_ref(slsp);

	/* Create the daemon. */
	error = kthread_add((void (*)(void *))sls_sockrcvd, (void *)slsp, NULL,
	    NULL, 0, 0, "sls_sockrecvd");
	if (error != 0) {
		slsp_deref(slsp);
		fdrop(fp, td);
		sls_finishop();
		return (error);
	}

	return (0);
}

static int
sls_receive_stopall(void)
{
	struct slskv_iter iter;
	struct slspart *slsp;
	uint64_t oid;
	int error;

	/* Check if we aborted initialization without setting up a table. */
	if (slsm.slsm_parts == NULL)
		return (0);

	KV_FOREACH(slsm.slsm_parts, iter, oid, slsp)
	{
		if (slsp->slsp_target != SLS_SOCKRCV)
			continue;

		error = sls_write_rcvdone(slsp);
		if (error != 0)
			return (error);
	}

	return (0);
}

/* Create a new, empty partition in the SLS. */
static int
sls_partadd(struct sls_partadd_args *args)
{
	struct slspart *slsp = NULL;
	int fd = args->backendfd;
	struct proc *p = curproc;
	struct file *fp;
	int target;
	int error;

	target = args->attr.attr_target;

	/* Only full checkpoints make sense if in-memory. */
	if (target == SLS_MEM)
		args->attr.attr_mode = SLS_FULL;

	/*
	 * Check that the attributes to be passed
	 * to the SLS process are valid.
	 */
	if (args->attr.attr_mode >= SLS_MODES)
		return (EINVAL);

	if (target >= SLS_TARGETS)
		return (EINVAL);

	/* Check if the OID is in range. */
	if (args->oid < SLS_OIDMIN || args->oid > SLS_OIDMAX)
		return (EINVAL);

	/* File and remote backends need a descriptor argument. */
	if (target == SLS_FILE || target == SLS_SOCKRCV ||
	    target == SLS_SOCKSND) {
		if (fd < 0)
			return (EINVAL);
	}

	if (fd >= 0) {
		fp = FDTOFP(p, fd);

		switch (target) {
		case SLS_OSD:
		case SLS_MEM:
			return (EINVAL);
		case SLS_SOCKSND:
		case SLS_SOCKRCV:
			if (fp->f_type != DTYPE_SOCKET)
				return (EINVAL);
			break;

		case SLS_FILE:
			if (fp->f_type != DTYPE_VNODE)
				return (ENOTDIR);

			if (fp->f_vnode->v_type != VDIR)
				return (ENOTDIR);

			break;
		default:
			return (EINVAL);
		}
	}

	/* Copy the SLS attributes to be given to the new process. */
	error = slsp_add(args->oid, args->attr, fd, (void *)&slsp);
	if (error != 0)
		return (error);

	if (args->attr.attr_target == SLS_SOCKRCV)
		return (sls_socket_receive(slsp));

	/* Only register SLOS partitions. */
	if (args->attr.attr_target != SLS_OSD)
		return (0);

	/* Write out the serial representation. */
	ssparts[args->oid].sspart_valid = true;
	ssparts[args->oid].sspart_oid = slsp->slsp_oid;
	ssparts[args->oid].sspart_attr = slsp->slsp_attr;
	ssparts[args->oid].sspart_epoch = 0;

	return (0);
}

/* Remove a partition from the SLS. All processes in the partition are removed.
 */
static int
sls_partdel(struct sls_partdel_args *args)
{
	struct slspart *slsp;

	/* Try to find the process. */
	slsp = slsp_find(args->oid);
	if (slsp == NULL)
		return (EINVAL);

	/* We got a reference to the process from slsp_find, free it. */
	slsp_deref(slsp);

	/*
	 * Set the status of the partition as detached, notifying
	 * processes currently checkpointing it to exit.
	 */
	slsp_setstate(slsp, SLSP_AVAILABLE, SLSP_DETACHED, true);

	/*
	 * Check the state directly - there might be a benign race between
	 * slsp_del() instances that causes slsp_setstate() to fail.
	 */
	KASSERT(
	    slsp_getstate(slsp) == SLSP_DETACHED, ("Partition still alive"));

	/*
	 * Dereference the partition. We can't just delete it,
	 * since it might still be checkpointing.
	 */
	slsp_deref(slsp);

	return (0);
}

static int
sls_epochwait(struct sls_epochwait_args *args)
{
	struct slspart *slsp;
	int error = 0;
	bool isdone;

	/* Try to find the process. */
	slsp = slsp_find(args->oid);
	if (slsp == NULL)
		return (EINVAL);

	mtx_lock(&slsp->slsp_epochmtx);

	/* Asynchronous case, just return with an answer. */
	if (!args->sync) {
		isdone = (args->epoch <= slsp->slsp_epoch);
		error = copyout(&isdone, args->isdone, sizeof(isdone));
		goto out;
	}

	/* Synchronous case, sleep until done. */
	while (args->epoch > slsp->slsp_epoch)
		cv_wait(&slsp->slsp_epochcv, &slsp->slsp_epochmtx);

out:
	mtx_unlock(&slsp->slsp_epochmtx);

	/* Free the reference given by slsp_find(). */
	slsp_deref(slsp);
	return (error);
}

static int
sls_memsnap(struct sls_memsnap_args *args)
{
	struct proc *p = curproc;
	struct slspart *slsp;
	uint64_t nextepoch;
	int error = 0;

	/* Take another reference for the worker thread. */
	if (sls_startop() != 0)
		return (EBUSY);

	/*
	 * Try to find the process. The partition is released inside the
	 * operation.
	 */
	slsp = slsp_find(args->oid);
	if (slsp == NULL) {
		sls_finishop();
		return (EINVAL);
	}

	PHOLD(p);
	error = slsckpt_dataregion(slsp, curproc, args->addr, &nextepoch);
	PRELE(p);

	if (error != 0)
		return (error);

	/* Give the next epoch to userspace if it asks for it. */
	if (args->nextepoch != NULL)
		error = copyout(&nextepoch, args->nextepoch, sizeof(nextepoch));

	return (error);
}

static int
sls_insls(struct sls_insls_args *args)
{
	struct proc *p = curproc;
	struct slspart *slsp;
	uint64_t oid;
	bool insls;
	int error;

	insls = false;
	oid = 0;

	/*
	 * Find the partition, if it exists. If not, the process is not in the
	 * SLS and we can return.
	 */
	slsp = slsp_find(p->p_auroid);
	if (slsp == NULL)
		goto out;

	/* Look for the process inside the partition. */
	if (slsp_hasproc(slsp, p->p_pid)) {
		insls = true;
		oid = slsp->slsp_oid;
	}

	slsp_deref(slsp);

out:
	error = copyout(&oid, args->oid, sizeof(oid));
	if (error != 0)
		return (error);

	error = copyout(&insls, args->insls, sizeof(insls));
	if (error != 0)
		return (error);

	return (0);
}

static int
sls_pgresident(struct sls_pgresident_args *args)
{
	struct thread *td = curthread;
	struct slspart *slsp;
	struct file *fp;
	int error;

	slsp = slsp_find(args->oid);
	if (slsp == NULL)
		return (EINVAL);

	error = fget_write(td, args->fd, &cap_write_rights, &fp);
	if (error) {
		slsp_deref(slsp);
		return (error);
	}

	error = slspre_resident(slsp, fp);

	slsp_deref(slsp);
	fdrop(fp, td);

	return (error);
}

static int
sls_sysctl_init(void)
{
	struct sysctl_oid *root;

	sysctl_ctx_init(&aurora_ctx);

	root = SYSCTL_ADD_ROOT_NODE(&aurora_ctx, OID_AUTO, "aurora", CTLFLAG_RW,
	    0, "Aurora statistics and configuration variables");

	(void)SYSCTL_ADD_U64(&aurora_ctx, SYSCTL_CHILDREN(root), OID_AUTO,
	    "bytes_written_vfs", CTLFLAG_RD, &sls_bytes_written_vfs, 0,
	    "Bytes written using the VFS interface");
	(void)SYSCTL_ADD_U64(&aurora_ctx, SYSCTL_CHILDREN(root), OID_AUTO,
	    "bytes_read_vfs", CTLFLAG_RD, &sls_bytes_read_vfs, 0,
	    "Bytes read using the VFS interface");
	(void)SYSCTL_ADD_U64(&aurora_ctx, SYSCTL_CHILDREN(root), OID_AUTO,
	    "bytes_written_direct", CTLFLAG_RD, &sls_bytes_written_direct, 0,
	    "Bytes written using direct SLOS IO");
	(void)SYSCTL_ADD_U64(&aurora_ctx, SYSCTL_CHILDREN(root), OID_AUTO,
	    "bytes_read_direct", CTLFLAG_RD, &sls_bytes_read_direct, 0,
	    "Bytes read using direct SLOS IO");
	(void)SYSCTL_ADD_U64(&aurora_ctx, SYSCTL_CHILDREN(root), OID_AUTO,
	    "pages_grabbed", CTLFLAG_RD, &sls_pages_grabbed, 0,
	    "Pages grabbed by the SLS");
	(void)SYSCTL_ADD_U64(&aurora_ctx, SYSCTL_CHILDREN(root), OID_AUTO,
	    "io_initiated", CTLFLAG_RD, &sls_io_initiated, 0,
	    "IOs to disk initiated");
	(void)SYSCTL_ADD_U64(&aurora_ctx, SYSCTL_CHILDREN(root), OID_AUTO,
	    "contig_limit", CTLFLAG_RW, &sls_contig_limit, 0,
	    "Limit of contiguous IOs");
	(void)SYSCTL_ADD_UINT(&aurora_ctx, SYSCTL_CHILDREN(root), OID_AUTO,
	    "drop_io", CTLFLAG_RW, &sls_drop_io, 0, "Drop all IOs immediately");
	(void)SYSCTL_ADD_UINT(&aurora_ctx, SYSCTL_CHILDREN(root), OID_AUTO,
	    "vfs_sync", CTLFLAG_RW, &sls_vfs_sync, 0,
	    "Sync to the device after finishing dumping");
	(void)SYSCTL_ADD_U64(&aurora_ctx, SYSCTL_CHILDREN(root), OID_AUTO,
	    "memsnap_attempted", CTLFLAG_RW, &sls_memsnap_attempted, 0,
	    "Attempted memsnap calls");
	(void)SYSCTL_ADD_U64(&aurora_ctx, SYSCTL_CHILDREN(root), OID_AUTO,
	    "memsnap_done", CTLFLAG_RW, &sls_memsnap_done, 0,
	    "Successful memsnap calls");
	(void)SYSCTL_ADD_U64(&aurora_ctx, SYSCTL_CHILDREN(root), OID_AUTO,
	    "ckpt_attempted", CTLFLAG_RW, &sls_ckpt_attempted, 0,
	    "Checkpoints attempted");
	(void)SYSCTL_ADD_U64(&aurora_ctx, SYSCTL_CHILDREN(root), OID_AUTO,
	    "ckpt_done", CTLFLAG_RW, &sls_ckpt_done, 0,
	    "Checkpoints successfully done");
	(void)SYSCTL_ADD_U64(&aurora_ctx, SYSCTL_CHILDREN(root), OID_AUTO,
	    "ckpt_duration", CTLFLAG_RW, &sls_ckpt_duration, 0,
	    "Total run time of the checkpointer");
	(void)SYSCTL_ADD_UINT(&aurora_ctx, SYSCTL_CHILDREN(root), OID_AUTO,
	    "async_slos", CTLFLAG_RW, &sls_async_slos, 0,
	    "Asynchronous SLOS writes");
	(void)SYSCTL_ADD_UINT(&aurora_ctx, SYSCTL_CHILDREN(root), OID_AUTO,
	    "objprotect", CTLFLAG_RW, &sls_objprotect, 1,
	    "Traverse VM objects instead of entries when applying COW");
	(void)SYSCTL_ADD_UINT(&aurora_ctx, SYSCTL_CHILDREN(root), OID_AUTO,
	    "tracebuf", CTLFLAG_RW, &sls_tracebuf, 1,
	    "Use the pmap trace buffer when protecting pages");
	(void)SYSCTL_ADD_INT(&aurora_ctx, SYSCTL_CHILDREN(root), OID_AUTO,
	    "only_flush_deltas", CTLFLAG_RD, &sls_only_flush_deltas, 0,
	    "Only flush delta checkponits, blackhole the full ones");
	(void)SYSCTL_ADD_U64(&aurora_ctx, SYSCTL_CHILDREN(root), OID_AUTO,
	    "prefault_anonpages", CTLFLAG_RD, &sls_prefault_anonpages, 0,
	    "Pages prefaulted by the SLS");
	(void)SYSCTL_ADD_U64(&aurora_ctx, SYSCTL_CHILDREN(root), OID_AUTO,
	    "prefault_anonios", CTLFLAG_RD, &sls_prefault_anonios, 0,
	    "Pages prefaulted by the SLS");
	(void)SYSCTL_ADD_U64(&aurora_ctx, SYSCTL_CHILDREN(root), OID_AUTO,
	    "prefault_vnpages", CTLFLAG_RD, &sls_prefault_vnpages, 0,
	    "Total pages for vnode prefaulting");
	(void)SYSCTL_ADD_U64(&aurora_ctx, SYSCTL_CHILDREN(root), OID_AUTO,
	    "prefault_vnios", CTLFLAG_RD, &sls_prefault_vnios, 0,
	    "Total IOs for vnode prefaulted");
	(void)SYSCTL_ADD_U64(&aurora_ctx, SYSCTL_CHILDREN(root), OID_AUTO,
	    "successful_restores", CTLFLAG_RD, &sls_successful_restores, 0,
	    "Total successful restores");
	(void)SYSCTL_ADD_PROC(&aurora_ctx, SYSCTL_CHILDREN(root), OID_AUTO,
	    "prefault_invalidate", CTLTYPE_U64 | CTLFLAG_RW, NULL, 0,
	    &slspre_clear, "I", "Write 1 to invalidate all ");

	return (0);
}

static void
sls_sysctl_fini(void)
{
	if (sysctl_ctx_free(&aurora_ctx))
		printf("Failed to destroy sysctl\n");
}

static int
sls_ioctl(struct cdev *dev, u_long cmd, caddr_t data, int flag __unused,
    struct thread *td)
{
	int error = 0;

	if (sls_startop())
		return (EBUSY);

	switch (cmd) {
		/* Attach a process into an SLS partition. */
	case SLS_ATTACH:
		error = sls_attach((struct sls_attach_args *)data);
		break;

		/* Create an empty partition in the SLS. */
	case SLS_PARTADD:
		error = sls_partadd((struct sls_partadd_args *)data);
		break;

		/* Detach a partition from the SLS. */
	case SLS_PARTDEL:
		error = sls_partdel((struct sls_partdel_args *)data);
		break;

		/* Checkpoint a process already in the SLS. */
	case SLS_CHECKPOINT:
		error = sls_checkpoint((struct sls_checkpoint_args *)data);
		break;

		/* Restore a process from a backend. */
	case SLS_RESTORE:
		error = sls_restore((struct sls_restore_args *)data);
		break;

	case SLS_EPOCHWAIT:
		error = sls_epochwait((struct sls_epochwait_args *)data);
		break;

	case SLS_MEMSNAP:
		error = sls_memsnap((struct sls_memsnap_args *)data);
		break;

	case SLS_INSLS:
		error = sls_insls((struct sls_insls_args *)data);
		break;

	case SLS_PGRESIDENT:
		error = sls_pgresident((struct sls_pgresident_args *)data);
		break;

	default:
		error = EINVAL;
		break;
	}

	sls_finishop();

	return (error);
}

static struct cdevsw slsmm_cdevsw = {
	.d_version = D_VERSION,
	.d_ioctl = sls_ioctl,
};

static int
sls_partadd_default_osd(void)
{
	struct sls_partadd_args partadd_args;
	struct slspart *slsp;

	/* Check if there is already a partition. */
	slsp = slsp_find(SLS_DEFAULT_PARTITION);
	if (slsp != NULL) {
		slsp_deref(slsp);
		return (0);
	}

	partadd_args.oid = SLS_DEFAULT_PARTITION;
	partadd_args.attr.attr_mode = SLS_FULL;
	partadd_args.attr.attr_target = SLS_OSD;
	partadd_args.attr.attr_period = 0;
	partadd_args.attr.attr_flags = SLSATTR_IGNUNLINKED;
	partadd_args.attr.attr_amplification = 1;
	partadd_args.backendfd = -1;

	return (sls_partadd(&partadd_args));
}

static int
sls_partadd_default_mem(void)
{
	struct sls_partadd_args partadd_args;
	struct slspart *slsp;

	/* Check if there is already a partition. */
	slsp = slsp_find(SLS_DEFAULT_MPARTITION);
	if (slsp != NULL) {
		slsp_deref(slsp);
		return (0);
	}

	partadd_args.oid = SLS_DEFAULT_MPARTITION;
	partadd_args.attr.attr_mode = SLS_FULL;
	partadd_args.attr.attr_target = SLS_MEM;
	partadd_args.attr.attr_period = 0;
	partadd_args.attr.attr_flags = SLSATTR_IGNUNLINKED;
	partadd_args.attr.attr_amplification = 1;
	partadd_args.backendfd = -1;

	return (sls_partadd(&partadd_args));
}

static void
sls_partadd_default(void)
{
	int error;

	error = sls_partadd_default_osd();
	if (error) {
		printf("Problem creating default on-disk partition\n");
	}

	error = sls_partadd_default_mem();
	if (error) {
		printf("Problem creating default in-memory partition\n");
	}
}

static void
slsm_init_locking(void)
{
	mtx_init(&slsm.slsm_mtx, "slsm", NULL, MTX_DEF);
	cv_init(&slsm.slsm_exitcv, "slsm");
}

static void
slsm_fini_locking(void)
{
	cv_destroy(&slsm.slsm_exitcv);
	mtx_destroy(&slsm.slsm_mtx);
}

static int
slsm_init_contents(void)
{
	int error;

	error = slskv_create(&slsm.slsm_procs);
	if (error != 0)
		return (error);

	error = slskv_create(&slsm.slsm_parts);
	if (error != 0)
		return (error);

	error = slskv_create(&slsm.slsm_prefault);
	if (error != 0)
		return (error);

	return (0);
}

static void
slsm_fini_contents(void)
{
	struct sls_prefault *slspre;
	uint64_t objid;

	/* Destroy the prefault bitmaps. */
	if (slsm.slsm_prefault != NULL) {
		KV_FOREACH_POP(slsm.slsm_prefault, objid, slspre)
		slspre_destroy(slspre);
		slskv_destroy(slsm.slsm_prefault);
	}

	/* Destroy partitions. */
	if (slsm.slsm_parts != NULL) {
		slskv_destroy(slsm.slsm_parts);
		slsm.slsm_parts = NULL;
	}

	/* Remove all processes from the global table.  */
	if (slsm.slsm_procs != NULL) {
		slskv_destroy(slsm.slsm_procs);
		slsm.slsm_procs = NULL;
	}
}

static void
sls_hook_attach(void)
{
	/* Construct the system call vector. */
	slssyscall_initsysvec();
	slsmetropolis_initsysvec();
	sls_exit_hook = sls_exit_procremove;
}

static void
sls_hook_detach(void)
{
	sls_exit_hook = NULL;
	slsmetropolis_finisysvec();
	slssyscall_finisysvec();
}

static int
SLSHandler(struct module *inModule, int inEvent, void *inArg)
{
	int error = 0;

	switch (inEvent) {
	case MOD_LOAD:
		bzero(&slsm, sizeof(slsm));
		/* We need the locks if we error out before we initialize the
		 * slsm. */
		slsm_init_locking();

		/* Initialize Aurora-related sysctls. */
		sls_sysctl_init();

		SLOS_LOCK(&slos);
		if (slos_getstate(&slos) != SLOS_MOUNTED) {
			SLOS_UNLOCK(&slos);
			printf("No SLOS mount found. Aborting SLS insert.\n");
			return (EINVAL);
		}

		slos_setstate(&slos, SLOS_WITHSLS);
		SLOS_UNLOCK(&slos);

		/* Read in the serialized partition metadata. */
		error = sls_import_ssparts();
		if (error != 0)
			return (error);

		/* Enable the hashtables.*/
		error = slskv_init();
		if (error != 0)
			return (error);

		/* Initialize global module state. Depends on the KV zone. */
		error = slsm_init_contents();
		if (error != 0)
			return (error);

		/* Initialize the IO system. Depends on global module state. */
		error = slstable_init();
		if (error != 0)
			return (error);

		/* Initialize restore state. Depends on the KV zone. */
		error = slsrest_zoneinit();
		if (error != 0)
			return (error);

		/* Add the syscall vectors and hooks. */
		sls_hook_attach();

		/* Commandeer the swap pager. */
		sls_pager_register();

		/* Import existing partitions. */
		sslsp_deserialize();

		/* Create a default on-disk and in-memory partition. */
		sls_partadd_default();

		error = slspre_import();
		if (error != 0)
			return (error);

		/* Make the SLS available to userspace. */
		slsm.slsm_cdev = make_dev(
		    &slsmm_cdevsw, 0, UID_ROOT, GID_WHEEL, 0666, "sls");

		break;

	case MOD_UNLOAD:

		slsm.slsm_exiting = 1;

		error = sls_receive_stopall();
		if (error != 0) {
			SLS_UNLOCK();
			return (error);
		}

		SLS_LOCK();

		/* Wait for all operations to be done. */
		while (slsm.slsm_inprog > 0)
			cv_wait(&slsm.slsm_exitcv, &slsm.slsm_mtx);

		/* Kill all processes in Aurora, sleep till they exit. */
		DEBUG("Killing all processes in Aurora...");
		error = sls_prockillall();
		if (error != 0) {
			SLS_UNLOCK();
			return (EBUSY);
		}

		SLS_UNLOCK();

		DEBUG("Turning off the swapper...");

		/* Destroy all in-memory partition data. */
		slsp_delall();

		error = slspre_export();
		if (error != 0)
			return (error);

		SLS_LOCK();

		/* Swap off the Aurora swapper. */
		sls_pager_swapoff();
		KASSERT(slsm.slsm_swapobjs == 0, ("sls_pager_swapoff failed"));
		SLS_UNLOCK();

		sls_hook_detach();
		/*
		 * Unregister the pager. By now there are no Aurora processes
		 * alive and no objects active, so we are safe doing this
		 * operation outside the lock.
		 */
		sls_pager_unregister();

		/*
		 * Destroy the device, wait for all ioctls in progress. We do
		 * this without the non-sleepable module lock.
		 */
		if (slsm.slsm_cdev != NULL)
			destroy_dev(slsm.slsm_cdev);

		DEBUG("Cleaning up image and module state...");

		slsrest_zonefini();

		slstable_fini();
		slsm_fini_contents();
		slskv_fini();

		error = sls_export_ssparts();
		if (error != 0)
			return (error);

		SLOS_LOCK(&slos);
		/*
		 * The state might be not be SLOS_WITHSLS if we failed to
		 * load and are running this as cleanup.
		 */
		if (slos_getstate(&slos) == SLOS_WITHSLS)
			slos_setstate(&slos, SLOS_MOUNTED);
		SLOS_UNLOCK(&slos);

		sls_sysctl_fini();
		slsm_fini_locking();

		DEBUG("Done.");

		break;
	default:
		error = EOPNOTSUPP;
		break;
	}

	return (error);
}

static moduledata_t moduleData = { "sls", SLSHandler, NULL };

DECLARE_MODULE(sls, moduleData, SI_SUB_DRIVERS, SI_ORDER_MIDDLE);
MODULE_DEPEND(sls, slsfs, 0, 0, 0);
MODULE_VERSION(sls, 0);
