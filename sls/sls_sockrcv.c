#include <sys/param.h>
#include <sys/file.h>
#include <sys/kthread.h>
#include <sys/lock.h>
#include <sys/pcpu.h>
#include <sys/rwlock.h>
#include <sys/wait.h>

#include <vm/vm.h>
#include <vm/vm_extern.h>
#include <vm/vm_object.h>
#include <vm/vm_page.h>
#include <vm/vm_pager.h>

#include <sls_ioctl.h>
#include <sls_message.h>

#include "debug.h"
#include "sls_backend.h"
#include "sls_internal.h"
#include "sls_io.h"
#include "sls_table.h"

struct sls_sockrcvd_state {
	bool slsrcvd_done;
	struct slsckpt_data *slsrcvd_sckpt;
	struct file *slsrcvd_sock;
	struct slspart *slsrcvd_slsp;
	uint64_t slsrcvd_epoch;
	vm_object_t slsrcvd_obj;
};

struct sockaddr_in recvbk_addrs[SLS_OIDRANGE];
uint64_t recvbk_active;

struct sls_backend_recv {
	int recvbk_type;
	struct sls_backend_ops *recvbk_ops;
	LIST_ENTRY(sls_backend) recvbk_backends;
};

static void
slsrcvd_fini(struct sls_sockrcvd_state *rcvd)
{
	struct thread *td = curthread;

	if (rcvd->slsrcvd_sckpt != NULL)
		slsckpt_drop(rcvd->slsrcvd_sckpt);

	if (rcvd->slsrcvd_obj != NULL)
		vm_object_deallocate(rcvd->slsrcvd_obj);

	if (rcvd->slsrcvd_sock != NULL)
		fdrop(rcvd->slsrcvd_sock, td);

	slsp_deref(rcvd->slsrcvd_slsp);
}

static int
slsrcvd_ckptstart(struct sls_sockrcvd_state *rcvd, struct slsmsg_ckptstart *msg)
{
	struct slsckpt_data *sckpt;
	int error;

	error = slsckpt_alloc(rcvd->slsrcvd_slsp, &sckpt);
	if (error != 0)
		return (error);

	rcvd->slsrcvd_sckpt = sckpt;
	rcvd->slsrcvd_epoch = msg->slsmsg_epoch;
	return (0);
}

static int
slsrcvd_recpages(struct sls_sockrcvd_state *rcvd, struct slsmsg_recpages *msg)
{
	size_t count = msg->slsmsg_len / PAGE_SIZE;
	vm_object_t obj = rcvd->slsrcvd_obj;
	vm_pindex_t pindex;
	vm_page_t *ma;
	int error;
	char *buf;
	int ret;
	int i;

	ma = malloc(sizeof(vm_page_t) * count, M_SLSMM, M_WAITOK);
	pindex = (msg->slsmsg_offset / PAGE_SIZE) - SLOS_OBJOFF;

	VM_OBJECT_WLOCK(obj);
	ret = vm_page_grab_pages(obj, pindex, VM_ALLOC_NORMAL, ma, count);
	KASSERT(ret == count, ("blocking allocation failed"));
	vm_object_pip_add(obj, count);

	for (i = 0; i < count; i++) {
		ma[i]->valid = 0;
		ma[i]->oflags |= VPO_SWAPINPROG;
	}

	VM_OBJECT_WUNLOCK(obj);

	for (i = 0; i < count; i++) {
		buf = (char *)PHYS_TO_DMAP(VM_PAGE_TO_PHYS(ma[i]));
		error = slsio_fpread(rcvd->slsrcvd_sock, buf, PAGE_SIZE);
		if (error != 0)
			goto error;

		ma[i]->valid = VM_PAGE_BITS_ALL;
		ma[i]->oflags &= ~VPO_SWAPINPROG;
		vm_page_xunbusy(ma[i]);
	}

	free(ma, M_SLSMM);

	VM_OBJECT_WLOCK(obj);
	vm_object_pip_add(obj, -count);
	VM_OBJECT_WUNLOCK(obj);

	return (0);

error:
	for (; i < count; i++)
		vm_page_xunbusy(ma[i]);

	free(ma, M_SLSMM);

	VM_OBJECT_WLOCK(obj);
	vm_object_pip_add(obj, -count);
	VM_OBJECT_WUNLOCK(obj);

	return (error);
}

static int
slsrcvd_recmeta_manifest(
    struct sls_sockrcvd_state *rcvd, struct slsmsg_recmeta *msg)
{
	size_t buflen, data_idlen;
	char *origbuf, *buf;
	int error;

	buf = malloc(msg->slsmsg_metalen, M_SLSMM, M_WAITOK);
	error = slsio_fpread(rcvd->slsrcvd_sock, buf, msg->slsmsg_metalen);
	if (error != 0) {
		free(buf, M_SLSMM);
		return (error);
	}

	buflen = msg->slsmsg_metalen;
	origbuf = buf;

	/* Ignore the data array. */
	data_idlen = *(uint64_t *)buf;
	buf += sizeof(data_idlen);
	buflen -= sizeof(data_idlen);
	buf += data_idlen * sizeof(uint64_t);
	buflen -= data_idlen * sizeof(uint64_t);

	error = sls_readmeta(buf, buflen, rcvd->slsrcvd_sckpt->sckpt_rectable);

	free(origbuf, M_SLSMM);

	return (error);
}

static int
slsrcvd_recmeta_vmobject(
    struct sls_sockrcvd_state *rcvd, struct slsmsg_recmeta *msg)
{
	struct sls_record *rec;
	struct slsvmobject info;
	struct sbuf *sb;
	vm_object_t obj;
	int error;

	KASSERT(msg->slsmsg_rectype == SLOSREC_VMOBJ, ("invalid record type"));

	if (msg->slsmsg_metalen != sizeof(info))
		return (EBADMSG);

	sb = sbuf_new_auto();

	error = slsio_fpread(rcvd->slsrcvd_sock, &info, msg->slsmsg_metalen);
	if (error != 0) {
		sbuf_delete(sb);
		return (error);
	}

	error = sbuf_bcat(sb, &info, sizeof(info));
	if (error != 0) {
		sbuf_delete(sb);
		return (error);
	}

	sbuf_finish(sb);

	rec = sls_getrecord(sb, msg->slsmsg_uuid, SLOSREC_VMOBJ);

	error = slskv_add(rcvd->slsrcvd_sckpt->sckpt_rectable, msg->slsmsg_uuid,
	    (uintptr_t)rec);
	if (error != 0) {
		sls_record_destroy(rec);
		return (error);
	}

	if (info.type != OBJT_DEFAULT && info.type != OBJT_SWAP)
		return (0);

	obj = vm_pager_allocate(OBJT_DEFAULT, NULL, IDX_TO_OFF(info.size),
	    VM_PROT_DEFAULT, 0, NULL);
	obj->objid = msg->slsmsg_uuid;
	vm_object_reference(obj);

	error = slskv_add(rcvd->slsrcvd_sckpt->sckpt_shadowtable, (uint64_t)obj,
	    (uintptr_t)NULL);
	if (error != 0) {
		vm_object_deallocate(obj);
		vm_object_deallocate(obj);
		return (error);
	}

	if (rcvd->slsrcvd_obj != NULL) {
		KASSERT(msg->slsmsg_uuid != rcvd->slsrcvd_obj->objid,
		    ("duplicate vmobject"));
		vm_object_deallocate(rcvd->slsrcvd_obj);
	}

	rcvd->slsrcvd_obj = obj;

	return (0);
}

static int
slsrcvd_recmeta(struct sls_sockrcvd_state *rcvd, struct slsmsg_recmeta *msg)
{
	switch (msg->slsmsg_rectype) {
	case SLOSREC_MANIFEST:
		return (slsrcvd_recmeta_manifest(rcvd, msg));

	case SLOSREC_VMOBJ:
		return (slsrcvd_recmeta_vmobject(rcvd, msg));

	default:
		SLS_WARN("Invalid message type %ld\n", msg->slsmsg_rectype);
		return (EINVAL);
	}
}

static int
slsrcvd_ckptdone(struct sls_sockrcvd_state *rcvd)
{
	struct slspart *slsp = rcvd->slsrcvd_slsp;
	struct thread *td = curthread;
	bool unused;
	int error;

	if (!slsckpt_prepare_state(slsp, &unused))
		return (EBUSY);

	/* XXX Need a special compact operation */
	if (slsp->slsp_sckpt == NULL)
		slsp->slsp_sckpt = rcvd->slsrcvd_sckpt;
	else
		slsckpt_compact(slsp, rcvd->slsrcvd_sckpt);
	rcvd->slsrcvd_sckpt = NULL;

	fdrop(rcvd->slsrcvd_sock, td);
	rcvd->slsrcvd_sock = NULL;

	error = slsp_setstate(slsp, SLSP_CHECKPOINTING, SLSP_AVAILABLE, false);
	KASSERT(error == 0, ("partition not in ckpt state"));

	return (0);
}

static int
slsrcvd_message(struct sls_sockrcvd_state *rcvd)
{
	enum slsmsgtype msgtype;
	union slsmsg msg;
	int error;

	error = slsio_fpread(rcvd->slsrcvd_sock, &msg, sizeof(msg));
	if (error != 0)
		return (error);

	msgtype = *(enum slsmsgtype *)&msg;
	switch (msgtype) {
	case SLSMSG_REGISTER:
		/*
		 * This message is only necessary for userspace
		 * servers that must create the partition before
		 * sending over the checkpoint. Ignore it and close
		 * the connection.
		 */
		error = slsrcvd_ckptdone(rcvd);
		break;
	case SLSMSG_CKPTSTART:
		error = slsrcvd_ckptstart(
		    rcvd, (struct slsmsg_ckptstart *)&msg);
		break;

	case SLSMSG_RECMETA:
		error = slsrcvd_recmeta(rcvd, (struct slsmsg_recmeta *)&msg);
		break;

	case SLSMSG_RECPAGES:
		error = slsrcvd_recpages(rcvd, (struct slsmsg_recpages *)&msg);
		break;

	case SLSMSG_CKPTDONE:
		error = slsrcvd_ckptdone(rcvd);
		break;

	case SLSMSG_DONE:
		rcvd->slsrcvd_done = true;
		error = 0;
		break;

	default:
		SLS_WARN("Invalid message type %d\n", msgtype);
		error = EINVAL;
	}

	return (error);
}

void
sls_sockrcvd(struct slspart *slsp)
{
	struct file *listenfp = (struct file *)slsp->slsp_backend;
	struct sls_sockrcvd_state rcvd;
	struct thread *td = curthread;
	int listenfd = -1;
	int error;

	bzero(&rcvd, sizeof(rcvd));
	rcvd.slsrcvd_slsp = slsp;

	error = finstall(td, listenfp, &listenfd, 0, NULL);
	if (error != 0) {
		fdrop(listenfp, td);
		goto out;
	}

	do {
		if (rcvd.slsrcvd_sock == NULL) {
			error = kern_accept(
			    td, listenfd, NULL, NULL, &rcvd.slsrcvd_sock);
			if (error != 0)
				break;
		}

		error = slsrcvd_message(&rcvd);
		if (error != 0)
			break;
	} while (!rcvd.slsrcvd_done);

out:
	if (error != 0)
		printf("BUG: SLS receive daemon exited with %d\n", error);

	if (listenfd > 0)
		kern_close(td, listenfd);

	slsrcvd_fini(&rcvd);

	atomic_subtract_64(&recvbk_active, 1);

	kthread_exit();
}

static int
recvbk_setup(struct sls_backend *slsbk)
{
	return (0);
}

static int
recvbk_teardown(struct sls_backend *slsbk)
{
	return (0);
}

static int
recvbk_import(struct sls_backend *slsbk)
{
	return (0);
}

static int
recvbk_terminate(uint64_t oid)
{
	struct thread *td = curthread;
	struct slsmsg_done *donemsg;
	union slsmsg msg;
	int error;
	int fd;

	error = slssnd_connect(&recvbk_addrs[oid], &fd);
	if (error != 0)
		return (error);

	donemsg = (struct slsmsg_done *)&msg;
	donemsg->slsmsg_type = SLSMSG_DONE;

	error = slsio_fdwrite(fd, (char *)&msg, sizeof(msg), NULL);
	if (error != 0) {
		SLS_WARN("slsio_fdwrite failed with %d\n", error);
		return (error);
	}

	error = kern_close(td, fd);
	if (error != 0)
		SLS_WARN("kern_close failed with %d\n", error);

	return (0);
}

static int
recvbk_export(struct sls_backend *slsbk)
{
	uint64_t oid;
	int error;

	for (oid = 0; oid < SLS_OIDRANGE; oid++) {
		if (recvbk_addrs[oid].sin_len == 0)
			continue;

		error = recvbk_terminate(oid);
		if (error != 0)
			return (error);
	}

	while (recvbk_active > 0)
		pause_sbt("recvbk", SBT_1MS, 0, C_HARDCLOCK);

	return (0);
}

static int
recvbk_partadd(struct sls_backend *slsbk, struct slspart *slsp)
{
	struct file *fp = (struct file *)slsp->slsp_backend;
	struct thread *td = curthread;
	struct sockaddr_in *sadst;
	int error;

	if (!fhold(fp))
		return (EBADF);

	if (sls_startop()) {
		fdrop(fp, td);
		return (EBUSY);
	}

	slsp_ref(slsp);

	sadst = &recvbk_addrs[slsp->slsp_oid];
	memcpy(sadst, slsp->slsp_name, sizeof(*sadst));

	/* Create the daemon. */
	error = kthread_add((void (*)(void *))sls_sockrcvd, (void *)slsp, NULL,
	    NULL, 0, 0, "sls_sockrcvd");
	if (error != 0) {
		slsp_deref(slsp);
		fdrop(fp, td);
		sls_finishop();
		return (error);
	}

	atomic_add_64(&recvbk_active, 1);

	sls_finishop();

	return (0);
}

static int
recvbk_setepoch(struct sls_backend *slsbk, uint64_t oid, uint64_t epoch)
{
	return (0);
}

struct sls_backend_ops recvbk_ops = {
	.slsbk_setup = recvbk_setup,
	.slsbk_teardown = recvbk_teardown,
	.slsbk_import = recvbk_import,
	.slsbk_export = recvbk_export,
	.slsbk_partadd = recvbk_partadd,
	.slsbk_setepoch = recvbk_setepoch,
};
