#include <sys/param.h>
#include <sys/systm.h>
#include <sys/lock.h>
#include <sys/queue.h>
#include <sys/caprights.h>
#include <sys/capsicum.h>
#include <sys/ioccom.h>
#include <sys/kernel.h>
#include <sys/ktr.h>
#include <sys/malloc.h>
#include <sys/module.h>
#include <sys/mount.h>
#include <sys/proc.h>
#include <sys/rwlock.h>

#include <vm/vm.h>
#include <vm/vm_extern.h>
#include <vm/vm_map.h>
#include <vm/vm_object.h>
#include <vm/vm_page.h>

#include <machine/pmap.h>
#include <machine/vmparam.h>

#include <fs/pseudofs/pseudofs.h>

#include <memsnap_ioctl.h>
#include "memsnap.h"

SDT_PROVIDER_DEFINE(sas);
SDT_PROBE_DEFINE4(sas, , , start, "long", "long", "long", "long");
SDT_PROBE_DEFINE0(sas, , , protect);
SDT_PROBE_DEFINE1(sas, , , write, "long");
SDT_PROBE_DEFINE0(sas, , , block);

uint64_t msnp_tracks;
uint64_t msnp_aborts;
uint64_t msnp_attempts;
uint64_t msnp_copies;

uint64_t msnp_commits;
long msnp_inserts, msnp_removes;

MALLOC_DEFINE(M_MSNP, "msnp_mount", "msnp mount structures");

/*
 * Find the top-level object starting from its original SAS-backed
 * ancestor. VM objects normally lock from shadow to parent, starting
 * from the object directly accessible from the VM entry. We have to
 * start searching the other way, so we back off if we come across
 * a locked object in the chain to avoid a deadlock.
 */
static int
msnp_find_top_object(vm_object_t origobj, vm_object_t *objp)
{
	vm_object_t obj, shadow;
	bool locked;

	obj = origobj;
	VM_OBJECT_WLOCK(obj);
	while (obj->shadow_count > 0) {
		KASSERT(obj->shadow_count == 1, ("SAS object overly shadowed"));
		shadow = LIST_FIRST(&obj->shadow_head);

		locked = VM_OBJECT_TRYWLOCK(shadow);
		VM_OBJECT_WUNLOCK(obj);
		if (!locked)
			return (EAGAIN);

		obj = shadow;
	}
	vm_object_reference_locked(obj);
	VM_OBJECT_WUNLOCK(obj);

	*objp = obj;
	return (0);
}

static int
msnp_mmap(struct pfs_node *pn, struct thread *td, vm_offset_t *addrp)
{
	struct slos_node *svp = (struct slos_node *)pn->pn_data;
	vm_prot_t prot = VM_PROT_READ | VM_PROT_WRITE;
	vm_offset_t addr = svp->sn_addr;
	struct proc *p = td->td_proc;
	vm_map_t map = &p->p_vmspace->vm_map;
	vm_object_t obj;
	int error;

	error = msnp_find_top_object(svp->sn_obj, &obj);
	while (error == EAGAIN) {
		pause_sbt("sasmap", SBT_1US * 100, 0, C_HARDCLOCK);
		error = msnp_find_top_object(svp->sn_obj, &obj);
	}

	if (error != 0)
		return (error);

	vm_map_lock(map);
	/*
	 * XXX Using minherit() to make this mapping shadowable causes all hell
	 * to break loose. We control how we use the mappings so we ensure
	 * this does not happen from userspace. The correct solution requires
	 * adding a flag for SAS mappings that prevents minherit() calls.
	 */
	error = vm_map_insert(map, obj, 0, addr, addr + ptoa(obj->size), prot,
	    prot, MAP_NO_MERGE | MAP_INHERIT_SHARE);
	vm_map_unlock(map);
	if (error != 0) {
		vm_object_deallocate(obj);
		return (error);
	}

	*addrp = addr;
	return (0);
}

static void
msnp_page_track(vm_offset_t vaddr, struct pglist *pglist, vm_page_t m)
{
	vm_page_lock(m);

	TAILQ_INSERT_HEAD(pglist, m, snapq);
	m->vaddr = vaddr;

	atomic_add_64(&msnp_inserts, 1);

	vm_page_unlock(m);
}

static void
msnp_page_untrack_unlocked(struct pglist *pglist, vm_page_t m)
{

	m->vaddr = 0;
	TAILQ_REMOVE(pglist, m, snapq);

	atomic_add_64(&msnp_removes, 1);
}

static void
msnp_page_untrack(struct pglist *pglist, vm_page_t m)
{
	vm_page_lock(m);
	msnp_page_untrack_unlocked(pglist, m);
	vm_page_unlock(m);
}

static void
msnp_refresh_protection(void)
{
	struct pmap *pmap = &curproc->p_vmspace->vm_pmap;
	pmap_protect(pmap, SLS_SAS_INITADDR, SLS_SAS_MAXADDR, VM_PROT_READ);
}

void
msnp_trace_update(vm_offset_t vaddr, vm_map_t map, vm_page_t m,
    int fault_type)
{
	if ((map->flags & MAP_SNAP_TRACE) == 0)
		return;

	atomic_add_64(&msnp_attempts, 1);

	if ((fault_type & (VM_PROT_WRITE | VM_PROT_COPY)) == 0)
		return;

	if (vaddr >= SLS_SAS_MAXADDR)
		return;
	if (vaddr < SLS_SAS_INITADDR)
		return;

	/* XXX Find out why we may fault a page twice. */
	if (m->vaddr != 0)
		return;

	atomic_add_64(&msnp_tracks, 1);
	msnp_page_track(vaddr, &curthread->td_snaplist, m);
}

struct msnp_commit_args {
	struct task tk;
	struct pglist pglist;
};

void
sas_test_cow(vm_offset_t vaddr, vm_page_t *m)
{
	vm_page_t oldm, newm;
	vm_object_t obj;

	if (((*m)->flags & VPO_SASCOW) == 0)
		return;

	if (vaddr < SLS_SAS_INITADDR || vaddr < SLS_SAS_MAXADDR)
		return;

	oldm = *m;
	vm_page_lock(oldm);
	if (oldm->object == NULL) {
		vm_page_unlock(oldm);
		return;
	}
	obj = oldm->object;

	VM_OBJECT_WLOCK(obj);
	vm_page_remove(oldm);
	vm_page_unlock(oldm);
	vm_page_xunbusy(oldm);

	pmap_remove_all(oldm);

	newm = vm_page_alloc(obj, oldm->pindex, VM_ALLOC_WAITOK);
	pmap_copy_page(oldm, newm);
	newm->flags = VM_PAGE_BITS_ALL;
	vm_page_xbusy(newm);
	*m = newm;
	VM_OBJECT_WUNLOCK(obj);

	/* Check if the IO finished while we were applying COW. */
	vm_page_lock(oldm);
	if ((oldm->flags & VPO_SASCOW) == 0) {
		vm_page_unlock(oldm);
		vm_page_free(oldm);
	}

	oldm->flags &= ~VPO_SASCOW;
	vm_page_unlock(oldm);

	atomic_add_64(&msnp_copies, 1);
}

#define MAX_SAS (256)

static __attribute__((noinline)) void
msnp_trace_commit(void)
{
	struct pglist *snaplist = &curthread->td_snaplist;
	struct pmap *pmap = &curproc->p_vmspace->vm_pmap;
	size_t written = 0;
	vm_page_t m, mtmp;

	SDT_PROBE4(sas, , , start, msnp_tracks, msnp_removes,
	    msnp_attempts, msnp_copies);
	msnp_tracks = 0;
	msnp_removes = 0;
	msnp_attempts = 0;

	PMAP_LOCK(pmap);
	TAILQ_FOREACH_SAFE(m, snaplist, snapq, mtmp) {
		if (m->object == NULL) {
			msnp_page_untrack(snaplist, m);
			continue;
		}

		written += 1;
		pmap_protect_page(pmap, m->vaddr, VM_PROT_READ);
		m->flags |= VPO_SASCOW;
	}

	pmap_invalidate_all(pmap);
	PMAP_UNLOCK(pmap);

	SDT_PROBE0(sas, , , protect);

	while (!TAILQ_EMPTY(snaplist)) {
		panic("unimplemented commit operation");
	}

	/* 
	 * NOTE: The names of these probes are now inaccurate,
	 * but we keep them like that to be compatible with the
	 * original MemSnap scripts. It used to be that writing and
	 * waiting for the write was two steps, but ObjSnap transactions
	 * always block.
	 */
	SDT_PROBE1(sas, , , write, written);
	TAILQ_FOREACH_SAFE(m, snaplist, snapq, mtmp) {
		m->flags &= ~VPO_SASCOW;
	}
	SDT_PROBE0(sas, , , block);

	atomic_add_64(&msnp_commits, 1);
}

static int
msnp_trace_start(void)
{
	vm_map_t map = &curproc->p_vmspace->vm_map;

	vm_map_lock(map);
	/* This should grab any fork-&-exec looping cases */
	KASSERT((map->flags & MAP_SNAP_TRACE) == 0,
	    ("map already being traced"));

	map->flags |= MAP_SNAP_TRACE;
	vm_map_unlock(map);

	msnp_refresh_protection();

	return (0);
}

static void
msnp_trace_abort(void)
{
	struct pglist *snaplist = &curthread->td_snaplist;
	pmap_t pmap = &curproc->p_vmspace->vm_pmap;
	vm_page_t m, mtmp;

	TAILQ_FOREACH_SAFE (m, snaplist, snapq, mtmp) {
		msnp_page_untrack(snaplist, m);
		pmap_protect_page(pmap, m->vaddr, VM_PROT_READ);
	}

	PMAP_LOCK(pmap);
	pmap_invalidate_all(pmap);
	PMAP_UNLOCK(pmap);

	atomic_add_64(&msnp_aborts, 1);
}

static void
msnp_trace_end(void)
{
	vm_map_t map = &curproc->p_vmspace->vm_map;

	msnp_trace_abort();

	vm_map_lock(map);

	KASSERT((map->flags & MAP_SNAP_TRACE) != 0, ("map not being traced"));

	map->flags &= ~MAP_SNAP_TRACE;
	vm_map_unlock(map);
}

static int
msnp_node_ioctl(PFS_IOCTL_ARGS)
{
	vm_offset_t addr, *inaddrp;
	int error;

	switch (cmd) {
	case SLSFS_SAS_MAP:
		inaddrp = (vm_offset_t *)data;
		addr = *inaddrp;

		error = msnp_mmap(pn, td, &addr);
		if (error != 0)
			return (error);

		*inaddrp= addr;
		return (0);

	/* 
	 * NOTE: These calls being accessible from the object nodes are an
	 * implementation artifact. It would have been nicer to have a 
	 * control device for exposing these operations.
	 */
	case SLSFS_SAS_TRACE_START:
		msnp_trace_start();
		return (0);

	case SLSFS_SAS_TRACE_END:
		msnp_trace_end();
		return (0);

	case SLSFS_SAS_TRACE_ABORT:
		msnp_trace_abort();
		return (0);

	case SLSFS_SAS_TRACE_COMMIT:
		msnp_trace_commit();
		return (0);

	case SLSFS_SAS_REFRESH_PROTECTION:
		msnp_refresh_protection();
		return (0);

	default:
		return (EINVAL);
	}

	return (0);
}


static int
msnp_node_destroy(PFS_DESTROY_ARGS)
{
	struct slos_node *svp = (struct slos_node *)pn->pn_data;

	vm_object_deallocate(svp->sn_obj);
	svp->sn_obj = NULL;
	svp->sn_addr = (vm_offset_t)0;

	free(pn->pn_data, M_MSNP);
	pn->pn_data = NULL;

	return (0);
}

static int
msnp_create_objinit(struct pfs_node *ctrl, struct pfs_node *pn, size_t size)
{
	struct slos_meta *sb = (struct slos_meta *)ctrl->pn_data;
	struct slos_node *svp = (struct slos_node *)pn->pn_data;

	if (svp->sn_obj != NULL)
		panic("double init for SAS object");

	svp->sn_addr = atomic_fetchadd_64(&sb->sb_sas_addr, size + PAGE_SIZE);
	if (svp->sn_addr + size >= SLS_SAS_MAXADDR)
		panic("Reached the end of the SAS");

	svp->sn_obj = vm_object_allocate(OBJT_DEFAULT, atop(size));
	if (svp->sn_obj == NULL)
		panic("could not init SAS node");

	svp->sn_obj->flags |= OBJ_NOSPLIT;

	return 0;
}


static int
msnp_create(struct pfs_node *ctrl, char *path, size_t size) 
{
	struct pfs_node *root = ctrl->pn_parent;
	struct pfs_node *pn;

	pn = pfs_create_file(root, path, 
			/* pn_fill */ NULL,
			/* pn_attr */ NULL,
			/* pn_vis */ NULL,
			/* pn_destroy */ msnp_node_destroy,
			PFS_RDWR | PFS_RAW);

	pn->pn_ioctl = msnp_node_ioctl;
	pn->pn_data = malloc(sizeof(struct slos_node), M_MSNP, M_WAITOK | M_ZERO);

	return (msnp_create_objinit(ctrl, pn, size));
}


static int
msnp_ctrl_ioctl(PFS_IOCTL_ARGS)
{
	struct slsfs_sas_create_args *sas_create_args;

	switch (cmd) {
	case SLSFS_SAS_CREATE:
		sas_create_args = (struct slsfs_sas_create_args *)data;
		return (msnp_create(pn, (char *)&sas_create_args->path, sas_create_args->size));

	default:
		return (EINVAL);
	}

	return (0);
}

static int
msnp_ctrl_destroy(PFS_DESTROY_ARGS)
{
	struct slos_meta *sb = (struct slos_meta *)pn->pn_data;

	mtx_destroy(&sb->sb_mtx);
	free(sb, M_MSNP);

	return (0);
}

static int
msnp_init(PFS_INIT_ARGS)
{
	struct pfs_node *root = pi->pi_root;
	struct pfs_node *ctrl;
	struct slos_meta *sb;
	
	sb = malloc(sizeof(*sb), M_MSNP, M_WAITOK | M_ZERO);
	mtx_init(&sb->sb_mtx, "sbmtx", NULL, MTX_DEF);
	sb->sb_sas_addr = SLS_SAS_INITADDR;

	ctrl = pfs_create_file(root, MSNP_CTRLDEV, 
			/* pn_fill */ NULL,
			/* pn_attr */ NULL,
			/* pn_vis */ NULL,
			/* pn_destroy */ msnp_ctrl_destroy,
			PFS_RDWR | PFS_RAW);
	ctrl->pn_ioctl = msnp_ctrl_ioctl;
	ctrl->pn_data = sb;

	sls_writefault_hook = msnp_trace_update;
	sas_cow_hook = sas_test_cow;

	return (0);
}

static int
msnp_uninit(struct pfs_info *pi, struct vfsconf *vfc)
{
	sls_writefault_hook = NULL;
	sas_cow_hook = NULL;

	return (0);
}

PSEUDOFS(msnp, 1, VFCF_JAIL);
//MODULE_DEPEND(msnp, objsnap, 0, 0, 0);
