#include <sys/types.h>
#include <sys/param.h>
#include <sys/bio.h>
#include <sys/buf.h>
#include <sys/caprights.h>
#include <sys/capsicum.h>
#include <sys/dirent.h>
#include <sys/filio.h>
#include <sys/kernel.h>
#include <sys/ktr.h>
#include <sys/mman.h>
#include <sys/module.h>
#include <sys/mount.h>
#include <sys/namei.h>
#include <sys/priv.h>
#include <sys/proc.h>
#include <sys/rwlock.h>
#include <sys/stat.h>
#include <sys/syscallsubr.h>
#include <sys/taskqueue.h>
#include <sys/ucred.h>
#include <sys/unistd.h>
#include <sys/vnode.h>

#include <vm/vm.h>
#include <vm/pmap.h>
#include <vm/vm_extern.h>
#include <vm/vm_map.h>
#include <vm/vm_object.h>
#include <vm/vm_page.h>
#include <vm/vnode_pager.h>

#include <machine/pmap.h>
#include <machine/vmparam.h>

#include <geom/geom_vfs.h>

#include "memsnap.h"

SDT_PROVIDER_DEFINE(sas);
SDT_PROBE_DEFINE4(sas, , , start, "long", "long", "long", "long");
SDT_PROBE_DEFINE0(sas, , , protect);
SDT_PROBE_DEFINE1(sas, , , write, "long");
SDT_PROBE_DEFINE0(sas, , , block);

uint64_t slsfs_sas_tracks;
uint64_t slsfs_sas_aborts;
uint64_t slsfs_sas_attempts;
uint64_t slsfs_sas_copies;

uint64_t slsfs_sas_commits;
long slsfs_sas_inserts, slsfs_sas_removes;

#define SLSVP(_vp) ((struct slos_node *)(_vp)->v_data)

static int
slsfs_sas_init(struct vnode *vp, size_t size)
{
	struct mount *mp = vp->v_mount;
	struct slos_node *svp = SLSVP(vp);
	uint64_t *sas_addr_allocator;
	struct slos_meta *sb;

	/* Get the address of the global allocator. */
	sb = (struct slos_meta *)mp->mnt_data;
	sas_addr_allocator = &sb->sb_sas_addr;

	if (svp->sn_obj != NULL)
		panic("double init for SAS object");

	svp->sn_addr = atomic_fetchadd_64(sas_addr_allocator, size + PAGE_SIZE);
	if (svp->sn_addr + size >= SLS_SAS_MAXADDR)
		panic("Reached the end of the SAS");

	svp->sn_obj = vm_object_allocate(OBJT_DEFAULT, atop(size));
	if (svp->sn_obj == NULL)
		panic("could not init SAS node");

	svp->sn_obj->flags |= OBJ_NOSPLIT;

	return 0;
}

static void
slsfs_sas_page_track(vm_offset_t vaddr, struct pglist *pglist, vm_page_t m)
{
	vm_page_lock(m);

	TAILQ_INSERT_HEAD(pglist, m, snapq);
	m->vaddr = vaddr;

	atomic_add_64(&slsfs_sas_inserts, 1);

	vm_page_unlock(m);
}

static void
slsfs_sas_page_untrack_unlocked(struct pglist *pglist, vm_page_t m)
{

	m->vaddr = 0;
	TAILQ_REMOVE(pglist, m, snapq);

	atomic_add_64(&slsfs_sas_removes, 1);
}

static void
slsfs_sas_page_untrack(struct pglist *pglist, vm_page_t m)
{
	vm_page_lock(m);
	slsfs_sas_page_untrack_unlocked(pglist, m);
	vm_page_unlock(m);
}

static void
slsfs_sas_refresh_protection(void)
{
	struct pmap *pmap = &curproc->p_vmspace->vm_pmap;
	pmap_protect(pmap, SLS_SAS_INITADDR, SLS_SAS_MAXADDR, VM_PROT_READ);
}

void
slsfs_sas_trace_update(vm_offset_t vaddr, vm_map_t map, vm_page_t m,
    int fault_type)
{
	if ((map->flags & MAP_SNAP_TRACE) == 0)
		return;

	atomic_add_64(&slsfs_sas_attempts, 1);

	if ((fault_type & (VM_PROT_WRITE | VM_PROT_COPY)) == 0)
		return;

	if (vaddr >= SLS_SAS_MAXADDR)
		return;
	if (vaddr < SLS_SAS_INITADDR)
		return;

	/* XXX Find out why we may fault a page twice. */
	if (m->vaddr != 0)
		return;

	atomic_add_64(&slsfs_sas_tracks, 1);
	slsfs_sas_page_track(vaddr, &curthread->td_snaplist, m);
}

struct slsfs_sas_commit_args {
	struct task tk;
	struct pglist pglist;
};

/* XXX Mark all pages we send out as done. */
static void
sas_pager_done(struct buf *bp)
{
	vm_page_t m;
	int i;

	for (i = 0; i < bp->b_npages; i++) {
		m = bp->b_pages[i];
		bp->b_pages[i] = NULL;

		m->flags &= ~VPO_SASCOW;
	}

	bp->b_bufsize = bp->b_bcount = 0;
	bp->b_npages = 0;
	bdone(bp);
}

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

	atomic_add_64(&slsfs_sas_copies, 1);
}

#define MAX_SAS (256)

static __attribute__((noinline)) void
slsfs_sas_trace_commit(void)
{
	struct pglist *snaplist = &curthread->td_snaplist;
	struct pmap *pmap = &curproc->p_vmspace->vm_pmap;
	size_t written = 0;
	vm_page_t m, mtmp;

	SDT_PROBE4(sas, , , start, slsfs_sas_tracks, slsfs_sas_removes,
	    slsfs_sas_attempts, slsfs_sas_copies);
	slsfs_sas_tracks = 0;
	slsfs_sas_removes = 0;
	slsfs_sas_attempts = 0;

	PMAP_LOCK(pmap);
	TAILQ_FOREACH_SAFE (m, snaplist, snapq, mtmp) {
		if (m->object == NULL) {
			slsfs_sas_page_untrack(snaplist, m);
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
		printf("Here we'll be committing to ObjSnap.\n");
	}

	/* XXX These are now extraneous, we write and commit in one go. */
	SDT_PROBE1(sas, , , write, written);
	SDT_PROBE0(sas, , , block);

	atomic_add_64(&slsfs_sas_commits, 1);
}

static int
slsfs_sas_trace_start(void)
{
	vm_map_t map = &curproc->p_vmspace->vm_map;

	vm_map_lock(map);
	/* This should grab any fork-&-exec looping cases */
	KASSERT((map->flags & MAP_SNAP_TRACE) == 0,
	    ("map already being traced"));

	map->flags |= MAP_SNAP_TRACE;
	vm_map_unlock(map);

	slsfs_sas_refresh_protection();

	return (0);
}

static void
slsfs_sas_trace_abort(void)
{
	struct pglist *snaplist = &curthread->td_snaplist;
	pmap_t pmap = &curproc->p_vmspace->vm_pmap;
	vm_page_t m, mtmp;

	TAILQ_FOREACH_SAFE (m, snaplist, snapq, mtmp) {
		slsfs_sas_page_untrack(snaplist, m);
		pmap_protect_page(pmap, m->vaddr, VM_PROT_READ);
	}

	PMAP_LOCK(pmap);
	pmap_invalidate_all(pmap);
	PMAP_UNLOCK(pmap);

	atomic_add_64(&slsfs_sas_aborts, 1);
}

static void
slsfs_sas_trace_end(void)
{
	vm_map_t map = &curproc->p_vmspace->vm_map;

	slsfs_sas_trace_abort();

	vm_map_lock(map);

	KASSERT((map->flags & MAP_SNAP_TRACE) != 0, ("map not being traced"));

	map->flags &= ~MAP_SNAP_TRACE;
	vm_map_unlock(map);
}

/*
 * Find the top-level object starting from its original SAS-backed
 * ancestor. VM objects normally lock from shadow to parent, starting
 * from the object directly accessible from the VM entry. We have to
 * start searching the other way, so we back off if we come across
 * a locked object in the chain to avoid a deadlock.
 */
static int
slsfs_sas_find_top_object(vm_object_t origobj, vm_object_t *objp)
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
slsfs_sas_mmap(struct thread *td, struct vnode *vp, vm_offset_t *addrp)
{
	vm_prot_t prot = VM_PROT_READ | VM_PROT_WRITE;
	struct slos_node *svp = SLSVP(vp);
	vm_offset_t addr = svp->sn_addr;
	struct proc *p = td->td_proc;
	vm_map_t map = &p->p_vmspace->vm_map;
	vm_object_t obj;
	int error;

	error = slsfs_sas_find_top_object(svp->sn_obj, &obj);
	while (error == EAGAIN) {
		pause_sbt("sasmap", SBT_1US * 100, 0, C_HARDCLOCK);
		error = slsfs_sas_find_top_object(svp->sn_obj, &obj);
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

static int
slsfs_sas_ioctl(struct vop_ioctl_args *ap)
{
	struct thread *td = curthread;
	struct vnode *vp = ap->a_vp;
	u_long com = ap->a_command;
	vm_offset_t addr;
	size_t size;
	int error;

	switch (com) {
	case SLSFS_SAS_MAP:
		addr = *(vm_offset_t *)ap->a_data;
		error = slsfs_sas_mmap(td, vp, &addr);
		if (error != 0)
			return (error);

		memcpy(ap->a_data, &addr, sizeof(void *));
		return (0);

	case SLSFS_SAS_TRACE_START:
		slsfs_sas_trace_start();
		return (0);

	case SLSFS_SAS_TRACE_END:
		slsfs_sas_trace_end();
		return (0);

	case SLSFS_SAS_TRACE_ABORT:
		slsfs_sas_trace_abort();
		return (0);

	case SLSFS_SAS_TRACE_COMMIT:
		slsfs_sas_trace_commit();
		return (0);

	case SLSFS_SAS_REFRESH_PROTECTION:
		slsfs_sas_refresh_protection();
		return (0);

	case SLSFS_SAS_INIT:
		size = *(vm_offset_t *)ap->a_data;
		slsfs_sas_init(vp, size);

	default:
		return (EINVAL);
	}

	return (0);
}

#define SLSFS_NAME_LEN (255)
static int
slsfs_create(struct vop_create_args *args)
{
	struct vnode *dvp = args->a_dvp;
	struct vnode **vpp = args->a_vpp;
	struct componentname *name = args->a_cnp;
	struct vnode *vp;

	if (name->cn_namelen > SLSFS_NAME_LEN)
		return (ENAMETOOLONG);

	panic("Get a new VP for MemSnap, store all the VPs. Need to implement vget() for it");

	*vpp = vp;
	if ((name->cn_flags & MAKEENTRY) != 0)
		cache_enter(dvp, *vpp, name);

	return (0);
}

static int
slsfs_reclaim(struct vop_reclaim_args *args)
{
	struct vnode *vp = args->a_vp;
	struct slos_node *svp = (struct slos_node *)vp->v_data;

	vp->v_data = NULL;

	vm_object_deallocate(svp->sn_obj);
	svp->sn_obj = NULL;
	svp->sn_addr = (vm_offset_t)0;

	free(svp, M_SLSFS);

	cache_purge(vp);
	vfs_hash_remove(vp);

	return (0);
}

static int
slsfs_lookup(struct vop_cachedlookup_args *args)
{
	struct vnode *dvp = args->a_dvp;
	struct vnode **vpp = args->a_vpp;
	struct componentname *cnp = args->a_cnp;
	int namelen, nameiop;
	char *name;

	name = cnp->cn_nameptr;
	namelen = cnp->cn_namelen;
	nameiop = cnp->cn_nameiop;

	if (cnp->cn_flags & ISDOTDOT)
		return (EINVAL);

	if (nameiop != CREATE && nameiop != LOOKUP)
		return (EINVAL);

	/* XXX Look up into our indexing structure. */

	/* XXX Then do a vget on the vnode. */
	panic("lookup: must look the name up in the main indexing structure");

	if ((cnp->cn_flags & MAKEENTRY) != 0)
		cache_enter(dvp, *vpp, cnp);
}

struct vop_vector slsfs_sas_vnodeops = {
	.vop_default = &default_vnodeops,
	.vop_fsync = VOP_PANIC,
	.vop_read = VOP_PANIC,
	.vop_reallocblks = VOP_PANIC,
	.vop_write = VOP_PANIC,
	.vop_bmap = VOP_EOPNOTSUPP,
	.vop_mkdir = VOP_PANIC,
	.vop_mknod = VOP_PANIC,
	.vop_poll = VOP_PANIC,
	.vop_readdir = VOP_PANIC,
	.vop_readlink = VOP_PANIC,
	.vop_remove = VOP_PANIC,
	.vop_rename = VOP_PANIC,
	.vop_rmdir = VOP_PANIC,
	.vop_open = VOP_NULL,
	.vop_close = VOP_NULL,
	.vop_lookup = vfs_cache_lookup,
	.vop_cachedlookup = slsfs_lookup,
	.vop_ioctl = slsfs_sas_ioctl,
	.vop_reclaim = slsfs_reclaim,
};
