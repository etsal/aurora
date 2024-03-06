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
#include <slos.h>
#include <slos_inode.h>
#include <slos_io.h>
#include <slsfs.h>

#include "btree.h"
#include "debug.h"
#include "slos_alloc.h"
#include "slos_io.h"
#include "slos_subr.h"
#include "slsfs_buf.h"
#include "slsfs_dir.h"

SDT_PROVIDER_DEFINE(slos);
SDT_PROBE_DEFINE3(slos, , , slsfs_deviceblk, "uint64_t", "uint64_t", "int");
SDT_PROBE_DEFINE3(slos, , , slsfs_vnodeblk, "uint64_t", "uint64_t", "int");

SDT_PROVIDER_DEFINE(sas);
SDT_PROBE_DEFINE4(sas, , , start, "long", "long", "long", "long");
SDT_PROBE_DEFINE0(sas, , , protect);
SDT_PROBE_DEFINE1(sas, , , write, "long");
SDT_PROBE_DEFINE0(sas, , , block);

/* 5 GiB */
static const size_t MAX_WAL_SIZE = 5368709000;
static size_t wal_space_used = 0;

uint64_t slsfs_sas_commit_async;
uint64_t slsfs_sas_tracks;
uint64_t slsfs_sas_aborts;
uint64_t slsfs_sas_attempts;
uint64_t slsfs_sas_copies;

uint64_t slsfs_sas_commits;
long slsfs_sas_inserts, slsfs_sas_removes;

static int
slsfs_inactive(struct vop_inactive_args *args)
{
	return (0);
}

static int
slsfs_getattr(struct vop_getattr_args *args)
{
	struct vnode *vp = args->a_vp;
	struct vattr *vap = args->a_vap;
	struct slos_node *slsvp = SLSVP(vp);

#ifdef VERBOSE
	DEBUG1("VOP_GETATTR on vnode %lu", slsvp->sn_pid);
#endif

	VATTR_NULL(vap);
	vap->va_type = IFTOVT(slsvp->sn_ino.ino_mode);
	vap->va_mode = slsvp->sn_ino.ino_mode & ~S_IFMT;
	vap->va_nlink = slsvp->sn_ino.ino_nlink;
	vap->va_uid = slsvp->sn_ino.ino_uid;
	vap->va_gid = slsvp->sn_ino.ino_gid;
	vap->va_fsid = VNOVAL;
	vap->va_fileid = slsvp->sn_pid;
	vap->va_blocksize = BLKSIZE(&slos);
	vap->va_size = slsvp->sn_ino.ino_size;
	vap->va_mode = slsvp->sn_ino.ino_mode & ~S_IFMT;

	vap->va_atime.tv_sec = slsvp->sn_ino.ino_atime;
	vap->va_atime.tv_nsec = slsvp->sn_ino.ino_atime_nsec;

	vap->va_mtime.tv_sec = slsvp->sn_ino.ino_mtime;
	vap->va_mtime.tv_nsec = slsvp->sn_ino.ino_mtime_nsec;
	vap->va_nlink = slsvp->sn_ino.ino_nlink;

	vap->va_ctime.tv_sec = slsvp->sn_ino.ino_ctime;
	vap->va_ctime.tv_nsec = slsvp->sn_ino.ino_ctime_nsec;

	vap->va_birthtime.tv_sec = slsvp->sn_ino.ino_birthtime;
	vap->va_birthtime.tv_nsec = slsvp->sn_ino.ino_birthtime_nsec;
	vap->va_gen = 0;
	vap->va_flags = slsvp->sn_ino.ino_flags;
	vap->va_rdev = NODEV;
	vap->va_bytes = slsvp->sn_ino.ino_asize;
	vap->va_filerev = 0;
	vap->va_vaflags = 0;

	return (0);
}

static int
slsfs_reclaim(struct vop_reclaim_args *args)
{
	struct vnode *vp = args->a_vp;
	struct slos_node *svp = SLSVP(vp);
	struct fbtree *tree = &svp->sn_tree;
	if (vp == slos.slsfs_inodes) {
		DEBUG("Special vnode trying to be reclaimed");
	}
	/* Keeping this here for now
	 * TODO: Need to figure out why some vnodes have 1 dirty btree node when
	 * reclaimed
	 */
	if (svp->sn_status == SLOS_DIRTY || FBTREE_DIRTYCNT(tree)) {
		DEBUG2("RECLAIMING DIRTY NODE %d %d\n",
		    vp->v_bufobj.bo_dirty.bv_cnt, FBTREE_DIRTYCNT(tree));
	}

	vp->v_data = NULL;

	vinvalbuf(vp, 0, 0, 0);

	/*
	 * TODO:
	 * While rerunning seqwrite-4t-64k twice vfs_hash_remove blew up
	 */
	if (!SLS_ISSAS(vp)) {
		vnode_destroy_vobject(vp);
	} else if (svp->sn_obj != NULL) {
		vm_object_deallocate(svp->sn_obj);
		svp->sn_obj = NULL;
		svp->sn_addr = (vm_offset_t)NULL;
	}

	if (vp->v_type != VCHR) {
		cache_purge(vp);
		if (vp != slos.slsfs_inodes)
			vfs_hash_remove(vp);
		slos_vpfree(svp->sn_slos, svp);
	}

	return (0);
}

static int
slsfs_mkdir(struct vop_mkdir_args *args)
{
	struct vnode *dvp = args->a_dvp;
	struct vnode **vpp = args->a_vpp;
	struct componentname *name = args->a_cnp;
	struct vattr *vap = args->a_vap;

	struct vnode *vp;
	int error;

	if (name->cn_namelen > SLSFS_NAME_LEN) {
		return (ENAMETOOLONG);
	}
	mode_t mode = MAKEIMODE(vap->va_type, vap->va_mode);
	error = SLS_VALLOC(dvp, mode, name->cn_cred, &vp);
	if (error) {
		*vpp = NULL;
		return (error);
	}

	SLSVP(vp)->sn_ino.ino_gid = SLSVP(dvp)->sn_ino.ino_gid;
	SLSVP(vp)->sn_ino.ino_uid = name->cn_cred->cr_uid;

	error = slsfs_init_dir(dvp, vp, name);
	if (error) {
		DEBUG("Issue init directory");
		*vpp = NULL;
		return (error);
	}

	SLSVP(dvp)->sn_ino.ino_nlink++;
	SLSVP(dvp)->sn_ino.ino_flags |= IN_CHANGE;
	SLSVP(dvp)->sn_status |= SLOS_DIRTY;

	MPASS(SLSVP(dvp)->sn_ino.ino_nlink >= 3);
	MPASS(SLSVP(vp)->sn_ino.ino_nlink == 2);

	*vpp = vp;

	return (0);
}

static int
slsfs_access(struct vop_access_args *args)
{
	struct vnode *vp = args->a_vp;
	accmode_t accmode = args->a_accmode;
	struct ucred *cred = args->a_cred;
	struct vattr vap;
	int error;

	/* VCHR vnodes are inaccessible vnodes backing btrees. */
	if (vp->v_type == VCHR)
		return (EOPNOTSUPP);

	error = VOP_GETATTR(vp, &vap, cred);
	if (error) {
		return (error);
	}

	error = vaccess(vp->v_type, vap.va_mode, vap.va_uid, vap.va_gid,
	    accmode, cred, NULL);

	return (error);
}

static int
slsfs_open(struct vop_open_args *args)
{
	struct vnode *vp = args->a_vp;
	struct slos_node *slsvp = SLSVP(vp);
	vnode_create_vobject(vp, SLSVPSIZ(slsvp), args->a_td);

	if (SLS_ISWAL(vp)) {
		slsfs_mark_wal(vp);
	}

	return (0);
}

static int
slsfs_readdir(struct vop_readdir_args *args)
{
	struct buf *bp;
	struct dirent dir;
	size_t blkno;
	off_t blkoff;
	size_t diroffset, anyleft;
	int error = 0;

	struct vnode *vp = args->a_vp;
	struct slos_node *slsvp = SLSVP(vp);
	struct uio *io = args->a_uio;
	size_t filesize = SLSINO(slsvp).ino_size;
	size_t blksize = IOSIZE(slsvp);

	KASSERT(slsvp->sn_slos != NULL, ("Null slos"));
	if (vp->v_type != VDIR) {
		return (ENOTDIR);
	}

	if ((io->uio_offset < filesize) &&
	    (io->uio_resid >= sizeof(struct dirent))) {
		diroffset = io->uio_offset;
		blkno = io->uio_offset / blksize;
		blkoff = io->uio_offset % blksize;
		error = slsfs_bread(
		    vp, blkno, blksize, curthread->td_ucred, 0, &bp);
		if (error) {
			brelse(bp);
			DEBUG("Problem reading from blk in readdir");
			return (error);
		}
		/* Create the UIO for the disk. */
		while (diroffset < filesize) {
			anyleft = ((diroffset % blksize) +
				      sizeof(struct dirent)) > blksize;
			if (anyleft) {
				blkoff = 0;
				blkno++;
				diroffset = blkno * blksize;
				brelse(bp);
				error = slsfs_bread(vp, blkno, blksize,
				    curthread->td_ucred, 0, &bp);
				if (error) {
					brelse(bp);
					return (error);
				}
			}
			if (buf_mapped(bp)) {
				KASSERT(bp->b_bcount > blkoff,
				    ("Blkoff out of range of buffer"));
				dir = *((struct dirent *)(bp->b_data + blkoff));
				if (dir.d_reclen == 0) {
					break;
				}
				dir.d_reclen = GENERIC_DIRSIZ(&dir);
				dirent_terminate(&dir);
				if (io->uio_resid < GENERIC_DIRSIZ(&dir)) {
					break;
				}

				error = uiomove(&dir, dir.d_reclen, io);
				if (error) {
					DEBUG("Problem moving buffer");
					return (error);
				}
			} else {
				brelse(bp);
				return (EIO);
			}
			diroffset += sizeof(struct dirent);
			blkoff += sizeof(struct dirent);
		}
		brelse(bp);
		io->uio_offset = diroffset;
	}

	if (args->a_eofflag != NULL) {
		*args->a_eofflag = 0;
	}

	return (error);
}

static int
slsfs_close(struct vop_close_args *args)
{
	return (0);
}

static int
slsfs_lookup(struct vop_cachedlookup_args *args)
{
	struct vnode *dvp = args->a_dvp;
	struct vnode **vpp = args->a_vpp;
	struct componentname *cnp = args->a_cnp;
	struct dirent dir;
	int namelen, nameiop, islastcn;
	char *name;
	int error = 0;
	struct vnode *vp = NULL;
	*vpp = NULL;

	name = cnp->cn_nameptr;
	namelen = cnp->cn_namelen;
	nameiop = cnp->cn_nameiop;
	islastcn = cnp->cn_flags & ISLASTCN;

	/* Self directory - Must just increase reference count of dir */
	DEBUG1("SLSFS Lookup called %x", cnp->cn_flags);
	if ((namelen == 1) && (name[0] == '.')) {
		*vpp = dvp;
		VREF(dvp);
		/* Check another case of the ".." directory */
	} else if (cnp->cn_flags & ISDOTDOT) {
		struct componentname tmp;
		tmp.cn_nameptr = "..";
		tmp.cn_namelen = 2;
		error = slsfs_lookup_name(dvp, &tmp, &dir);
		/* Record was not found */
		if (error)
			goto out;

		error = SLS_VGET(dvp, dir.d_fileno, LK_EXCLUSIVE, &vp);
		if (!error) {
			*vpp = vp;
		}
	} else {
		error = slsfs_lookup_name(dvp, cnp, &dir);
		if (error == EINVAL) {
			error = ENOENT;
			/*
			 * Are we creating or renaming the directory
			 */
			if ((nameiop == CREATE || nameiop == RENAME) &&
			    islastcn) {
				/* Normally should check access rights but
				 * won't for now */
				DEBUG("Regular name lookup - not found");
				cnp->cn_flags |= SAVENAME;
				error = EJUSTRETURN;
			}
		} else if (error == 0) {
			/* Cases for when name is found, others to be filled in
			 * later */
			if ((nameiop == DELETE) && islastcn) {
				DEBUG("Delete of file");
				error = SLS_VGET(
				    dvp, dir.d_fileno, LK_EXCLUSIVE, &vp);
				if (!error) {
					cnp->cn_flags |= SAVENAME;
					*vpp = vp;
				}
			} else {
				DEBUG1("Lookup of file dvp_usecount(%lu)",
				    dvp->v_usecount);
				error = SLS_VGET(
				    dvp, dir.d_fileno, LK_EXCLUSIVE, &vp);
				if (!error) {
					*vpp = vp;
				}
			}
		} else {
			DEBUG1("ERROR IN LOOKUP %d", error);
			return (error);
		}
	}

out:
	// Cache the entry in the name cache for the future
	if ((cnp->cn_flags & MAKEENTRY) != 0) {
		cache_enter(dvp, *vpp, cnp);
	}
	return (error);
}

static int
slsfs_rmdir(struct vop_rmdir_args *args)
{
	DEBUG("Removing directory");
	struct vnode *vp = args->a_vp;
	struct vnode *dvp = args->a_dvp;
	struct componentname *cnp = args->a_cnp;
	int error;

	struct slos_node *svp = SLSVP(vp);

	if (svp->sn_ino.ino_nlink < 2) {
		return (EINVAL);
	}

	if (!slsfs_dirempty(vp)) {
		return (ENOTEMPTY);
	}

	if ((svp->sn_ino.ino_flags & (IMMUTABLE | APPEND | NOUNLINK)) ||
	    (SLSVP(dvp)->sn_ino.ino_flags & APPEND)) {
		return (EPERM);
	}

	if (vp->v_mountedhere != NULL) {
		return (EPERM);
	}

	/* Assert self and parent reference */
	MPASS(svp->sn_ino.ino_nlink == 2);

	error = slos_remove_node(dvp, vp, cnp);
	if (error) {
		return (error);
	}

	MPASS(SLSVP(dvp)->sn_ino.ino_nlink >= 3);
	slsfs_declink(dvp);
	SLSVP(dvp)->sn_status |= SLOS_DIRTY;
	// XXX This is weird , this is something FFS does, it purges the cache
	// of the parent directory which seems funky
	cache_purge(dvp);
	// Purge name entries that point to vp
	cache_purge(vp);

	error = slos_truncate(vp, 0);
	if (error) {
		return (error);
	}

	svp->sn_ino.ino_nlink -= 2;
	svp->sn_ino.ino_flags |= IN_CHANGE;
	svp->sn_status |= SLOS_DIRTY | IN_DEAD;
	KASSERT(svp->sn_ino.ino_nlink == 0,
	    ("Problem with ino links - %lu", svp->sn_ino.ino_nlink));
	DEBUG("Removing directory done");

	return (0);
}

static int
slsfs_create(struct vop_create_args *args)
{
	struct vnode *dvp = args->a_dvp;
	struct vnode **vpp = args->a_vpp;
	struct componentname *name = args->a_cnp;
	struct vattr *vap = args->a_vap;

	struct vnode *vp;
	int error;

	if (name->cn_namelen > SLSFS_NAME_LEN) {
		return (ENAMETOOLONG);
	}
	mode_t mode = MAKEIMODE(vap->va_type, vap->va_mode);
	DEBUG1("Creating file %u", mode);
	error = SLS_VALLOC(dvp, mode, name->cn_cred, &vp);
	if (error) {
		*vpp = NULL;
		return (error);
	}

	error = slsfs_add_dirent(
	    dvp, VINUM(vp), name->cn_nameptr, name->cn_namelen, IFTODT(mode));
	if (error == -1) {
		return (EIO);
	}

	*vpp = vp;
	if ((name->cn_flags & MAKEENTRY) != 0) {
		cache_enter(dvp, *vpp, name);
	}

	return (0);
}

static int
slsfs_remove(struct vop_remove_args *args)
{
	struct vnode *vp = args->a_vp;
	struct vnode *dvp = args->a_dvp;
	struct componentname *cnp = args->a_cnp;
	int error;

	if (vp->v_type == VDIR) {

		return (EISDIR);
	}

	if ((SLSVP(vp)->sn_ino.ino_flags & (IMMUTABLE | APPEND | NOUNLINK)) ||
	    (SLSVP(dvp)->sn_ino.ino_flags & APPEND)) {

		return (EPERM);
	}

	DEBUG2(
	    "Removing file %s %lu", cnp->cn_nameptr, SLSVP(vp)->sn_ino.ino_pid);
	error = slos_remove_node(dvp, vp, cnp);
	if (error) {
		return (error);
	}

	slsfs_declink(vp);
	SLSVP(dvp)->sn_status |= SLOS_DIRTY;
	SLSVP(vp)->sn_status |= SLOS_DIRTY;

	return (0);
}

static int
slsfs_write(struct vop_write_args *args)
{
	struct buf *bp;
	int xfersize;
	size_t filesize;
	uint64_t off;
	int error = 0;
	int gbflag = 0;

	struct vnode *vp = args->a_vp;
	struct slos_node *svp = SLSVP(vp);
	size_t blksize = IOSIZE(svp);
	struct uio *uio = args->a_uio;
	int ioflag = args->a_ioflag;

	filesize = svp->sn_ino.ino_size;

	// Check if full
	if (uio->uio_offset < 0) {
		DEBUG1("Offset write at %lx", uio->uio_offset);
		return (EINVAL);
	}
	if (uio->uio_resid == 0) {
		return (0);
	}

	switch (vp->v_type) {
	case VREG:
		break;
	case VDIR:
		return (EISDIR);
	case VLNK:
		break;
	default:
		panic("bad file type %d", vp->v_type);
	}

	if (ioflag & IO_APPEND) {
		uio->uio_offset = filesize;
	}

	if (uio->uio_offset + uio->uio_resid > filesize) {
		svp->sn_ino.ino_size = uio->uio_offset + uio->uio_resid;
		vnode_pager_setsize(vp, svp->sn_ino.ino_size);
	}

	int modified = 0;

	while (uio->uio_resid) {
		// Grab the key thats closest to offset, but not over it
		// Mask out the lower order bits so we just have the block;
		if (!checksum_enabled) {
			gbflag |= GB_UNMAPPED;
		}

		error = slsfs_retrieve_buf(vp, uio->uio_offset, uio->uio_resid,
		    uio->uio_rw, gbflag, &bp);
		if (error) {
			DEBUG1("Problem getting buffer for write %d", error);
			return (error);
		}

		off = uio->uio_offset - (bp->b_lblkno * blksize);
		KASSERT(
		    off < bp->b_bcount, ("Offset should inside buf, %p", bp));
		xfersize = omin(uio->uio_resid, bp->b_bcount - off);

		KASSERT(xfersize != 0, ("No 0 uio moves slsfs write"));
		KASSERT(xfersize <= uio->uio_resid, ("This should neveroccur"));
		if (buf_mapped(bp)) {
			error = vn_io_fault_uiomove(
			    (char *)bp->b_data + off, xfersize, uio);
		} else {
			error = vn_io_fault_pgmove(
			    bp->b_pages, off, xfersize, uio);
		}

		vfs_bio_set_flags(bp, ioflag);
		/* One thing thats weird right now is our inodes and meta data
		 * is currently not
		 * in the buf cache, so we don't really have to worry about
		 * dirtying those buffers,
		 * but later we will have to dirty them.
		 */
		slsfs_bdirty(bp);
		modified++;
		if (error || xfersize == 0)
			break;
	}

	if (modified) {
		svp->sn_status |= SLOS_DIRTY;
	}

	return (error);
}

static int
slsfs_read(struct vop_read_args *args)
{
	struct slos_inode *sivp;
	struct buf *bp;
	size_t filesize;
	uint64_t off;
	size_t resid;
	size_t toread;
	int gbflag = 0;
	int error = 0;

	struct vnode *vp = args->a_vp;
	struct slos_node *svp = SLSVP(vp);
	size_t blksize = IOSIZE(svp);
	struct uio *uio = args->a_uio;

	svp = SLSVP(vp);
	sivp = &SLSINO(svp);
	filesize = sivp->ino_size;

	// Check if full
	if (uio->uio_offset < 0)
		return (EINVAL);
	if (uio->uio_resid == 0)
		return (0);

	if (uio->uio_offset >= filesize) {
		return (0);
	}

	resid = omin(uio->uio_resid, (filesize - uio->uio_offset));
#ifdef VERBOSE
	DEBUG3("Reading filesize %lu - %lu, %lu", SLSVP(vp)->sn_pid, filesize,
	    uio->uio_offset);
#endif
	while (resid > 0) {
		if (!checksum_enabled) {
			gbflag |= GB_UNMAPPED;
		}

		error = slsfs_retrieve_buf(vp, uio->uio_offset, resid,
		    uio->uio_rw, gbflag, &bp);
		if (error) {
			DEBUG1("Problem getting buffer for write %d", error);
			return (error);
		}

		off = uio->uio_offset - (bp->b_lblkno * blksize);
		toread = omin(resid, bp->b_bcount - off);

		/* One thing thats weird right now is our inodes and meta data
		 * is currently not
		 * in the buf cache, so we don't really have to worry about
		 * dirtying those buffers,
		 * but later we will have to dirty them.
		 */
		KASSERT(toread != 0, ("Should not occur"));
		if (buf_mapped(bp)) {
			error = vn_io_fault_uiomove(
			    (char *)bp->b_data + off, toread, uio);
		} else {
			error = vn_io_fault_pgmove(
			    bp->b_pages, off, toread, uio);
		}
		brelse(bp);
		resid -= toread;
		if (error || toread == 0)
			break;
	}

	return (error);
}

static int
slsfs_wal_bmap(struct vop_bmap_args *ap)
{
	struct vnode *vp = ap->a_vp;
	struct slos_node *svp = SLSVP(vp);
	struct slos *slos = VPSLOS(vp);
	struct slos_inode *ino = &SLSINO(svp);
	size_t off = ino->ino_wal_segment.offset;
	size_t size_b = ino->ino_wal_segment.size / BLKSIZE(slos);
	uint64_t pbno = off + ap->a_bn;

	if (ap->a_bop != NULL)
		*ap->a_bop = &ap->a_vp->v_bufobj;
	if (ap->a_bnp != NULL)
		*ap->a_bnp = pbno;
	if (ap->a_runp != NULL) {
		// Calculate how much left
		*ap->a_runp = (size_b - (pbno - off));
	}

	if (ap->a_runb != NULL) {
		*ap->a_runb = (pbno - off);
	}

	return (0);
}

static int
slsfs_bmap(struct vop_bmap_args *args)
{
	struct vnode *vp = args->a_vp;
	daddr_t lbn = args->a_bn;
	daddr_t *bnp = args->a_bnp;

	struct slsfsmount *smp = TOSMP(vp->v_mount);
	struct slos_node *svp = SLSVP(vp);
	size_t fsbsize, devbsize;
	size_t scaling;

	struct fnode_iter biter;
	uint64_t extsize;
	daddr_t extlbn;
	daddr_t extbn;
	diskptr_t ptr;

	int error;

	/* Constants so that we scale by the FS to device block size ratio. */
	fsbsize = vp->v_bufobj.bo_bsize;
	devbsize = slos.slos_vp->v_bufobj.bo_bsize;

	KASSERT(fsbsize >= devbsize, ("Sector size larger than block size"));

	/* Scaling factor of sectors per FS block. */
	scaling = fsbsize / devbsize;

	if (args->a_bop != NULL)
		*args->a_bop = &smp->sp_slos->slos_vp->v_bufobj;

	/* No readbehind/readahead for now.
	 * Be careful in the future when setting these values, the pager will
	 * look at these values and panic if the readahead number is past the
	 * EOF offset.
	 */
	if (args->a_runp != NULL)
		*args->a_runp = 0;
	if (args->a_runb != NULL)
		*args->a_runb = 0;

	/* If no resolution is necessary we're done. */
	if (bnp == NULL)
		return (0);

	/*
	 * We use VCHR to denote vnodes holding btree
	 * buffers. The mapping for these is simple.
	 */
	if (vp->v_type == VCHR) {
		*bnp = lbn * scaling;
		return (0);
	}

	/* Look up the physical block number. */
	error = slsfs_lookupbln(svp, lbn, &biter);
	if (error != 0)
		return (error);

	if (ITER_ISNULL(biter)) {
		ITER_RELEASE(biter);
		*bnp = -1;
		return (0);
	}

	/* Extract the extent's dimensions. */
	ptr = ITER_VAL_T(biter, diskptr_t);

	/* Turn everything into sector size blocks.*/
	extlbn = ITER_KEY_T(biter, uint64_t);
	extbn = ptr.offset;
	extsize = ptr.size / fsbsize;
	ITER_RELEASE(biter);

	/* Check if we're in a hole. */
	if (extlbn + extsize <= lbn) {
		*bnp = -1;
		return (0);
	}

	*bnp = (extbn + (lbn - extlbn)) * scaling;

	return (0);
}

static int
slsfs_fsync(struct vop_fsync_args *args)
{
	return (0);
}

static int
slsfs_print(struct vop_print_args *args)
{
	struct vnode *vp = args->a_vp;
	struct slos_node *slsvp = SLSVP(vp);

	if (slsvp == NULL) {
		printf("\t(null)");
	} else if ((void *)slsvp == (void *)&slos) {
		printf("\tslos");
	} else {
		printf("\tslos inode");
		printf("\tsn_pid = %ld", slsvp->sn_pid);
		printf("\tsn_uid = %ld", slsvp->sn_uid);
		printf("\tsn_gid = %ld", slsvp->sn_gid);
		printf("\tsn_blk = %ld", slsvp->sn_blk);
		printf("\tsn_status = %lx", slsvp->sn_status);
	}

	return (0);
}

static int
slsfs_check_cksum(struct buf *bp)
{
	size_t cksize;
	uint32_t cksum, check;
	int error;
	struct fbtree *tree = &slos.slos_cktree->sn_tree;
	uint64_t blk = bp->b_blkno;
	size_t size = 0;

	MPASS((bp->b_bcount % BLKSIZE(&slos)) == 0);

	while (size < bp->b_bcount) {
		cksize = min(PAGE_SIZE, bp->b_bcount - size);
		cksum = calculate_crc32c(~0, bp->b_data + size, cksize);
		size += cksize;
		blk++;
		error = fbtree_get(tree, &blk, &check);
		if (error == EINVAL) {
			return 0;
		} else if (error) {
			panic("Problem with read cksum %d", error);
		}

		if (check != cksum) {
			printf("%lu, %lu, %lu", blk, cksize, bp->b_bcount);
			return EINVAL;
		}
	}
	return (0);
}

static int
slsfs_update_cksum(struct buf *bp)
{
	size_t cksize;
	uint32_t cksum;
	struct fnode_iter iter;
	int error = 0;

	struct fbtree *tree = &slos.slos_cktree->sn_tree;
	uint64_t blk = bp->b_blkno;
	size_t size = 0;
	while (size < bp->b_bcount) {
		cksize = min(PAGE_SIZE, bp->b_bcount - size);
		cksum = calculate_crc32c(~0, bp->b_data + size, cksize);
		size += cksize;
		error = fbtree_keymin_iter(tree, &blk, &iter);
		KASSERT(error == 0, ("error %d by fbtree_keymin_iter", error));
		if (ITER_ISNULL(iter) || ITER_KEY_T(iter, uint64_t) != blk) {
			error = fnode_insert(iter.it_node, &blk, &cksum);
		} else {
			fiter_replace(&iter, &cksum);
		}
		if (error) {
			panic("Issue with updating checksum tree %d", error);
		}
		blk++;
	}
	return (0);
}

int
slsfs_cksum(struct buf *bp)
{
	int error;
	struct fbtree *tree = &slos.slos_cktree->sn_tree;

	if (bp->b_data == unmapped_buf ||
	    (bp->b_vp == slos.slos_cktree->sn_fdev) ||
	    slos.slos_sb->sb_epoch == EPOCH_INVAL) {
		return 0;
	}

	switch (bp->b_iocmd) {
	case BIO_READ:
		BTREE_LOCK(tree, LK_SHARED);
		error = slsfs_check_cksum(bp);
		BTREE_UNLOCK(tree, 0);

		return (error);
	case BIO_WRITE:
		BTREE_LOCK(tree, LK_EXCLUSIVE);
		error = slsfs_update_cksum(bp);
		BTREE_UNLOCK(tree, 0);

		return (error);
	default:
		panic(
		    "Unknown buffer IO command %d for bp %p", bp->b_iocmd, bp);
	};

	return (-1);
}

static int
slsfs_strategy(struct vop_strategy_args *args)
{
	int error;
	struct slos_diskptr ptr;
	struct buf *bp = args->a_bp;
	struct vnode *vp = args->a_vp;
	struct fnode_iter iter;
	size_t fsbsize, devbsize;

#ifdef VERBOSE
	DEBUG2("vp=%p blkno=%x", vp, bp->b_lblkno);
#endif
	/* The FS and device block sizes are needed below. */
	fsbsize = bp->b_bufobj->bo_bsize;
	devbsize = slos.slos_vp->v_bufobj.bo_bsize;
	KASSERT(fsbsize >= devbsize,
	    ("FS bsize %lu > device bsize %lu", fsbsize, devbsize));
	KASSERT((fsbsize % devbsize) == 0,
	    ("FS bsize %lu not multiple of device bsize %lu", fsbsize,
		devbsize));

	if (vp->v_type != VCHR) {
		/* We are a regular vnode, so there are mappings. */
		KASSERT(bp->b_lblkno != EPOCH_INVAL,
		    ("No logical block number should be -1 - vnode effect %lu",
			SLSVP(vp)->sn_pid));

		/* Get the physical segment for the buffer. */
		error = BTREE_LOCK(&SLSVP(vp)->sn_tree, LK_SHARED);
		if (error != 0) {
			bp->b_error = error;
			bp->b_ioflags |= BIO_ERROR;
			bufdone(bp);

			return (0);
		}

		error = fbtree_keymin_iter(
		    &SLSVP(vp)->sn_tree, &bp->b_lblkno, &iter);
		if (error != 0)
			goto error;

		/*
		 * Out of bounds.
		 * XXX Why is this a problem? What if there is a hole?
		 */
		if (ITER_ISNULL(iter)) {
			fnode_print(iter.it_node);
			DEBUG4(
			    "Issue finding block vp(%p), lbn(%lu), fnode(%p) bp(%p)",
			    vp, bp->b_lblkno, iter.it_node, bp);
			goto error;
		}

		/* Make sure the segment includes the buffer's start. */
		ptr = ITER_VAL_T(iter, diskptr_t);
		if (ITER_KEY_T(iter, uint64_t) != bp->b_lblkno) {
			if (!INTERSECT(iter, bp->b_lblkno, IOSIZE(SLSVP(vp)))) {
				fnode_print(iter.it_node);
				DEBUG4(
				    "Key %lu not found, got (%lu %lu) on block %lu instead",
				    ITER_KEY_T(iter, uint64_t), ptr.offset,
				    ptr.size, bp->b_lblkno);
				goto error;
			}
		}

		if (bp->b_iocmd == BIO_WRITE) {
			if (ptr.epoch == slos.slos_sb->sb_epoch &&
			    ptr.offset != 0) {
				/* The segment is current and exists on disk. */
				slos_ptr_trimstart(bp->b_lblkno,
				    ITER_KEY_T(iter, uint64_t), fsbsize, &ptr);
			} else {
				/* Otherwise we need to create it. */
				error = BTREE_LOCK(
				    &SLSVP(vp)->sn_tree, LK_UPGRADE);
				if (error != 0)
					goto error;

				/*
				 * Insert the range into the tree, possibly
				 * overwriting or clipping existing ranges.
				 * Actually allocate the block, then insert it
				 * to actually back the new range.
				 */
				error = fbtree_rangeinsert(&SLSVP(vp)->sn_tree,
				    bp->b_lblkno, bp->b_bcount);
				if (error != 0)
					goto error;

				error = slos_blkalloc(
				    SLSVP(vp)->sn_slos, bp->b_bcount, &ptr);
				if (error != 0)
					goto error;

				error = fbtree_replace(
				    &SLSVP(vp)->sn_tree, &bp->b_lblkno, &ptr);
				if (error != 0)
					goto error;
			}

			atomic_add_64(
			    &slos.slos_sb->sb_data_synced, bp->b_bcount);
			bp->b_blkno = ptr.offset;
		} else if (bp->b_iocmd == BIO_READ) {
			if (ptr.offset != 0) {
				/* Find where we must start reading from. */
				slos_ptr_trimstart(bp->b_lblkno,
				    ITER_KEY_T(iter, uint64_t), fsbsize, &ptr);
				bp->b_blkno = ptr.offset;
			} else {
				/* The segment is unbacked, zero fill.  */
				ITER_RELEASE(iter);
				bp->b_blkno = (daddr_t)(-1);
				vfs_bio_clrbuf(bp);
				bufdone(bp);
				return (0);
			}
		}

		/* This unlocks the btree. */
		ITER_RELEASE(iter);
	} else {
		/*
		 * No other translation, the blocks are right in the disk.
		 */
		bp->b_blkno = bp->b_lblkno;
		SDT_PROBE3(slos, , , slsfs_deviceblk, bp->b_blkno, fsbsize,
		    fsbsize / devbsize);
		if (bp->b_iocmd == BIO_WRITE) {
			atomic_add_64(
			    &slos.slos_sb->sb_meta_synced, bp->b_bcount);
		}
	}

	KASSERT(bp->b_resid <= ptr.size,
	    ("Filling buffer %p with "
	     "%lu bytes from region with %lu bytes",
		bp, bp->b_resid, ptr.size));
	KASSERT(bp->b_blkno != 0,
	    ("Vnode %p has buffer %p without a disk address", bp, vp));


	/* The physical disk offset in bytes. */
	KASSERT(devbsize == dbtob(1),
	    ("Inconsistent device block size, %lu vs %lu", devbsize, dbtob(1)));
	/* Scale the block number from the filesystem's to the device's size. */
	bp->b_blkno *= (fsbsize / devbsize);
	bp->b_iooffset = dbtob(bp->b_blkno);

#ifdef VERBOSE
	if (bp->b_iocmd == BIO_WRITE) {
		DEBUG4("bio_write: bp(%p), vp(%lu) - %lu:%lu", bp,
		    SLSVP(vp)->sn_pid, bp->b_lblkno, bp->b_blkno);
	} else {
		DEBUG4("bio_read: bp(%p), vp(%lu) - %lu:%lu", bp,
		    SLSVP(vp)->sn_pid, bp->b_lblkno, bp->b_blkno);
	}
#endif
	g_vfs_strategy(&slos.slos_vp->v_bufobj, bp);
	if (checksum_enabled) {
		error = slsfs_cksum(bp);
		if (error) {
			panic("Problem with checksum for buffer %p", bp);
		}
	}

	return (0);

error:
	ITER_RELEASE(iter);
	bp->b_error = error;
	bp->b_ioflags |= BIO_ERROR;
	bufdone(bp);

	return (0);
}

static int
slsfs_chmod(struct vnode *vp, int mode, struct ucred *cred, struct thread *td)
{
	struct slos_node *node = SLSVP(vp);
	int error;

	if ((error = VOP_ACCESSX(vp, VWRITE_ACL, cred, td))) {
		return (error);
	}

	if (vp->v_type != VDIR && (mode & S_ISTXT)) {
		if (priv_check_cred(cred, PRIV_VFS_STICKYFILE, 0)) {
			return (EFTYPE);
		}
	}

	if (!groupmember(node->sn_ino.ino_gid, cred) && (mode & ISGID)) {
		error = priv_check_cred(cred, PRIV_VFS_SETGID, 0);
		if (error) {
			return (error);
		}
	}

	if ((mode & ISUID) && node->sn_ino.ino_uid != cred->cr_uid) {
		error = priv_check_cred(cred, PRIV_VFS_ADMIN, 0);
		if (error) {
			return (error);
		}
	}

	node->sn_ino.ino_mode &= ~ALLPERMS;
	node->sn_ino.ino_mode |= (mode & ALLPERMS);
	node->sn_ino.ino_flags |= IN_CHANGE;
	if (error == 0 && (node->sn_ino.ino_flags & IN_CHANGE) != 0) {
		error = slos_update(node);
	}

	return (error);
}

static int
slsfs_chown(struct vnode *vp, uid_t uid, gid_t gid, struct ucred *cred,
    struct thread *td)
{
	uid_t ouid;
	gid_t ogid;
	int error = 0;

	struct slos_node *svp = SLSVP(vp);

	if (uid == (uid_t)VNOVAL)
		uid = svp->sn_ino.ino_uid;

	if (gid == (uid_t)VNOVAL)
		gid = svp->sn_ino.ino_gid;

	if ((error = VOP_ACCESSX(vp, VWRITE_OWNER, cred, td))) {
		return (error);
	}

	if (((uid != svp->sn_ino.ino_uid && uid != cred->cr_uid) ||
		(gid != svp->sn_ino.ino_gid && !groupmember(gid, cred))) &&
	    (error = priv_check_cred(cred, PRIV_VFS_CHOWN, 0))) {

		return (error);
	}

	ouid = svp->sn_ino.ino_uid;
	ogid = svp->sn_ino.ino_gid;

	svp->sn_ino.ino_uid = uid;
	svp->sn_ino.ino_gid = gid;

	svp->sn_status |= IN_CHANGE;
	if ((svp->sn_ino.ino_mode & (ISUID | ISGID)) &&
	    (ouid != uid || ogid != gid)) {
		if (priv_check_cred(cred, PRIV_VFS_RETAINSUGID, 0)) {
			svp->sn_ino.ino_mode &= ~(ISUID | ISGID);
		}
	}
	return (0);
}

static int
slsfs_setattr(struct vop_setattr_args *args)
{
	struct vnode *vp = args->a_vp;
	struct vattr *vap = args->a_vap;
	struct ucred *cred = args->a_cred;
	struct thread *td = curthread;
	struct slos_node *node = SLSVP(vp);
	int error = 0;

	if ((vap->va_type != VNON) || (vap->va_nlink != VNOVAL) ||
	    (vap->va_fsid != VNOVAL) || (vap->va_fileid != VNOVAL) ||
	    (vap->va_blocksize != VNOVAL) || (vap->va_rdev != VNOVAL) ||
	    (vap->va_bytes != VNOVAL) || (vap->va_gen != VNOVAL)) {

		return (EINVAL);
	}

	/*
	 * XXX Flag handling is incomplete (some flags might be ignored),
	 * but preventing their use blocks us from running specific commands
	 * on the file system.
	 */
	if (vap->va_flags != VNOVAL) {

		/* Support the same flags as ext2. */
		if (vap->va_flags & ~(SF_APPEND | SF_IMMUTABLE))
			return (EOPNOTSUPP);

		if (vp->v_mount->mnt_flag & MNT_RDONLY)
			return (EROFS);

		if ((error = VOP_ACCESS(vp, VADMIN, cred, td)))
			return (error);

		if (!priv_check_cred(cred, PRIV_VFS_SYSFLAGS, 0)) {
			if (node->sn_ino.ino_flags &
			    (SF_IMMUTABLE | SF_APPEND)) {
				error = securelevel_gt(cred, 0);
				if (error)
					return (error);
			}
		} else {
			if (node->sn_ino.ino_flags & (SF_IMMUTABLE | SF_APPEND))
				return (EPERM);
		}

		node->sn_ino.ino_flags = vap->va_flags | IN_CHANGE;
		error = slos_update(node);
		if (node->sn_ino.ino_flags & (IMMUTABLE | APPEND)) {
			return (error);
		}
	}

	if (node->sn_ino.ino_flags & (IMMUTABLE | APPEND)) {
		return (EPERM);
	}

	if (vap->va_size != (u_quad_t)VNOVAL) {
		switch (vp->v_type) {
		case VDIR:
			return (EISDIR);
		case VLNK:
		case VREG:
			if (vp->v_mount->mnt_flag & MNT_RDONLY) {
				return (EROFS);
			}
			if ((node->sn_ino.ino_flags & SF_SNAPSHOT) != 0) {
				return (EPERM);
			}
			break;
		default:
			return (0);
		}

		if (SLS_ISWAL(vp))
			return (EPERM);
		/*
		 * For SAS objects we only use truncate to adjust the size in
		 * the file metadata. The underlying object is already large
		 * enough accommodate the maximum size and is not affected.
		 * There are no buffers to remove or OBJT_VNODE objects to
		 * adjust.
		 */
		if (SLS_ISSAS(vp)) {
			node->sn_ino.ino_size = vap->va_size;
		} else {
			error = slos_truncate(vp, vap->va_size);
			if (error)
				return (error);
		}
	}

	if (vap->va_uid != (uid_t)VNOVAL || vap->va_gid != (gid_t)VNOVAL) {
		if (vp->v_mount->mnt_flag & MNT_RDONLY) {
			return (EROFS);
		}

		error = slsfs_chown(vp, vap->va_uid, vap->va_gid, cred, td);
		if (error) {
			return (error);
		}
	}

	if (vap->va_atime.tv_sec != VNOVAL || vap->va_mtime.tv_sec != VNOVAL ||
	    vap->va_birthtime.tv_sec != VNOVAL) {

		if ((node->sn_ino.ino_flags & SF_SNAPSHOT) != 0) {
			return (EPERM);
		}
		error = vn_utimes_perm(vp, vap, cred, td);
		if (error) {
			return (error);
		}

		node->sn_ino.ino_flags |= IN_CHANGE | IN_MODIFIED;
		if (vap->va_atime.tv_sec != VNOVAL) {
			node->sn_ino.ino_flags &= ~IN_ACCESS;
			node->sn_ino.ino_atime = vap->va_atime.tv_sec;
			node->sn_ino.ino_atime_nsec = vap->va_atime.tv_nsec;
		}

		if (vap->va_mtime.tv_sec != VNOVAL) {
			node->sn_ino.ino_flags &= ~IN_UPDATE;
			node->sn_ino.ino_mtime = vap->va_mtime.tv_sec;
			node->sn_ino.ino_mtime_nsec = vap->va_mtime.tv_nsec;
		}

		error = slos_update(node);

		if (error) {
			return (error);
		}
	}

	error = 0;
	if (vap->va_mode != (mode_t)VNOVAL) {
		if (vp->v_mount->mnt_flag & MNT_RDONLY) {
			return (EROFS);
		}
		if ((node->sn_ino.ino_flags & SF_SNAPSHOT) != 0) {
			return (EPERM);
		}
		error = slsfs_chmod(vp, (int)vap->va_mode, cred, td);
	}

	return (error);
}

/* Check to make sure the target directory does not have the src directory
 * within it, this is used to stop cycles from occuring from hard links */
static int
slsfs_checkpath(struct vnode *src, struct vnode *target, struct ucred *cred)
{
	int error = 0;
	struct dirent dir;

	DEBUG("Checking path");
	if (SLSVP(target)->sn_pid == SLSVP(src)->sn_pid) {
		error = EEXIST;
		goto out;
	}

	if (SLSVP(target)->sn_pid == SLOS_ROOT_INODE) {
		goto out;
	}

	for (;;) {
		if (target->v_type != VDIR) {
			error = ENOTDIR;
			break;
		}

		error = vn_rdwr(UIO_READ, target, &dir, sizeof(struct dirent),
		    0, UIO_SYSSPACE, IO_NODELOCKED | IO_NOMACCHECK, cred,
		    NOCRED, NULL, NULL);
		if (error != 0) {
			DEBUG1("Error reading a writing %d", error);
			break;
		}

		if (dir.d_namlen != 2 || dir.d_name[0] != '.' ||
		    dir.d_name[1] != '.') {
			DEBUG("Not a directory");
			error = ENOTDIR;
			break;
		}

		if (dir.d_fileno == SLSVP(src)->sn_pid) {
			DEBUG("Found within path");
			error = EINVAL;
			break;
		}

		if (dir.d_fileno == SLOS_ROOT_INODE) {
			DEBUG("Parent is root");
			break;
		}
		vput(target);
		if ((error = VFS_VGET(src->v_mount, dir.d_fileno, LK_EXCLUSIVE,
			 &target)) != 0) {
			target = NULL;
			break;
		}
	}
out:
	if (target != NULL) {
		vput(target);
	}
	return (error);
}

static int
slsfs_rename(struct vop_rename_args *args)
{
	struct vnode *tvp = args->a_tvp;   // Target Vnode (if it exists)
	struct vnode *tdvp = args->a_tdvp; // Target directory
	struct vnode *fvp = args->a_fvp;   // Source vnode
	struct vnode *fdvp = args->a_fdvp; // From directory
	int error = 0;

	struct componentname *tname = args->a_tcnp; // Name data of target
	struct componentname *fname = args->a_fcnp; // Name data of source

	struct slos_node *svp = SLSVP(fvp);
	struct slos_node *sdvp = SLSVP(fdvp);
	struct slos_node *tdnode = SLSVP(tdvp);
	struct slos_node *tnode = NULL, *fnode1 = NULL;
	uint64_t oldparent = 0, newparent = 0;
	int isdir = 0;

	mode_t mode = svp->sn_ino.ino_mode;

	DEBUG("Rename or move");
	// Following nandfs example here -- cross device renaming
	if ((fvp->v_mount != tdvp->v_mount) ||
	    (tvp && (fvp->v_mount != tvp->v_mount))) {
		error = EXDEV;
	abort:
		if (tdvp == tvp) {
			vrele(tdvp);
		} else {
			vput(tdvp);
		}

		if (tvp) {
			vput(tvp);
		}

		vrele(fdvp);
		vrele(fvp);
		return (error);
	}

	if (tvp &&
	    ((SLSVP(tvp)->sn_ino.ino_flags & (NOUNLINK | IMMUTABLE | APPEND)) ||
		(SLSVP(tdvp)->sn_ino.ino_flags & APPEND))) {
		error = EPERM;
		goto abort;
	}

	if (fvp == tvp) {
		error = 0;
		DEBUG("Cannot rename a file to itself");
		goto abort;
		vput(tvp);
	}

	if ((error = vn_lock(fvp, LK_EXCLUSIVE)) != 0) {
		goto abort;
	}

	if ((SLSVP(fvp)->sn_ino.ino_flags & (NOUNLINK | IMMUTABLE | APPEND)) ||
	    (SLSVP(fdvp)->sn_ino.ino_flags & APPEND)) {
		VOP_UNLOCK(fvp, 0);
		error = EPERM;
		goto abort;
	}

	// Check if the source is a directory and whether we are renaming a
	// directory
	if ((mode & S_IFMT) == S_IFDIR) {
		int isdot = fname->cn_namelen == 1 &&
		    fname->cn_nameptr[0] == '.';
		int isownparent = fdvp == fvp;
		int isdotdot = (fname->cn_flags | tname->cn_flags) & ISDOTDOT;
		if (isdot || isdotdot || isownparent) {
			VOP_UNLOCK(fvp, 0);
			error = EINVAL;
			goto abort;
		}
		isdir = 1;
		svp->sn_ino.ino_flags |= IN_RENAME;
		oldparent = sdvp->sn_pid;
	}

	vrele(fdvp);

	// Check whether there exists a file that we are replacing
	if (tvp) {
		tnode = SLSVP(tvp);
	}

	SLSVP(fvp)->sn_ino.ino_nlink++;

	error = VOP_ACCESS(fvp, VWRITE, tname->cn_cred, tname->cn_thread);
	VOP_UNLOCK(fvp, 0);
	if (oldparent != tdnode->sn_pid) {
		newparent = tdnode->sn_pid;
	}

	if (isdir && newparent) {
		DEBUG("Checking if directory doens't exist within path");
		if (error) {
			goto bad;
		}
		error = slsfs_checkpath(fvp, tdvp, tname->cn_cred);
		if (error) {
			goto bad;
		}

		VREF(tdvp);
		error = relookup(tdvp, &tvp, tname);
		if (error) {
			goto bad;
		}
		vrele(tdvp);
		tdnode = SLSVP(tdvp);
		tnode = NULL;
		if (tvp) {
			tnode = SLSVP(tvp);
		}
	}

	if (tvp == NULL) {
		DEBUG("tvp is null, directory doens't exist");
		if (isdir && fdvp != tdvp) {
			tdnode->sn_ino.ino_nlink++;
		}

		error = slsfs_add_dirent(tdvp, svp->sn_ino.ino_pid,
		    tname->cn_nameptr, tname->cn_namelen,
		    IFTODT(svp->sn_ino.ino_mode));
		if (error) {
			if (isdir && fdvp != tdvp) {
				slsfs_declink(tdvp);
			}
			goto bad;
		}

		slos_update(tdnode);
		vput(tdvp);
	} else {
		DEBUG("!null tdvp");
		if ((tdnode->sn_ino.ino_mode & S_ISTXT) &&
		    tname->cn_cred->cr_uid != 0 &&
		    tname->cn_cred->cr_uid != tdnode->sn_ino.ino_uid &&
		    tnode->sn_ino.ino_uid != tname->cn_cred->cr_uid) {
			error = EPERM;

			goto bad;
		}

		mode = tnode->sn_ino.ino_mode;
		if ((mode & S_IFMT) == S_IFDIR) {
			if (!slsfs_dirempty(tvp)) {
				error = ENOTEMPTY;
				goto bad;
			}

			if (!isdir) {
				error = ENOTDIR;
				goto bad;
			}

			cache_purge(tvp);
		} else if (isdir) {
			error = EISDIR;
			goto bad;
		}

		error = slsfs_update_dirent(tdvp, fvp, tvp);
		if (error) {
			goto bad;
		}

		if (isdir && !newparent) {
			MPASS(tdnode->sn_ino.ino_nlink != 0);
			slsfs_declink(tdvp);
		}

		vput(tdvp);
		slsfs_declink(tvp);
		vput(tvp);
		tnode = NULL;
	}

	fname->cn_flags &= ~MODMASK;
	fname->cn_flags |= LOCKPARENT | LOCKLEAF;
	VREF(fdvp);
	KASSERT(SLSVP(fdvp)->sn_ino.ino_nlink >= 2,
	    ("Problem with link number %p", fdvp));
	error = relookup(fdvp, &fvp, fname);
	if (error == 0) {
		vrele(fdvp);
	}

	if (fvp != NULL) {
		DEBUG("fvp != null");
		fnode1 = SLSVP(fvp);
		sdvp = SLSVP(fdvp);
	} else {
		if (isdir) {
			panic("lost dir");
		}
		DEBUG("fvp == NULL");
		vrele(args->a_fvp);
		vrele(fdvp);
		return (0);
	}

	if (fnode1 != svp) {
		DEBUG("fnode1 != svp");
		if (isdir) {
			panic("lost dir");
		}
	} else {
		if (isdir && newparent) {
			DEBUG("isdir && newparent");
			slsfs_declink(fdvp);
		}
		DEBUG("Removing dirent");
		error = slsfs_unlink_dir(fdvp, fvp, fname);
		if (error) {
			panic("Problem unlinking directory");
		} else {
			slsfs_declink(fvp);
		}
		svp->sn_ino.ino_flags &= ~IN_RENAME;
	}

	if (sdvp) {
		vput(fdvp);
	}

	if (svp) {
		vput(fvp);
	}

	DEBUG("usecount-- fvp");
	vrele(args->a_fvp);

	KASSERT(SLSVP(fdvp)->sn_ino.ino_nlink >= 2,
	    ("Problem with link number after %p", fdvp));

	return (error);
bad:
	if (tnode) {
		vput(tvp);
	}

	vput(tdvp);

	if (isdir) {
		svp->sn_status &= ~IN_RENAME;
	}
	if (vn_lock(fvp, LK_EXCLUSIVE) == 0) {
		MPASS(svp->sn_ino.ino_nlink != 0);
		slsfs_declink(fvp);
		svp->sn_ino.ino_flags &= ~IN_RENAME;
		vput(fvp);
	} else {
		vrele(fvp);
	}

	KASSERT(SLSVP(fdvp)->sn_ino.ino_nlink >= 2,
	    ("Problem with link number after bad %p", fdvp));

	return (error);
}

/* Get the number of extents for the inode. */
static int
slsfs_numextents(struct slos_node *svp, uint64_t *numextentsp)
{
	/* Read the offset we want to start from. */
	uint64_t lblkno = *numextentsp;
	uint64_t numextents = 0;
	struct fnode_iter iter;
	int error;

	/* Find the first logical block after the given block number. */
	BTREE_LOCK(&svp->sn_tree, LK_SHARED);
	error = fbtree_keymax_iter(&svp->sn_tree, &lblkno, &iter);
	if (error != 0) {
		BTREE_UNLOCK(&svp->sn_tree, 0);
		return (error);
	}

	/* There are no extentp after the limit, notify the caller. */
	if (ITER_ISNULL(iter)) {
		numextents = 0;
		ITER_RELEASE(iter);
		return (0);
	}

	for (; !ITER_ISNULL(iter); ITER_NEXT(iter))
		numextents += 1;

	ITER_RELEASE(iter);

	*numextentsp = numextents;
	return (0);
}

/* Fill in the slos_extent structures with the file's extents. */
static int
slsfs_getextents(struct slos_node *svp, struct slos_extent *extents)
{
	/* Read the offset we want to start from. */
	uint64_t lblkno = extents[0].sxt_lblkno;
	struct fnode_iter iter;
	uint64_t fileblkcnt;
	int error;
	int i;

	/* Find the first logical block after the given block number. */
	BTREE_LOCK(&svp->sn_tree, LK_SHARED);
	error = fbtree_keymax_iter(&svp->sn_tree, &lblkno, &iter);
	if (error != 0) {
		BTREE_UNLOCK(&svp->sn_tree, 0);
		return (error);
	}

	/* There are no extentp after the limit, notify the caller. */
	if (ITER_ISNULL(iter)) {
		ITER_RELEASE(iter);
		return (0);
	}

	for (i = 0; !ITER_ISNULL(iter); ITER_NEXT(iter), i++) {
		fileblkcnt = svp->sn_ino.ino_size / IOSIZE(svp);
		KASSERT(lblkno < fileblkcnt, ("extent past the end of file"));

		extents[i].sxt_lblkno = ITER_KEY_T(iter, uint64_t);
		extents[i].sxt_cnt = (ITER_VAL_T(iter, diskptr_t).size) /
		    IOSIZE(svp);
	}

	ITER_RELEASE(iter);

	return (0);
}

/* Assign a type to the node's records. */
static int
slsfs_setrstat(struct slos_node *svp, struct slos_rstat *st)
{
	svp->sn_ino.ino_rstat = *st;
	return (0);
}

/* Get the nodes' record type. */
static int
slsfs_getrstat(struct slos_node *svp, struct slos_rstat *st)
{
	*st = svp->sn_ino.ino_rstat;
	return (0);
}

static int
slsfs_mountsnapshot(int index)
{
	struct mount *mp = slos.slsfs_mount;
	struct slsfsmount *smp = mp->mnt_data;

	SLOS_LOCK(&slos);
	slos_setstate(&slos, SLOS_SNAPCHANGE);
	smp->sp_index = index;
	SLOS_UNLOCK(&slos);

	return VFS_MOUNT(mp);
}

static int
slsfs_sas_create(char *path, size_t size, int *ret)
{
	uint64_t *sas_new_addr = &slos.slos_sb->sb_sas_addr;
	struct thread *td = curthread;
	int flags = O_CREAT | O_RDWR;
	struct slos_node *svp;
	struct vnode *vp;
	struct file *fp;
	int mode = 0666;
	int error;
	int fd;

	if (size == 0) {
		*ret = -1;
		return EINVAL;
	}

	size = roundup(size, PAGE_SIZE);
	if (size > MAX_SAS_SIZE)
		return EINVAL;

	error = kern_openat(td, AT_FDCWD, path, UIO_SYSSPACE, flags, mode);
	if (error != 0) {
		*ret = -1;
		return (error);
	}

	fd = td->td_retval[0];
	error = fget(td, fd, &cap_no_rights, &fp);
	if (error != 0) {
		*ret = fd;
		return (error);
	}

	vp = fp->f_vnode;
	vp->v_op = &slsfs_sas_vnodeops;

	svp = SLSVP(vp);
	svp->sn_addr = atomic_fetchadd_64(sas_new_addr, size + PAGE_SIZE);
	if (svp->sn_addr + size >= SLS_SAS_MAXADDR)
		panic("Reached the end of the SAS");

	svp->sn_obj = vm_object_allocate(OBJT_DEFAULT, atop(size));
	if (svp->sn_obj != NULL) {
		svp->sn_obj->flags |= OBJ_NOSPLIT;
		kern_close(td, fd);
		fd = -1;
	}

	fdrop(fp, td);

	*ret = fd;
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

static __attribute__((noinline)) struct vnode *
slsfs_sas_getvp(uint64_t oid)
{
	struct vnode *vp;
	int error;

	error = slos_svpalloc(&slos, MAKEIMODE(VREG, S_IRWXU), &oid);
	if (error != 0) {
		printf("%s:%d error %d\n", __func__, __LINE__, error);
		return (NULL);
	}

	error = VFS_VGET(slos.slsfs_mount, oid, LK_EXCLUSIVE, &vp);
	if (error != 0) {
		printf("%s:%d error %d\n", __func__, __LINE__, error);
		return (NULL);
	}

	return (vp);
}

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

static int
sas_bawrite(struct vnode *vp, vm_page_t *ma, size_t mlen)
{
	size_t off, size;
	struct buf *bp;
	int i;

	off = PAGE_SIZE * (ma[0]->pindex + SLOS_OBJOFF);
	size = PAGE_SIZE * mlen;

	bp = trypbuf(&slos_pbufcnt);
	KASSERT(bp != NULL, ("failed to grab buffer"));
	while (bp == NULL) {
		pause_sbt("saspbf", 30 * SBT_1US, 0, 0);
		bp = trypbuf(&slos_pbufcnt);
	}

	bp->b_data = unmapped_buf;
	bp->b_lblkno = ma[0]->pindex + SLOS_OBJOFF;
	bp->b_iocmd = BIO_WRITE;

	bp->b_resid = bp->b_bufsize = bp->b_bcount = mlen * PAGE_SIZE;
	bp->b_iocmd = BIO_WRITE;
	bp->b_iodone = sas_pager_done;

	bp->b_npages = mlen;
	for (i = 0; i < mlen; i++)
		bp->b_pages[i] = ma[i];

	slos_iotask_create(vp, bp, true);

	return (0);
}

#define MAX_PAGES (16)

static int __attribute__((noinline))
sas_genio(struct vnode *vp, struct pglist *snaplist, uint64_t oid)
{
	vm_page_t ma[MAX_PAGES];
	vm_pindex_t pindex;
	vm_page_t m, mtmp;
	size_t mlen;

	pindex = 0;
	mlen = 0;
	TAILQ_FOREACH_SAFE (m, snaplist, snapq, mtmp) {
		vm_page_lock(m);
		if (m->object == NULL) {
			vm_page_unlock(m);
			continue;
		}
		if (m->object->objid == oid) {
			ma[mlen++] = m;
			slsfs_sas_page_untrack_unlocked(snaplist, m);
		}

		vm_page_unlock(m);

		if (mlen == MAX_PAGES) {
			sas_bawrite(vp, ma, mlen);
			mlen = 0;
		}
	}

	if (mlen > 0)
		sas_bawrite(vp, ma, mlen);

	return (0);
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
	struct vnode *vp;
	uint64_t oid;

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
		oid = TAILQ_FIRST(snaplist)->object->objid;
		vp = slsfs_sas_getvp(oid);
		sas_genio(vp, snaplist, oid);
		vput(vp);
	}

	SDT_PROBE1(sas, , , write, written);

	/* XXX This flag is used only to make the evaluation scripts easier. */
	if (!slsfs_sas_commit_async)
		taskqueue_drain_all(slos.slos_tq);
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

static int
slsfs_ioctl(struct vop_ioctl_args *ap)
{
	uint64_t *numextentsp;
	uint64_t *checks;
	int fd;
	int error;

	struct vnode *vp = ap->a_vp;
	struct thread *td = curthread;
	u_long com = ap->a_command;
	struct slos_node *svp = SLSVP(vp);
	struct slos_rstat *st = NULL;
	struct slos_extent *extents;
	struct slsfs_getsnapinfo *info = NULL;
	struct slsfs_create_wal_args *wal_args = NULL;
	struct slsfs_sas_create_args *sas_args = NULL;

	switch (com) {
	case SLS_NUM_EXTENTS:
		numextentsp = (uint64_t *)ap->a_data;
		return (slsfs_numextents(svp, numextentsp));

	case SLS_GET_EXTENTS:
		extents = (struct slos_extent *)ap->a_data;
		return (slsfs_getextents(svp, extents));

	case SLS_SET_RSTAT:
		st = (struct slos_rstat *)ap->a_data;
		return (slsfs_setrstat(svp, st));

	case SLS_GET_RSTAT:
		st = (struct slos_rstat *)ap->a_data;
		return (slsfs_getrstat(svp, st));

	case SLSFS_GET_SNAP:
		info = (struct slsfs_getsnapinfo *)ap->a_data;
		return (slos_sbat(&slos, info->index, &info->snap_sb));

	case SLSFS_MOUNT_SNAP:
		DEBUG("Remounting on snap");
		info = (struct slsfs_getsnapinfo *)ap->a_data;
		return (slsfs_mountsnapshot(info->index));

	case SLSFS_COUNT_CHECKPOINTS:
		checks = (uint64_t *)ap->a_data;
		*checks = checkpoints;
		return (0);

	case SLSFS_CREATE_WAL:
		wal_args = (struct slsfs_create_wal_args *)ap->a_data;
		error = _slsfs_create_wal(wal_args->path, wal_args->flags,
		    wal_args->mode, wal_args->size, &fd);
		if (error) {
			td->td_retval[0] = -1;
			return (error);
		}

		td->td_retval[0] = fd;
		return (error);

	case SLSFS_SAS_CREATE:
		sas_args = (struct slsfs_sas_create_args *)ap->a_data;
		error = slsfs_sas_create(sas_args->path, sas_args->size, &fd);
		if (error != 0) {
			td->td_retval[0] = -1;
			return (error);
		}

		td->td_retval[0] = 0;
		return (0);

	case FIOSEEKDATA: // Fallthrough
	case FIOSEEKHOLE:
		printf("UNSUPPORTED SLSFS IOCTL FIOSEEKDATA/HOLE");
		return (ENOSYS);

	default:
		return (ENOTTY);
	}
}

static int
slsfs_symlink(struct vop_symlink_args *ap)
{
	struct vnode **vpp = ap->a_vpp;
	struct vnode *dvp = ap->a_dvp;
	int len, error;
	uint16_t mode = MAKEIMODE(ap->a_vap->va_type, ap->a_vap->va_mode);
	struct componentname *cnp = ap->a_cnp;
	struct vnode *vp;

	error = SLS_VALLOC(dvp, mode | S_IFLNK, cnp->cn_cred, &vp);
	if (error) {
		return (error);
	}

	error = slsfs_add_dirent(dvp, SLSVP(vp)->sn_pid, cnp->cn_nameptr,
	    cnp->cn_namelen, IFTODT(mode));
	if (error) {
		vput(vp);
		return ENOTDIR;
	}

	len = strlen(ap->a_target);
	SLSVP(vp)->sn_ino.ino_size = len;

	error = vn_rdwr(UIO_WRITE, vp, ap->a_target, len, (off_t)0,
	    UIO_SYSSPACE, IO_NODELOCKED | IO_NOMACCHECK, cnp->cn_cred, NOCRED,
	    NULL, NULL);
	if (error) {
		vput(vp);
	}
	SLSVP(vp)->sn_status |= SLOS_DIRTY;
	*vpp = vp;

	return (error);
}

static int
slsfs_readlink(struct vop_readlink_args *ap)
{
	struct vnode *vp = ap->a_vp;

	return (VOP_READ(vp, ap->a_uio, 0, ap->a_cred));
}

static int
slsfs_link(struct vop_link_args *ap)
{
	int error = 0;

	struct vnode *tdvp = ap->a_tdvp;
	struct vnode *vp = ap->a_vp;
	struct componentname *cnp = ap->a_cnp;

	DEBUG1("Linking file %p", vp);

	error = slsfs_add_dirent(tdvp, SLSVP(vp)->sn_pid, cnp->cn_nameptr,
	    cnp->cn_namelen, IFTODT(SLSVP(vp)->sn_ino.ino_mode));
	if (error) {
		panic("Problem linking");
	}
	SLSVP(vp)->sn_ino.ino_nlink++;
	SLSVP(vp)->sn_ino.ino_flags |= IN_CHANGE;
	slos_update(SLSVP(vp));

	return (error);
}

static int
slsfs_markatime(struct vop_markatime_args *args)
{
	struct vnode *vp = args->a_vp;
	struct slos_node *svp = SLSVP(vp);

	VI_LOCK(vp);
	svp->sn_ino.ino_flags = IN_ACCESS;
	VI_UNLOCK(vp);

	slos_update(svp);

	return (0);
}

/*
 * Although the syscall mknod is deprecated, the syscall mkfifo still requires
 * VOP_MKNOD.
 */
static int
slsfs_mknod(struct vop_mknod_args *args)
{

	struct vnode *vp;
	int error;

	struct vnode *dvp = args->a_dvp;
	struct vnode **vpp = args->a_vpp;
	struct componentname *name = args->a_cnp;
	struct vattr *vap = args->a_vap;

	mode_t mode = MAKEIMODE(vap->va_type, vap->va_mode);
	error = SLS_VALLOC(dvp, mode, name->cn_cred, &vp);
	if (error) {
		*vpp = NULL;
		return (error);
	}

	error = slsfs_add_dirent(
	    dvp, VINUM(vp), name->cn_nameptr, name->cn_namelen, IFTODT(mode));
	if (error == -1) {
		return (EIO);
	}

	SLSVP(vp)->sn_ino.ino_gid = SLSVP(dvp)->sn_ino.ino_gid;
	SLSVP(vp)->sn_ino.ino_uid = name->cn_cred->cr_uid;

	if (vap->va_rdev != VNOVAL) {
		SLSVP(vp)->sn_ino.ino_special = vap->va_rdev;
	}

	SLSVP(vp)->sn_status |= IN_ACCESS | IN_CHANGE | IN_UPDATE;

	*vpp = vp;

	return (0);
}

static int
slsfs_pathconf(struct vop_pathconf_args *args)
{
	int error = 0;
	struct vnode *vp = args->a_vp;

	switch (args->a_name) {
	case _PC_PIPE_BUF:
		if (vp->v_type == VDIR || vp->v_type == VFIFO) {
			*args->a_retval = PIPE_BUF;
		} else {
			error = EINVAL;
		}
		break;
	case _PC_NAME_MAX:
		*args->a_retval = SLSFS_NAME_LEN;
		break;
	case _PC_ALLOC_SIZE_MIN:
		*args->a_retval = BLKSIZE(&slos);
		break;
	case _PC_ACL_EXTENDED:
		*args->a_retval = 0;
		break;
	case _PC_FILESIZEBITS:
		*args->a_retval = 64;
		break;
	case _PC_REC_MIN_XFER_SIZE:
		*args->a_retval = IOSIZE(SLSVP(vp));
		break;
	case _PC_REC_MAX_XFER_SIZE:
		*args->a_retval = -1;
		break;
	default:
		error = vop_stdpathconf(args);
		break;
	}

	return (error);
}

static int
slsfs_wal_strategy(struct vop_strategy_args *args)
{
	struct buf *bp = args->a_bp;
	struct vnode *vp = args->a_vp;
	struct slos *slos = VPSLOS(vp);
	size_t fsbsize = vp->v_bufobj.bo_bsize;
	size_t devbsize = slos->slos_vp->v_bufobj.bo_bsize;

	bp->b_blkno *= (fsbsize / devbsize);
	bp->b_iooffset = dbtob(bp->b_blkno);
	BO_STRATEGY(&slos->slos_vp->v_bufobj, bp);
	return (0);
}

static int
slsfs_retrieve_wal_buf(struct vnode *vp, uint64_t offset, uint64_t size,
    enum uio_rw rw, int gbflag, int seqcount, struct buf **bp)
{
	struct buf *tempbuf = NULL;
	struct slos_node *svp = SLSVP(vp);
	struct slos_diskptr ptr = svp->sn_ino.ino_wal_segment;
	size_t blksize = IOSIZE(svp);
	uint64_t lbno = (offset / blksize);
	uint64_t pbno = ptr.offset + (offset / blksize);
	int error = 0;
	uint64_t newsize = omin(size, MAXBCACHEBUF);

	if (rw == UIO_READ) {
		if ((vp->v_mount->mnt_flag & MNT_NOCLUSTERR) == 0) {
			error = cluster_read(vp, svp->sn_ino.ino_size, lbno,
			    blksize, NOCRED, offset + size, seqcount, gbflag,
			    &tempbuf);
		} else {
			error = bread_gb(vp, lbno, newsize, NOCRED, gbflag,
			    &tempbuf);
		}
	} else if (rw == UIO_WRITE) {
		tempbuf = getblk(vp, lbno, newsize, 0, 0, gbflag);
		if (offset > roundup(SLSINO(SLSVP(vp)).ino_size, blksize)) {
			vfs_bio_clrbuf(tempbuf);
		}
	}

	if (tempbuf == NULL) {
		*bp = NULL;
		return (EIO);
	}

	tempbuf->b_blkno = pbno;
	*bp = tempbuf;

	return (error);
}

/*
 * Never mark a WAL section as dirty and it will always be skipped, easy to do
 * if we have our own buffer look up etc.
 */
static int
slsfs_wal_write(struct vop_write_args *args)
{
	struct buf *bp;
	int xfersize;
	size_t filesize;
	uint64_t off;
	int error = 0;
	int gbflag = 0;

	struct vnode *vp = args->a_vp;
	struct slos_node *svp = SLSVP(vp);
	size_t blksize = IOSIZE(svp);
	struct uio *uio = args->a_uio;
	int ioflag = args->a_ioflag;

	filesize = svp->sn_ino.ino_size;

	/* Check if full */
	if (uio->uio_offset < 0) {
		DEBUG1("Offset write at %lx", uio->uio_offset);
		return (EINVAL);
	}
	if (uio->uio_resid == 0) {
		return (0);
	}

	switch (vp->v_type) {
	case VREG:
		break;
	case VDIR:
		return (EISDIR);
	case VLNK:
		break;
	default:
		panic("bad file type %d", vp->v_type);
	}

	if (ioflag & IO_APPEND) {
		uio->uio_offset = filesize;
	}

	/* If  we are larger then the WAL segment allocated then dont write */
	if ((uio->uio_offset + uio->uio_resid) > SLS_WALSIZE(vp)) {
		return (ENOMEM);
	}

	if (uio->uio_offset + uio->uio_resid > filesize) {
		svp->sn_ino.ino_size = uio->uio_offset + uio->uio_resid;
		vnode_pager_setsize(vp, svp->sn_ino.ino_size);
		slos_update(svp);
	}

	while (uio->uio_resid) {
		/*
		 * Grab the key thats closest to offset, but not over it
		 * Mask out the lower order bits so we just have the block
		 */
		if (!checksum_enabled) {
			gbflag |= GB_UNMAPPED;
		}

		error = slsfs_retrieve_wal_buf(vp, uio->uio_offset,
		    uio->uio_resid, uio->uio_rw, gbflag, 0, &bp);
		if (error) {
			DEBUG1("Problem getting buffer for write %d", error);
			return (error);
		}

		off = uio->uio_offset - (bp->b_lblkno * blksize);
		KASSERT(off < bp->b_bcount,
		    ("Offset should inside buf, %p", bp));
		xfersize = omin(uio->uio_resid, bp->b_bcount - off);

		KASSERT(xfersize != 0, ("No 0 uio moves slsfs write"));
		KASSERT(xfersize <= uio->uio_resid, ("This should neveroccur"));
		if (buf_mapped(bp)) {
			error = vn_io_fault_uiomove((char *)bp->b_data + off,
			    xfersize, uio);
		} else {
			error = vn_io_fault_pgmove(bp->b_pages, off, xfersize,
			    uio);
		}

		vfs_bio_set_flags(bp, ioflag);

		/* Taken from FFS code on how they deal with specific flags */
		if (ioflag & IO_SYNC) {
			bwrite(bp);
		} else if (ioflag & IO_DIRECT) {
			bp->b_flags |= B_CLUSTEROK;
			bawrite(bp);
		} else {
			bp->b_flags |= B_CLUSTEROK;
			bdwrite(bp);
		}

		if (error || xfersize == 0)
			break;
	}

	return (error);
}

static int
slsfs_wal_read(struct vop_read_args *args)
{
	struct slos_inode *sivp;
	struct buf *bp;
	size_t filesize;
	uint64_t off;
	size_t resid;
	size_t toread;
	int gbflag = 0;
	int error = 0;

	struct vnode *vp = args->a_vp;
	struct slos_node *svp = SLSVP(vp);
	size_t blksize = IOSIZE(svp);
	struct uio *uio = args->a_uio;
	int seqcount = args->a_ioflag >> IO_SEQSHIFT;

	svp = SLSVP(vp);
	sivp = &SLSINO(svp);
	filesize = sivp->ino_size;

	// Check if full
	if (uio->uio_offset < 0)
		return (EINVAL);
	if (uio->uio_resid == 0)
		return (0);

	if (uio->uio_offset >= filesize) {
		return (0);
	}

	resid = omin(uio->uio_resid, (filesize - uio->uio_offset));
#ifdef VERBOSE
	DEBUG3("Reading filesize %lu - %lu, %lu", SLSVP(vp)->sn_pid, filesize,
	    uio->uio_offset);
#endif
	gbflag |= GB_UNMAPPED;

	while (resid > 0) {
		error = slsfs_retrieve_wal_buf(vp, uio->uio_offset, resid,
		    uio->uio_rw, gbflag, seqcount, &bp);
		if (error) {
			DEBUG1("Problem getting buffer for write %d", error);
			return (error);
		}

		off = uio->uio_offset - (bp->b_lblkno * blksize);
		toread = omin(resid, bp->b_bcount - off);

		/* One thing thats weird right now is our inodes and meta data
		 * is currently not
		 * in the buf cache, so we don't really have to worry about
		 * dirtying those buffers,
		 * but later we will have to dirty them.
		 */
		KASSERT(toread != 0, ("Should not occur"));
		if (buf_mapped(bp)) {
			error = vn_io_fault_uiomove((char *)bp->b_data + off,
			    toread, uio);
		} else {
			error = vn_io_fault_pgmove(bp->b_pages, off, toread,
			    uio);
		}
		brelse(bp);
		resid -= toread;
		if (error || toread == 0)
			break;
	}

	return (error);
}

void
slsfs_mark_wal(struct vnode *vp)
{
	vp->v_op = &slsfs_wal_vnodeops;
}

static int
slsfs_wal_allocate_extent(struct vnode *vp, size_t size)
{
	struct slos_node *svp;
	struct slos_inode *ino;
	struct slos *slos;
	int error;

	svp = SLSVP(vp);
	ino = &svp->sn_ino;
	slos = VPSLOS(vp);

	error = slos_blkalloc_wal(slos, size, &ino->ino_wal_segment);
	if (error != 0)
		return error;

	wal_space_used += ino->ino_wal_segment.size;
	svp->sn_ino.ino_size = 0;
	slos_update(svp);

	return 0;
}

static int
slsfs_wal_fsync(struct vop_fsync_args *ap)
{
	return (vn_fsync_buf(ap->a_vp, ap->a_waitfor));
}

int
_slsfs_create_wal(char *path, int flags, int mode, size_t size, int *ret)
{
	struct file *fp;
	struct vnode *vp;
	int error;
	int fd = -1;

	struct thread *td = curthread;
	if (size == 0) {
		*ret = fd;
		return EINVAL;
	}

	if (size > MAX_WAL_SIZE) {
		return EINVAL;
	}

	error = kern_openat(td, AT_FDCWD, path, UIO_SYSSPACE, flags, mode);
	if (error != 0) {
		*ret = fd;
		return (error);
	}

	fd = td->td_retval[0];
	error = fget(td, fd, &cap_no_rights, &fp);
	if (error != 0) {
		*ret = fd;
		return (error);
	}

	vp = fp->f_vnode;
	fdrop(fp, td);

	slsfs_mark_wal(vp);

	error = slsfs_wal_allocate_extent(vp, size);
	if (error != 0) {
		*ret = fd;
		return (-1);
	}

	*ret = fd;
	return 0;
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
	int error;
	vm_offset_t addr;

	switch (com) {
	case SLSFS_SAS_MAP:
		addr = *(vm_offset_t *)ap->a_data;
		KASSERT(SLS_ISSAS(vp), ("mapping non-SAS node"));
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

	default:
		return (EINVAL);
	}

	return (0);
}

static void
slsfs_sas_destroy(struct vnode *vp)
{
	struct slos_node *svp = SLSVP(vp);
	vm_object_t obj = svp->sn_obj;

	if (obj == NULL)
		return;

	KASSERT(obj != NULL, ("Unbacked SAS node"));
	vm_object_deallocate(obj);

	svp->sn_obj = NULL;
	svp->sn_addr = (vm_offset_t)NULL;
}

struct vop_vector slsfs_fifoops = {
	.vop_default = &fifo_specops,
	.vop_fsync = VOP_PANIC,
	.vop_access = slsfs_access,
	.vop_inactive = slsfs_inactive,
	.vop_pathconf = slsfs_pathconf,
	.vop_read = VOP_PANIC,
	.vop_reclaim = slsfs_reclaim,
	.vop_setattr = slsfs_setattr,
	.vop_getattr = slsfs_getattr,
	.vop_write = VOP_PANIC,
};

struct vop_vector slsfs_vnodeops = {
	.vop_default = &default_vnodeops,
	.vop_fsync = slsfs_fsync,
	.vop_read = slsfs_read,
	.vop_reallocblks = VOP_PANIC, // TODO
	.vop_write = slsfs_write,
	.vop_access = slsfs_access,
	.vop_bmap = slsfs_bmap,
	.vop_cachedlookup = slsfs_lookup,
	.vop_close = slsfs_close,
	.vop_create = slsfs_create,
	.vop_getattr = slsfs_getattr,
	.vop_inactive = slsfs_inactive,
	.vop_ioctl = slsfs_ioctl,
	.vop_link = slsfs_link,
	.vop_lookup = vfs_cache_lookup,
	.vop_pathconf = slsfs_pathconf,
	.vop_markatime = slsfs_markatime,
	.vop_mkdir = slsfs_mkdir,
	.vop_mknod = slsfs_mknod,
	.vop_open = slsfs_open,
	.vop_poll = vop_stdpoll,
	.vop_print = slsfs_print,
	.vop_readdir = slsfs_readdir,
	.vop_readlink = slsfs_readlink,
	.vop_reclaim = slsfs_reclaim,
	.vop_remove = slsfs_remove,
	.vop_rename = slsfs_rename,
	.vop_rmdir = slsfs_rmdir,
	.vop_setattr = slsfs_setattr,
	.vop_strategy = slsfs_strategy,
	.vop_symlink = slsfs_symlink,
	.vop_whiteout = VOP_PANIC, // TODO
};

struct vop_vector slsfs_wal_vnodeops = {
	.vop_default = &default_vnodeops,
	.vop_fsync = slsfs_wal_fsync,
	.vop_read = slsfs_wal_read,
	.vop_reallocblks = VOP_PANIC, // TODO
	.vop_write = slsfs_wal_write,
	.vop_access = slsfs_access,
	.vop_bmap = slsfs_wal_bmap,
	.vop_cachedlookup = slsfs_lookup,
	.vop_close = slsfs_close,
	.vop_create = slsfs_create,
	.vop_getattr = slsfs_getattr,
	.vop_inactive = slsfs_inactive,
	.vop_ioctl = slsfs_ioctl,
	.vop_link = slsfs_link,
	.vop_lookup = vfs_cache_lookup,
	.vop_pathconf = slsfs_pathconf,
	.vop_markatime = slsfs_markatime,
	.vop_mkdir = slsfs_mkdir,
	.vop_mknod = slsfs_mknod,
	.vop_open = slsfs_open,
	.vop_poll = vop_stdpoll,
	.vop_print = slsfs_print,
	.vop_readdir = slsfs_readdir,
	.vop_readlink = slsfs_readlink,
	.vop_reclaim = slsfs_reclaim,
	.vop_remove = slsfs_remove,
	.vop_rename = slsfs_rename,
	.vop_rmdir = slsfs_rmdir,
	.vop_setattr = slsfs_setattr,
	.vop_strategy = slsfs_wal_strategy,
	.vop_symlink = slsfs_symlink,
	.vop_whiteout = VOP_PANIC, // TODO
};

struct vop_vector slsfs_sas_vnodeops = {
	.vop_default = &default_vnodeops,
	.vop_fsync = VOP_PANIC,
	.vop_read = VOP_PANIC,
	.vop_reallocblks = VOP_PANIC, // TODO
	.vop_write = VOP_PANIC,
	.vop_access = slsfs_access,
	.vop_bmap = VOP_PANIC,
	.vop_cachedlookup = slsfs_lookup,
	.vop_close = slsfs_close,
	.vop_create = slsfs_create,
	.vop_getattr = slsfs_getattr,
	.vop_inactive = slsfs_inactive,
	.vop_ioctl = slsfs_sas_ioctl,
	.vop_link = slsfs_link,
	.vop_lookup = vfs_cache_lookup,
	.vop_pathconf = slsfs_pathconf,
	.vop_markatime = slsfs_markatime,
	.vop_mkdir = VOP_PANIC,
	.vop_mknod = VOP_PANIC,
	.vop_open = slsfs_open,
	.vop_poll = VOP_PANIC,
	.vop_print = slsfs_print,
	.vop_readdir = VOP_PANIC,
	.vop_readlink = VOP_PANIC,
	.vop_reclaim = slsfs_reclaim,
	.vop_remove = slsfs_remove,
	.vop_rename = slsfs_rename,
	.vop_rmdir = VOP_PANIC,
	.vop_setattr = slsfs_setattr,
	.vop_strategy = VOP_PANIC,
	.vop_symlink = slsfs_symlink,
	.vop_whiteout = VOP_PANIC, // TODO
};
