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
#include <sys/module.h>
#include <sys/mount.h>
#include <sys/namei.h>
#include <sys/priv.h>
#include <sys/proc.h>
#include <sys/stat.h>
#include <sys/syscallsubr.h>
#include <sys/ucred.h>
#include <sys/unistd.h>
#include <sys/vnode.h>

#include <vm/vm.h>
#include <vm/vm_extern.h>
#include <vm/vnode_pager.h>

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

/* 5 GiB */
static const size_t MAX_WAL_SIZE = 5368709000;
static size_t wal_space_used = 0;

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
	struct slos_node *svp;
	struct vnode *vp = args->a_vp;

	if (vp == slos.slsfs_inodes) {
		DEBUG("Special vnode trying to be reclaimed");
	}

	if (vp->v_type != VCHR) {
	  svp = SLSVP(vp);
		cache_purge(vp);
    if (vp != slos.slsfs_inodes)
		  vfs_hash_remove(vp);

    if (svp != NULL)
		  slos_vpfree(svp->sn_slos, svp);
	}

	vp->v_data = NULL;
  vnode_destroy_vobject(vp);

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
	error = slsfs_lookupbln(svp, lbn, &ptr);
	if (error != 0)
		return (error);

	/* Turn everything into sector size blocks.*/
	extlbn = lbn;
	extbn = ptr.offset;
	extsize = ptr.size / fsbsize;

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
	/* size_t cksize; */
	/* uint32_t cksum, check; */
	/* int error; */
	/* struct fbtree *tree = &slos.slos_cktree->sn_tree; */
	/* uint64_t blk = bp->b_blkno; */
	/* size_t size = 0; */

	/* MPASS((bp->b_bcount % BLKSIZE(&slos)) == 0); */

	/* while (size < bp->b_bcount) { */
	/* 	cksize = min(PAGE_SIZE, bp->b_bcount - size); */
	/* 	cksum = calculate_crc32c(~0, bp->b_data + size, cksize); */
	/* 	size += cksize; */
	/* 	blk++; */
	/* 	error = fbtree_get(tree, &blk, &check); */
	/* 	if (error == EINVAL) { */
	/* 		return 0; */
	/* 	} else if (error) { */
	/* 		panic("Problem with read cksum %d", error); */
	/* 	} */

	/* 	if (check != cksum) { */
	/* 		printf("%lu, %lu, %lu", blk, cksize, bp->b_bcount); */
	/* 		return EINVAL; */
	/* 	} */
	/* } */
	return (0);
}

static int
slsfs_update_cksum(struct buf *bp)
{
	/* size_t cksize; */
	/* uint32_t cksum; */
	/* struct fnode_iter iter; */
	/* int error = 0; */

	/* struct fbtree *tree = &slos.slos_cktree->sn_tree; */
	/* uint64_t blk = bp->b_blkno; */
	/* size_t size = 0; */
	/* while (size < bp->b_bcount) { */
	/* 	cksize = min(PAGE_SIZE, bp->b_bcount - size); */
	/* 	cksum = calculate_crc32c(~0, bp->b_data + size, cksize); */
	/* 	size += cksize; */
	/* 	error = fbtree_keymin_iter(tree, &blk, &iter); */
	/* 	KASSERT(error == 0, ("error %d by fbtree_keymin_iter", error)); */
	/* 	if (ITER_ISNULL(iter) || ITER_KEY_T(iter, uint64_t) != blk) { */
	/* 		error = fnode_insert(iter.it_node, &blk, &cksum); */
	/* 	} else { */
	/* 		fiter_replace(&iter, &cksum); */
	/* 	} */
	/* 	if (error) { */
	/* 		panic("Issue with updating checksum tree %d", error); */
	/* 	} */
	/* 	blk++; */
	/* } */
	/* return (0); */
  return 0;
}

int
slsfs_cksum(struct buf *bp)
{
	/* int error; */
	/* struct fbtree *tree = &slos.slos_cktree->sn_tree; */

	/* if (bp->b_data == unmapped_buf || */
	/*     (bp->b_vp == slos.slos_cktree->sn_fdev) || */
	/*     slos.slos_sb->sb_epoch == EPOCH_INVAL) { */
	/* 	return 0; */
	/* } */

	/* switch (bp->b_iocmd) { */
	/* case BIO_READ: */
	/* 	BTREE_LOCK(tree, LK_SHARED); */
	/* 	error = slsfs_check_cksum(bp); */
	/* 	BTREE_UNLOCK(tree, 0); */

	/* 	return (error); */
	/* case BIO_WRITE: */
	/* 	BTREE_LOCK(tree, LK_EXCLUSIVE); */
	/* 	error = slsfs_update_cksum(bp); */
	/* 	BTREE_UNLOCK(tree, 0); */

	/* 	return (error); */
	/* default: */
	/* 	panic( */
	/* 	    "Unknown buffer IO command %d for bp %p", bp->b_iocmd, bp); */
	/* }; */

	/* return (-1); */
  return 0;
}

static int
slsfs_strategy(struct vop_strategy_args *args)
{
	int error;
	diskptr_t ptr;
	struct buf *bp = args->a_bp;
	struct vnode *vp = args->a_vp;
  struct slos_node *svp = SLSVP(vp);
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
    error = slsfs_lookupbln(svp, bp->b_lblkno, &ptr);
    if (error) {
      printf("Could not find the block!\n");
    }
    MPASS(error == 0);
    if (bp->b_iocmd == BIO_WRITE) {
      /* This are on disk is marked as cow, or have not been allocated a block */
      if ((ptr.epoch < slos.slos_sb->sb_epoch) || (ptr.offset == 0)) {
        /* Allocate a new block */
        error = slos_blkalloc(&slos, ptr.size, &ptr);
        MPASS(error == 0);
        /* Update the vtree with this value */
        vtree_insert(&svp->sn_vtree, bp->b_lblkno, &ptr);
        MPASS(error == 0);
        printf("Copy on write for %p at %lu\n", vp, bp->b_lblkno);
      } else {
        printf("Not COW for %p at %lu\n", vp, bp->b_lblkno);
      }


      atomic_add_64(
          &slos.slos_sb->sb_data_synced, bp->b_bcount);
      bp->b_blkno = ptr.offset;
    };
  } else {
    printf("Write for btree at %lu\n", bp->b_lblkno);
    bp->b_blkno = bp->b_lblkno;
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

  g_vfs_strategy(&slos.slos_vp->v_bufobj, bp);

	return (0);

/* error: */
/* 	bp->b_error = error; */
/* 	bp->b_ioflags |= BIO_ERROR; */
/* 	bufdone(bp); */

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

		error = slos_truncate(vp, vap->va_size);
		if (error) {
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
  panic("NOT IMPLEMENTED");
	return (0);
}

/* Fill in the slos_extent structures with the file's extents. */
static int
slsfs_getextents(struct slos_node *svp, struct slos_extent *extents)
{
  panic("NOT IMPLEMENTED");
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
