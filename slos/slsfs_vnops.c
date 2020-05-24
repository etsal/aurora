#include <sys/types.h>
#include <sys/filio.h>
#include <sys/param.h>
#include <sys/ktr.h>
#include <sys/kernel.h>
#include <sys/module.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <sys/vnode.h>
#include <sys/dirent.h>
#include <sys/namei.h>
#include <sys/bio.h>
#include <geom/geom_vfs.h>

#include <vm/vm.h>
#include <vm/vm_extern.h>
#include <vm/vnode_pager.h>

#include "../kmod/sls_internal.h"
#include <slsfs.h>
#include <slos.h>
#include <slos_inode.h>
#include <slos_record.h>
#include <slos_btree.h>
#include <slos_io.h>
#include <slos_record.h>

#include "slsfs_dir.h"
#include "slsfs_subr.h"
#include "slsfs_buf.h"

SDT_PROVIDER_DEFINE(slos);
SDT_PROBE_DEFINE3(slos, , , slsfs_deviceblk, "uint64_t", "uint64_t", "int");
SDT_PROBE_DEFINE3(slos, , , slsfs_vnodeblk, "uint64_t", "uint64_t", "int");

extern struct slos slos;

static int
slsfs_inactive(struct vop_inactive_args *args)
{
	int error = 0;
	struct vnode *vp = args->a_vp;
	struct slos_node *svp = SLSVP(vp);

	if (svp->sn_status == SLOS_VDEAD) {
		error = slsfs_truncate(vp, 0);
		slsfs_destroy_node(svp);
		vrecycle(vp);
	}

	return (error);
}

static int
slsfs_getattr(struct vop_getattr_args *args)
{
	struct vnode *vp = args->a_vp;
	struct vattr *vap = args->a_vap;
	struct slos_node *slsvp = SLSVP(vp);
	struct slsfsmount *smp = TOSMP(vp->v_mount);
	DBUG("GET Attr on Vnode %lu\n", slsvp->sn_pid);

	VATTR_NULL(vap);
	vap->va_type = IFTOVT(slsvp->sn_ino.ino_mode);
	vap->va_mode = slsvp->sn_ino.ino_mode & ~S_IFMT;
	vap->va_nlink = slsvp->sn_ino.ino_nlink;
	vap->va_uid = 0;
	vap->va_gid = 0;
	vap->va_fsid = VNOVAL;
	vap->va_fileid = slsvp->sn_pid;
	vap->va_blocksize = smp->sp_sdev->devblocksize;
	vap->va_size = slsvp->sn_ino.ino_size;

	vap->va_atime.tv_sec = slsvp->sn_ino.ino_mtime;
	vap->va_atime.tv_nsec = slsvp->sn_ino.ino_mtime_nsec;

	vap->va_mtime.tv_sec = slsvp->sn_ino.ino_mtime;
	vap->va_mtime.tv_nsec = slsvp->sn_ino.ino_mtime_nsec;

	vap->va_ctime.tv_sec = slsvp->sn_ino.ino_ctime;
	vap->va_ctime.tv_nsec = slsvp->sn_ino.ino_ctime_nsec;

	vap->va_birthtime.tv_sec = 0;
	vap->va_gen = 0;
	vap->va_flags = slsvp->sn_ino.ino_flags;
	vap->va_rdev = NODEV;
	vap->va_bytes = slsvp->sn_ino.ino_asize;
	vap->va_filerev = 0;
	vap->va_vaflags = 0;
	DBUG("Done GET Attr on Vnode %lu\n", slsvp->sn_pid);

	return (0);
}


static int
slsfs_reclaim(struct vop_reclaim_args *args)
{
	struct vnode *vp = args->a_vp;
	struct slos_node *svp = SLSVP(vp);

	DBUG("Reclaiming vnode %p\n", vp);

	if (vp == slos.slsfs_inodes) {
		DBUG("Special vnode trying to be reclaimed\n");
	}

	slos_vpfree(svp->sn_slos, svp);

	vp->v_data = NULL;
	cache_purge(vp);
	vnode_destroy_vobject(vp);
	if (vp->v_vflag & VV_SYSTEM) {
		vfs_hash_remove(vp);
	}
	DBUG("Done reclaiming vnode %p\n", vp);

	return (0);
}

static int
slsfs_mkdir(struct vop_mkdir_args *args)
{
	struct vnode *dvp = args->a_dvp;
	struct vnode **vpp = args->a_vpp;
	struct componentname *name = args->a_cnp;
	struct vattr *vap = args->a_vap;

	struct ucred creds;
	struct vnode *vp;
	int error;

	int mode = MAKEIMODE(vap->va_type, vap->va_mode);
	error = SLS_VALLOC(dvp, mode, &creds, &vp);
	if (error) {
		*vpp = NULL;
		return (error);
	} 

	DBUG("Initing Directory named %s\n", name->cn_nameptr);
	error = slsfs_init_dir(dvp, vp, name);
	if (error) {
		DBUG("Issue init directory\n");
		*vpp = NULL;
		return (error);
	}
	*vpp = vp;

	return (0);
}

static int
slsfs_accessx(struct vop_accessx_args *args)
{
	return (0);
}

static int
slsfs_open(struct vop_open_args *args)
{
	struct vnode *vp = args->a_vp;
	struct slos_node *slsvp = SLSVP(vp);
	vnode_create_vobject(vp, SLSVPSIZ(slsvp), args->a_td);

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

	DBUG("READING DIRECTORY %lu\n", filesize);
	if ((io->uio_offset < filesize) &&
	    (io->uio_resid >= sizeof(struct dirent)))
	{
		diroffset = io->uio_offset;
		blkno = io->uio_offset / blksize;
		blkoff = io->uio_offset % blksize;
		error = slsfs_bread(vp, blkno, blksize, curthread->td_ucred, 
		    &bp);
		if (error) {
			brelse(bp);
			DBUG("Problem reading from blk in readdir\n");
			return (error);
		}
		/* Create the UIO for the disk. */
		while (diroffset < filesize) {
			DBUG("dir offet %lu\n", diroffset);
			anyleft = ((diroffset % blksize) + sizeof(struct dirent)) > blksize;
			if (anyleft) {
				blkoff = 0;
				blkno++;
				diroffset = blkno * blksize;
				brelse(bp);
				error = slsfs_bread(vp, blkno, blksize, 
				    curthread->td_ucred, &bp);
				if (error) {
					brelse(bp);
					return (error);
				}
			}
			if (buf_mapped(bp)) {
				KASSERT(bp->b_bcount > blkoff, ("Blkoff out of range of buffer\n"));
				dir = *((struct dirent *)(bp->b_data + blkoff));
				if (dir.d_reclen == 0) {
					break;
				}
				dir.d_reclen = GENERIC_DIRSIZ(&dir);
				dirent_terminate(&dir);
				if (io->uio_resid < GENERIC_DIRSIZ(&dir)) {
					break;
				}
				DBUG("%s\n", dir.d_name);
				error = uiomove(&dir, dir.d_reclen, io);
				if (error) {
					DBUG("Problem moving buffer\n");
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
	char * name;
	int error = 0;
	struct vnode *vp = NULL;
	*vpp = NULL;

	name = cnp->cn_nameptr;
	namelen = cnp->cn_namelen;
	nameiop =  cnp->cn_nameiop;
	islastcn = cnp->cn_flags & ISLASTCN;

	/* Self directory - Must just increase reference count of dir */
	if((namelen == 1) && (name[0] == '.')) {
		VREF(dvp);
		*vpp = dvp;
		/* Check another case of the ".." directory */
	} else if (cnp->cn_flags & ISDOTDOT){
		struct componentname tmp;
		tmp.cn_nameptr = ".."; 
		tmp.cn_namelen = 2;
		error = slsfs_lookup_name(dvp, &tmp, &dir);
		/* Record was not found */
		if (error)
			goto out;

		error = SLS_VGET(dvp, dir.d_fileno, LK_EXCLUSIVE,  &vp);
		if (!error) {
			*vpp = vp;
		}
	} else {
		DBUG("Looking up file %s\n", cnp->cn_nameptr);
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
				DBUG("Regular name lookup - not found\n");
				cnp->cn_flags |= SAVENAME;
				error = EJUSTRETURN;
			} 
		} else if (error == 0) {
			/* Cases for when name is found, others to be filled in 
			 * later */
			if ((nameiop == DELETE) && islastcn) {
				DBUG("Delete of file %s\n", cnp->cn_nameptr);
				error = SLS_VGET(dvp, dir.d_fileno, 
				    LK_EXCLUSIVE, &vp);
				if (!error) {
					cnp->cn_flags |= SAVENAME;
					*vpp = vp;
				}
			} else {
				DBUG("Lookup of file %s\n", cnp->cn_nameptr);
				error = SLS_VGET(dvp, dir.d_fileno, 
				    LK_EXCLUSIVE, &vp);
				if (!error) {
					*vpp = vp;
				}
			}
		} else {
			DBUG("ERROR IN LOOKUP %d\n", error);
			return (error);
		}
	}

out:
	// Cache the entry in the name cache for the future 
	if((cnp->cn_flags & MAKEENTRY) != 0) {
		cache_enter(dvp, *vpp, cnp);
	}
	return (error);
}

static int
slsfs_rmdir(struct vop_rmdir_args *args)
{
	DBUG("Removing directory\n");
	struct vnode *vp = args->a_vp;
	struct vnode *dvp = args->a_dvp;
	struct componentname *cnp = args->a_cnp;
	int error;

	struct slos_node *svp = SLSVP(vp);
	struct slos_inode *ivp = &SLSINO(svp);

	/* Check if directory is empty */
	/* Are we mounted here*/
	if (ivp->ino_nlink > 2) {
		return (ENOTEMPTY);
	}

	if (vp->v_vflag & VV_ROOT) {
		return (EPERM);
	}

	error = slsfs_remove_node(dvp, vp, cnp);
	if (error) {
		return (error);
	}

	cache_purge(dvp);
	cache_purge(vp);
	DBUG("Removing directory done\n");

	return (0);

}

static int
slsfs_create(struct vop_create_args *args)
{
	DBUG("Creating file\n");
	struct vnode *dvp = args->a_dvp;
	struct vnode **vpp = args->a_vpp;
	struct componentname *name = args->a_cnp;
	struct vattr *vap = args->a_vap;

	struct ucred creds;
	struct vnode *vp;
	int error;

	int mode = MAKEIMODE(vap->va_type, vap->va_mode);
	error = SLS_VALLOC(dvp, mode, &creds, &vp);
	if (error) {
		*vpp = NULL;
		return (error);
	} 

	error = slsfs_add_dirent(dvp, VINUM(vp), name->cn_nameptr,
	    name->cn_namelen, DT_REG);
	if (error == -1) {
		return (EIO);
	}

	SLSVP(dvp)->sn_ino.ino_nlink += 1;

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

	DBUG("Removing file %s\n", cnp->cn_nameptr);

	error = slsfs_remove_node(dvp, vp, cnp);
	if (error) {
		return (error);
	}

	SLSVP(dvp)->sn_ino.ino_nlink -= 1;
	cache_purge(dvp);

	DBUG("Removing file\n");

	return (0);
}

static int
slsfs_write(struct vop_write_args *args)
{
	struct buf *bp;
	size_t xfersize, filesize;
	uint64_t off;
	int error = 0;

	struct vnode *vp = args->a_vp;
	struct slos_node *svp = SLSVP(vp);
	size_t blksize = IOSIZE(svp);
	struct uio *uio = args->a_uio;
	int ioflag = args->a_ioflag;

	filesize =  svp->sn_ino.ino_size;

	// Check if full
	if (uio->uio_offset < 0) {
		DBUG("Offset write at %lx\n", uio->uio_offset);
		return (EINVAL);
	}
	if (uio->uio_resid == 0) {
		DBUG("Write of no buff\n");
		return (0);
	}
	switch(vp->v_type) {
	case VREG:
		break;
	case VDIR:
		return (EISDIR);
	case VLNK:
		break;
	default:
		panic("bad file type");
	}

	if (ioflag & IO_APPEND) {
		uio->uio_offset = filesize;
	}

	if (uio->uio_offset + uio->uio_resid > filesize)  {
		svp->sn_ino.ino_size = uio->uio_offset + uio->uio_resid;
		vnode_pager_setsize(vp, svp->sn_ino.ino_size);
	}

	int modified = 0;

	if (ioflag & IO_DIRECT) {
		DBUG("direct\n");
	}

	if (ioflag & IO_SYNC) {
		DBUG("sync\n");
	}

	while(uio->uio_resid) {
		// Grab the key thats closest to offset, but not over it
		// Mask out the lower order bits so we just have the block;
		error = slsfs_retrieve_buf(vp, uio->uio_offset, uio->uio_resid, &bp);
		if (error) {
			DBUG("Problem getting buffer for write %d\n", error);
			return (error);
		}

		off = uio->uio_offset - (bp->b_lblkno * blksize);
		KASSERT(off < bp->b_bcount, ("Offset should inside buf, %p", bp));
		xfersize = omin(uio->uio_resid, bp->b_bcount - off);

		KASSERT(xfersize != 0, ("No 0 uio moves slsfs write"));
		KASSERT(xfersize <= uio->uio_resid, ("This should neveroccur"));
		uiomove((char *)bp->b_data + off, xfersize, uio);
		/* One thing thats weird right now is our inodes and meta data 
		 * is currently not
		 * in the buf cache, so we don't really have to worry about 
		 * dirtying those buffers,
		 * but later we will have to dirty them.
		 */
		slsfs_bdirty(bp);
		modified++;
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
	int error = 0;

	struct vnode *vp = args->a_vp;
	struct slos_node *svp = SLSVP(vp);
	size_t blksize = IOSIZE(svp);
	struct uio *uio = args->a_uio;

	svp = SLSVP(vp);
	sivp = &SLSINO(svp);
	filesize =  sivp->ino_size;

	// Check if full
	if (uio->uio_offset < 0)
		return (EINVAL);
	if (uio->uio_resid == 0)
		return (0);

	DBUG("Reading filesize %lu - %lu\n", SLSVP(vp)->sn_pid, filesize);
	if (uio->uio_offset >= filesize) {
		return (0);
	}

	resid = omin(uio->uio_resid, (filesize - uio->uio_offset));
	DBUG("READING global off %lu, global size %lu\n", uio->uio_offset, uio->uio_resid); 
	while(resid) {
		error = slsfs_retrieve_buf(vp, uio->uio_offset, uio->uio_resid, &bp);
		if (error) {
			DBUG("Problem getting buffer for write %d\n", error);
			return (error);
		}

		off = uio->uio_offset - (bp->b_lblkno * blksize);
		toread = omin(resid, bp->b_bcount - off);
		DBUG("%lu --- %lu, %lu, %p\n", resid, bp->b_bcount, off, vp);

		/* One thing thats weird right now is our inodes and meta data 
		 * is currently not
		 * in the buf cache, so we don't really have to worry about 
		 * dirtying those buffers,
		 * but later we will have to dirty them.
		 */
		DBUG("Reading: Read at bno %lu for vnode %p, read size %lu\n", 
		    bp->b_lblkno, vp, toread);
		DBUG("Relative offset %lu, global off %lu, global size %lu\n", 
		    off, uio->uio_offset, uio->uio_resid); 
		KASSERT(toread != 0, ("Should not occur"));
		error = uiomove((char *)bp->b_data + off, toread, uio);
		if (error) {
			brelse(bp);
			break;
		}
		brelse(bp);
		resid -= toread;
	}

	return (error);
}



static int
slsfs_bmap(struct vop_bmap_args *args)
{
	struct vnode *vp = args->a_vp;
	struct slsfsmount *smp = TOSMP(vp->v_mount);

	if (args->a_bop != NULL)
		*args->a_bop = &smp->sp_slos->slos_vp->v_bufobj;
	if (args->a_bnp != NULL)
		*args->a_bnp = args->a_bn;
	if (args->a_runp != NULL)
		*args->a_runp = 0;
	if (args->a_runb != NULL)
		*args->a_runb = 0;

	/*
	 * We just want to allocate for now, since allocations are persistent 
	 * and get written to disk
	 * (this is obviously very slow), if we want to make this transactional 
	 * we will need to to probably do the ZFS strategy of just having this 
	 * sent the physical block to the logical one
	 * and over write the buf_ops so that allocation occurs on the flush or 
	 * the sync?  How would this interact with checkpointing.  I'm thinking 
	 * we will probably have all the flushes occur
	 * on a checkpoint, or before.
	 *
	 * After discussion, we believe that optimistically flushing would be a 
	 * good idea, as it would reduce the dump time for the checkpoint thus 
	 * reducing latency on packets being help up waiting for the data to be 
	 * dumped to disk. Another issue we face here is that if we allocate on 
	 * each block we turn our extents and larger writes into blocks.  So I 
	 * believe the best thing
	 * to do is do allocation on flush. So we will make our bmap return the 
	 * logical block
	 */
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
		printf("\t(null)\n");
	} else if ((void *)slsvp == (void *)&slos) {
		printf("\tslos\n");
	} else {
		printf("\tslos inode\n");
		printf("\tsn_pid = %ld\n", slsvp->sn_pid);
		printf("\tsn_uid = %ld\n", slsvp->sn_uid);
		printf("\tsn_gid = %ld\n", slsvp->sn_gid);
		printf("\tsn_blk = %ld\n", slsvp->sn_blk);
		printf("\tsn_status = %lx\n", slsvp->sn_status);
		printf("\tsn_refcnt = %ld\n", slsvp->sn_refcnt);
	}

	return (0);
}

static void
adjust_ptr(uint64_t lbln, uint64_t bln, diskptr_t *ptr) 
{
	if (bln == lbln) {
		return;
	}
	
	KASSERT(lbln > bln, ("Should be slightly larger %lu : %lu", lbln, bln));
	uint64_t off = lbln - bln;
	ptr->offset += off;
}

static int
slsfs_strategy(struct vop_strategy_args *args)
{
	int error;
	struct slos_diskptr ptr;

	struct buf *bp = args->a_bp;
	struct vnode *vp = args->a_vp;
	struct fnode_iter iter;

	//FOR BETTER BUF TRACKING
	/*if (bp->b_iocmd == BIO_WRITE) {*/
		/*printf("BIOWRITE : bp(%p), vp(%p:%lu) - %lu:%lu, %lu\n", bp, vp, SLSVP(vp)->sn_pid, bp->b_lblkno, bp->b_blkno, bp->b_iooffset);*/
	/*} else {*/
		/*printf("BIOREAD : bp(%p), vp(%p:%lu) - %lu:%lu, %lu\n", bp, vp, SLSVP(vp)->sn_pid, bp->b_lblkno, bp->b_blkno, bp->b_iooffset);*/
	/*}*/
	
        CTR2(KTR_SPARE5, "slsfs_strategy vp=%p blkno=%x\n", vp, bp->b_lblkno);
	if (vp->v_type != VCHR) {
		KASSERT(bp->b_lblkno != (-1), 
			("No logical block number should be -1 - vnode effect %lu", 
			 SLSVP(vp)->sn_pid));
		error = BTREE_LOCK(&SLSVP(vp)->sn_tree, LK_SHARED);
		if (error) {
			panic("Problem getting lock %d\n", error);
		}
		error = fbtree_keymin_iter(&SLSVP(vp)->sn_tree, &bp->b_lblkno, &iter);
		if (error != 0) {
			return (error);
		}

		if (ITER_ISNULL(iter)) {
			if (iter.it_node->fn_dnode->dn_parent) {
				if (!iter.it_node->fn_parent) {
					fnode_parent(iter.it_node, 
					    &iter.it_node->fn_parent);
				}
				fnode_print(iter.it_node->fn_parent);
			}
			fnode_print(iter.it_node);
			panic("whats %p, %lu, %p", vp, bp->b_lblkno, 
			    iter.it_node);
		}

		if (ITER_KEY_T(iter, uint64_t) != bp->b_lblkno) {
			if (!INTERSECT(iter, bp->b_lblkno, IOSIZE(SLSVP(vp)))) {
				panic("Key not found");
			}
		}

		ptr = ITER_VAL_T(iter, diskptr_t);
		if (ptr.offset != (0))  {
			adjust_ptr(bp->b_lblkno, ITER_KEY_T(iter, uint64_t), &ptr);
			bp->b_blkno = ptr.offset;
		} else if (bp->b_iocmd == BIO_WRITE) {
			error = ALLOCATEBLK(SLSVP(vp)->sn_slos, ptr.size, &ptr);
			if (error) {
				panic("UH OH");
			}

			if (ptr.offset == 0) {
				panic("Uh oh\n");
			}
			error = fbtree_replace(&SLSVP(vp)->sn_tree, ITER_KEY(iter), &ptr);
			if (error) {
				panic("Issue replacing key %d\n", error);
			}
			adjust_ptr(bp->b_lblkno, ITER_KEY_T(iter, uint64_t), &ptr);
			bp->b_blkno = ptr.offset;
		} else {
			bp->b_blkno = (daddr_t) (-1);
			vfs_bio_clrbuf(bp); bufdone(bp);
			ITER_RELEASE(iter);
			return (0);
		}
		ITER_RELEASE(iter);
	} else {
		bp->b_blkno = bp->b_lblkno;
		int change =  bp->b_bufobj->bo_bsize / 
		    slos.slos_vp->v_bufobj.bo_bsize;
		SDT_PROBE3(slos, , , slsfs_deviceblk, bp->b_blkno, 
		    bp->b_bufobj->bo_bsize, change);
	}

	KASSERT(bp->b_blkno != 0, ("Fucking die %p - %p", bp, vp));
	int change =  bp->b_bufobj->bo_bsize / slos.slos_vp->v_bufobj.bo_bsize;
	bp->b_blkno = bp->b_blkno * change;
	bp->b_iooffset = dbtob(bp->b_blkno);

	g_vfs_strategy(&slos.slos_vp->v_bufobj, bp);

	return (0);
}

static int
slsfs_setattr(struct vop_setattr_args *args)
{
	struct vnode *vp = args->a_vp;
	struct vattr *vap = args->a_vap;
	int error = 0;
	if (vap->va_size != (u_quad_t)VNOVAL) {
		error = slsfs_truncate(vp, vap->va_size);
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
	DBUG("Checking path\n");
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
		    0, UIO_SYSSPACE,
		    IO_NODELOCKED | IO_NOMACCHECK, cred, NOCRED, NULL, NULL);
		if (error != 0) {
			DBUG("Error reading a writing %d\n", error);
			break;
		}

		if (dir.d_namlen != 2 || dir.d_name[0] != '.' || dir.d_name[1] 
		    != '.') {
			DBUG("Not a directory\n");
			error = ENOTDIR;
			break;
		}

		if (dir.d_fileno == SLSVP(src)->sn_pid) {
			DBUG("Found within path\n");
			error = EINVAL;
			break;
		}

		if (dir.d_fileno == SLOS_ROOT_INODE) {
			DBUG("Parent is root\n");
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
		DBUG("Vputing target path\n");
		vput(target);
	}
	return (error);
}

static int
slsfs_rename(struct vop_rename_args *args)
{
	struct vnode *tvp = args->a_tvp; // Target Vnode (if it exists)
	struct vnode *tdvp = args->a_tdvp; // Target directory
	struct vnode *fvp = args->a_fvp; // Source vnode
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

	DBUG("Rename or move from %s to %s\n", fname->cn_nameptr, 
	    tname->cn_nameptr);
	// Following nandfs example here -- cross device renaming
	if ((fvp->v_mount != tdvp->v_mount) || (tvp && (fvp->v_mount != 
	    tvp->v_mount))) {
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

	if (fvp == tvp) {
		error = 0;
		goto abort;
		vput(tvp);
	}

	if ((error = vn_lock(fvp, LK_EXCLUSIVE)) != 0) {
		goto abort;
	}

	// Check if the source is a directory and whether we are renaming a 
	// directory
	if ((mode & S_IFMT) == S_IFDIR) {
		int isdot = fname->cn_namelen == 1 && fname->cn_nameptr[0] 
		    =='.';
		int isownparent = fdvp == fvp;
		int isdotdot = (fname->cn_flags | tname->cn_flags) & ISDOTDOT;
		if (isdot || isdotdot || isownparent) {
			VOP_UNLOCK(fvp, 0);
			error = EINVAL;
			goto abort;
		}
		isdir = 1;
		svp->sn_status = SLOS_RENAME;
		oldparent = sdvp->sn_pid;
	}

	vrele(fdvp);

	// Check whether there exists a file that we are replacing
	if (tvp) {
		tnode = SLSVP(tvp);
	}

	// Check parents
	VOP_UNLOCK(fvp, 0);
	if (oldparent != tdnode->sn_pid) {
		newparent = tdnode->sn_pid;
	}

	if (isdir && newparent) {
		DBUG("Checking if directory doens't exist within path\n");
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
		if (isdir && fdvp != tdvp) {
			//XXX LINK STUFF?
		}

		error = slsfs_add_dirent(tdvp, svp->sn_ino.ino_pid, 
		    tname->cn_nameptr,
		    tname->cn_namelen, IFTODT(svp->sn_ino.ino_mode));
		if (error) {
			// XXX LINK STUFF
			goto bad;
		}

		vput(tdvp);
	} else {
		mode = tnode->sn_ino.ino_mode;
		if ((mode & S_IFMT) == S_IFDIR) {
			if (tnode->sn_ino.ino_nlink > 2) {
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
			// Update links??
		}

		vput(tdvp);
		vput(tvp);
	} 

	fname->cn_flags &= ~MODMASK;
	fname->cn_flags |= LOCKPARENT | LOCKLEAF;
	VREF(fdvp);

	error = relookup(fdvp, &fvp, fname);
	if (error) {
		vrele(fdvp);
	}

	if (fvp != NULL) {
		DBUG("fvp != null");
		fnode1 = SLSVP(fvp);
		sdvp = SLSVP(fdvp);
	} else {
		if (isdir) {
			panic("lost dir");
		}
		DBUG("fvp == NULL");
		vrele(args->a_fvp);
		return (0);
	}

	if (fnode1 != svp) {
		DBUG("fnode1 != svp");
		if (isdir) {
			panic("lost dir");
		}
	} else {
		DBUG("fnode1 == svp\n");
		if (isdir && newparent) {
			DBUG("isdir && newparent\n");
		}
		DBUG("Removing dirent\n");
		error = slsfs_unlink_dir(fdvp, fvp, fname);
		svp->sn_status &= ~SLOS_RENAME;
	}

	if (sdvp) {
		vput(fdvp);
	}

	if (svp) {
		vput(fvp);
	}

	vrele(args->a_fvp);

	return (error);
bad:
	if (tnode) {
		vput(tvp);
	}

	vput(tdvp);

	if (isdir) {
		svp->sn_status &= ~SLOS_RENAME;
	}
	if (vn_lock(fvp, LK_EXCLUSIVE) == 0) {
		vput(fvp);
	} else {
		vrele(fvp);
	}

	return (error);
}

/* Seek an extent. Gets the first start of an extent after the offset. */
static int
slsfs_seekextent(struct slos_node *svp, struct uio *uio)
{
	struct fnode_iter iter;
	uint64_t offset;
	uint64_t size;
	uint64_t blocks;
	int error;

	offset = uio->uio_offset / PAGE_SIZE;
	size = 0;

	/* Get btree for vnode */
	BTREE_LOCK(&svp->sn_tree, LK_SHARED);
	error = fbtree_keymax_iter(&svp->sn_tree, &offset, &iter);
	if (error != 0) {
		BTREE_UNLOCK(&svp->sn_tree, 0);
		return (error);
	}

	if (ITER_ISNULL(iter)) {
		uio->uio_offset = EOF;
		uio->uio_resid = 0;
		goto out;
	}

	offset = ITER_KEY_T(iter, uint64_t);
	blocks = (ITER_VAL_T(iter, diskptr_t).size) / PAGE_SIZE;
	KASSERT(blocks != 0, ("zero IO"));

	uio->uio_offset = offset * PAGE_SIZE;
	uio->uio_resid = blocks * PAGE_SIZE;

	for (; !ITER_ISNULL(iter); ITER_NEXT(iter)) {
		if (offset + blocks != ITER_KEY_T(iter, uint64_t))
			break;

		offset = ITER_KEY_T(iter, uint64_t);
		blocks = (ITER_VAL_T(iter, diskptr_t).size) / PAGE_SIZE;

		uio->uio_resid += blocks * PAGE_SIZE;
	}

out:
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
slsfs_ioctl(struct vop_ioctl_args *ap)
{
	struct vnode *vp = ap->a_vp;
	u_long com = ap->a_command;
	struct slos_node *svp = SLSVP(vp);
	struct slos_rstat *st;
	struct uio *uio;

	switch(com) {
	case SLS_SEEK_EXTENT:
		uio = (struct uio *) ap->a_data;
		return (slsfs_seekextent(svp, uio));

	case SLS_SET_RSTAT:
		st = (struct slos_rstat *) ap->a_data;
		return (slsfs_setrstat(svp, st));

	case SLS_GET_RSTAT:
		st = (struct slos_rstat *) ap->a_data;
		return (slsfs_getrstat(svp, st));

	case FIOSEEKDATA: // Fallthrough
	case FIOSEEKHOLE:
		printf("UNSUPPORTED SLSFS IOCTL FIOSEEKDATA/HOLE\n");
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

	SLS_VALLOC(dvp, mode | S_IFLNK, cnp->cn_cred, &vp);

	error = slsfs_add_dirent(dvp, SLSVP(vp)->sn_pid, cnp->cn_nameptr, 
	    cnp->cn_namelen, IFTODT(mode));
	if (error) {
		vput(vp);
		return ENOTDIR;
	}
	len = strlen(ap->a_target);
	error = vn_rdwr(UIO_WRITE, vp, ap->a_target, len, 0, UIO_SYSSPACE, 
	    IO_NODELOCKED | IO_NOMACCHECK,
	    cnp->cn_cred, NOCRED, NULL, NULL);
	if (error) {
		vput(vp);
	}
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

	error = slsfs_add_dirent(tdvp, SLSVP(vp)->sn_pid, cnp->cn_nameptr,
	    cnp->cn_namelen, IFTODT(SLSVP(vp)->sn_ino.ino_mode));

	SLSVP(vp)->sn_ino.ino_nlink++;
	slos_updateroot(SLSVP(vp));

	return (error);
}

struct vop_vector sls_vnodeops = {
	.vop_default =		&default_vnodeops,
	.vop_fsync =		slsfs_fsync, 
	.vop_read =		slsfs_read, 
	.vop_reallocblks =	VOP_PANIC, // TODO
	.vop_write =		slsfs_write,
	.vop_accessx =		slsfs_accessx,
	.vop_bmap =		slsfs_bmap,
	.vop_cachedlookup =	slsfs_lookup, 
	.vop_close =		slsfs_close, 
	.vop_create =		slsfs_create, 
	.vop_getattr =		slsfs_getattr,
	.vop_inactive =		slsfs_inactive,
	.vop_ioctl =		slsfs_ioctl,
	.vop_link =		slsfs_link, 
	.vop_lookup =		vfs_cache_lookup, 
	.vop_markatime =	VOP_PANIC,
	.vop_mkdir =		slsfs_mkdir, 
	.vop_mknod =		VOP_PANIC, // TODO
	.vop_open =		slsfs_open, 
	.vop_poll =		vop_stdpoll,
	.vop_print =		slsfs_print,
	.vop_readdir =		slsfs_readdir,
	.vop_readlink =		slsfs_readlink,
	.vop_reclaim =		slsfs_reclaim,
	.vop_remove =		slsfs_remove,
	.vop_rename =		slsfs_rename,
	.vop_rmdir =		slsfs_rmdir,
	.vop_setattr =		slsfs_setattr,
	.vop_strategy =		slsfs_strategy,
	.vop_symlink =		slsfs_symlink,
	.vop_whiteout =		VOP_PANIC, // TODO
};
