#include "slsmm.h"

#include <sys/param.h>
#include <sys/conf.h>
#include <sys/uio.h>
#include <sys/rwlock.h>
#include <sys/malloc.h>
#include <sys/module.h>
#include <sys/kernel.h>
#include <sys/systm.h>
#include <sys/filio.h>

#include <machine/vmparam.h>

#include <vm/vm.h>
#include <vm/vm_object.h>
#include <vm/vm_page.h>
#include <vm/vm_pager.h>

static vm_paddr_t
slsmap_mem_ofstophys(vm_ooffset_t offset)
{
    return offset;
}

struct slsmm_vm_handle_t {
    struct cdev *dev;
};

static int
slsmm_dev_pager_ctor(void *handle, vm_ooffset_t size, vm_prot_t prot,
        vm_ooffset_t foff, struct ucred *cred, u_short *color)
{
    printf("page ctor\n");
    struct slsmm_vm_handle_t *vmh = handle;

    if (color) *color = 0;
    dev_ref(vmh->dev);
    return 0;
}

static void
slsmm_dev_pager_dtor(void *handle)
{
    printf("page dtor\n");
    struct slsmm_vm_handle_t *vmh = handle;
    struct cdev *dev = vmh->dev;
    free(vmh, M_DEVBUF);
    dev_rel(dev);
}

static int
slsmm_dev_pager_fault(vm_object_t object, vm_ooffset_t offset, int prot, 
        vm_page_t *mres)
{
    printf("page fault\n");
    vm_memattr_t memattr = object->memattr;
    vm_pindex_t pidx = OFF_TO_IDX(offset);
    vm_paddr_t paddr =  slsmap_mem_ofstophys(offset);
    vm_page_t page;
    if (paddr == 0) return (VM_PAGER_FAIL);

    if (((*mres)->flags & PG_FICTITIOUS) != 0) {
        page = *mres;
        vm_page_updatefake(page, paddr, memattr);
    } else {
        
#ifndef VM_OBJECT_WUNLOCK	/* FreeBSD < 10.x */
#define VM_OBJECT_WUNLOCK VM_OBJECT_UNLOCK
#define VM_OBJECT_WLOCK	VM_OBJECT_LOCK
#endif
        VM_OBJECT_WUNLOCK(object);
		page = vm_page_getfake(paddr, memattr);
		VM_OBJECT_WLOCK(object);
		vm_page_lock(*mres);
		vm_page_free(*mres);
		vm_page_unlock(*mres);
		*mres = page;
		vm_page_insert(page, object, pidx);
    }
    page->valid = VM_PAGE_BITS_ALL;

    return  (VM_PAGER_OK);
}

static struct cdev_pager_ops slsmm_cdev_pager_ops = {
    .cdev_pg_ctor = slsmm_dev_pager_ctor,
    .cdev_pg_dtor = slsmm_dev_pager_dtor,
    .cdev_pg_fault = slsmm_dev_pager_fault,
};

static int
slsmm_mmap_single(struct cdev *cdev, vm_ooffset_t *foff, vm_size_t objsize, 
        vm_object_t *objp, int prot)
{
    struct slsmm_vm_handle_t *vmh;
    vm_object_t obj;

    vmh = malloc(sizeof(struct slsmm_vm_handle_t), M_DEVBUF, M_NOWAIT | M_ZERO);
    if (vmh == NULL) return ENOMEM;
    obj = cdev_pager_allocate(vmh, OBJT_DEVICE, &slsmm_cdev_pager_ops, objsize, 
            prot, *foff, NULL);
    if (obj == NULL) {
        free(vmh, M_DEVBUF);
        return EINVAL;
    }

    *objp = obj;
    return 0;

    return 0;
}

//=============================================================================

static struct cdev *zero_dev;

static d_write_t null_write;
static d_ioctl_t zero_ioctl;
static d_read_t zero_read;

static struct cdevsw zero_cdevsw = {
    .d_version =  D_VERSION,
    .d_read =	zero_read,
    .d_write =	null_write,
    .d_ioctl =	zero_ioctl,
    .d_name =	"slsmm",
    .d_flags =	D_MMAP_ANON,
    .d_mmap_single = slsmm_mmap_single,
};

static int zero_read(struct cdev *dev __unused, struct uio *uio, int flags __unused) {
    printf("called zero_read\n");
    void *zbuf;
    ssize_t len;
    int error = 0;

    KASSERT(uio->uio_rw == UIO_READ,
            ("Can't be in %s for write", __func__));
    zbuf = __DECONST(void *, zero_region);
    while (uio->uio_resid > 0 && error == 0) {
        len = uio->uio_resid;
        if (len > ZERO_REGION_SIZE)
            len = ZERO_REGION_SIZE;
        error = uiomove(zbuf, len, uio);
    }

    return (error);
}

static char single;

static int zero_ioctl(struct cdev *dev __unused, u_long cmd, caddr_t data __unused,
        int flags __unused, struct thread *td) {
    printf("ioctl\n");
    int error;
    error = 0;

    switch (cmd) {
        case SLSMM_WRITE:
            single = *(char *)data;
            break;
        case SLSMM_READ:
            *(char *)data = single;
        case FIONBIO:
            break;
        case FIOASYNC:
            if (*(int *)data != 0)
                error = EINVAL;
            break;
        default:
            error = ENOIOCTL;
    }
    return (error);
}

static int null_write(struct cdev *dev __unused, struct uio *uio, int flags __unused) {
    printf("called null_write\n");
    uio->uio_resid = 0;

    return (0);
}

static int SLSMMHandler(struct module *inModule, int inEvent, void *inArg) {
    switch (inEvent) {
        case MOD_LOAD:
            uprintf("Load\n");
            zero_dev = make_dev_credf(MAKEDEV_ETERNAL_KLD, &zero_cdevsw, 0,
                    NULL, UID_ROOT, GID_WHEEL, 0666, "slsmm");
            return 0;

        case MOD_UNLOAD:
            uprintf("Unload\n");
            destroy_dev(zero_dev);
            return 0;

        default:
            return (EOPNOTSUPP);
    }
}

static moduledata_t moduleData = {
    "slsmm",
    SLSMMHandler,
    NULL
};

DECLARE_MODULE(slsmm_kmod, moduleData, SI_SUB_DRIVERS, SI_ORDER_MIDDLE);
