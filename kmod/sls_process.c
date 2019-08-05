#include <sys/types.h>

#include <sys/conf.h>
#include <sys/kernel.h>
#include <sys/limits.h>
#include <sys/malloc.h>
#include <sys/module.h>
#include <sys/param.h>
#include <sys/pcpu.h>
#include <sys/proc.h>
#include <sys/ptrace.h>
#include <sys/queue.h>
#include <sys/rwlock.h>
#include <sys/shm.h>
#include <sys/signalvar.h>
#include <sys/systm.h>
#include <sys/syscallsubr.h>
#include <sys/time.h>

#include <machine/reg.h>

#include <vm/vm.h>
#include <vm/pmap.h>
#include <vm/vm_extern.h>
#include <vm/vm_map.h>
#include <vm/vm_object.h>
#include <vm/vm_page.h>
#include <vm/vm_radix.h>
#include <vm/uma.h>

#include "sls.h"
#include "slsmm.h"
#include "sls_data.h"
#include "sls_dump.h"
#include "sls_process.h"


static struct sls_process *
slsp_init(pid_t pid)
{
    struct sls_process *procnew;
    struct slsp_list *bucket;

    printf("Creating slsp for %ld\n", (long) pid);
    bucket = &slsm.slsm_proctable[pid & slsm.slsm_procmask];

    procnew = malloc(sizeof(*procnew), M_SLSMM, M_WAITOK);
    procnew->slsp_pid = pid;
    procnew->slsp_vm = NULL;
    procnew->slsp_charge = 0;
    procnew->slsp_status = 0;
    procnew->slsp_epoch = 0;
    procnew->slsp_ckptbuf = sbuf_new_auto();
    procnew->slsp_refcount = 1;
    if (procnew->slsp_ckptbuf == NULL) {
	free(procnew, M_SLSMM);
	return NULL;
    }
    
    LIST_INSERT_HEAD(bucket, procnew, slsp_procs);
    printf("Returning the slsp\n");

    return procnew;
}

struct sls_process *
slsp_add(pid_t pid)
{
    struct sls_process *slsp;

    /* 
     * Try to find if we already  have added the process to
     * the SLS, if so we can't add it again.
     */
    slsp = slsp_find(pid);
    if (slsp != NULL) {
	/* We got a reference to the process with slsp_find, release it. */
	slsp_deref(slsp);
	return NULL;
    }

    return slsp_init(pid);
}

void
slsp_fini(struct sls_process *slsp)
{
    
    LIST_REMOVE(slsp, slsp_procs);

    if (slsp->slsp_ckptbuf != NULL) {
	if (sbuf_done(slsp->slsp_ckptbuf) == 0)
	    sbuf_finish(slsp->slsp_ckptbuf);

	sbuf_delete(slsp->slsp_ckptbuf);
    }

    if (slsp->slsp_vm != NULL)
	vmspace_free(slsp->slsp_vm);

}

void
slsp_del(pid_t pid)
{
    struct sls_process *slsp;
    struct slsp_list *bucket;

    bucket = &slsm.slsm_proctable[pid & slsm.slsm_procmask];
    
    LIST_FOREACH(slsp, bucket, slsp_procs) {
	if (slsp->slsp_pid == pid) {
	    slsp_fini(slsp);
	    return;
	}
    }
}


static void
slsp_list(void) {
	u_long hashmask;
	struct slsp_list *bucket;
	struct sls_process *slsp;
	int i;

	hashmask = slsm.slsm_procmask;
	for (i = 0; i <= hashmask; i++) {
		bucket = &slsm.slsm_proctable[i];
		LIST_FOREACH(slsp, bucket, slsp_procs) {
			printf("Bucket %d, PID %ld\n", i, (long) slsp->slsp_pid);
		}
	}

}

struct sls_process *
slsp_find(pid_t pid)
{
    struct sls_process *slsp;
    struct slsp_list *bucket;

    bucket = &slsm.slsm_proctable[pid & slsm.slsm_procmask];

    LIST_FOREACH(slsp, bucket, slsp_procs) {
	if (slsp->slsp_pid == pid) {
	    /* Get a reference to the process and return it. */
	    slsp_ref(slsp);
	    return slsp;
	}
    }

    return NULL;
}

void
slsp_delall(void)
{
	u_long hashmask;
	struct slsp_list *bucket;
	struct sls_process *slsp;
	int i;

	/* If we never completed initialization, abort*/
	if (slsm.slsm_proctable == NULL)
	    return;

	hashmask = slsm.slsm_procmask;
	for (i = 0; i <= hashmask; i++) {
		bucket = &slsm.slsm_proctable[i];
		while (!LIST_EMPTY(bucket)) {
			slsp = LIST_FIRST(bucket);
			printf("Removing snapshots of process %ld\n", (long) slsp->slsp_pid);
			LIST_REMOVE(slsp, slsp_procs);
			slsp_fini(slsp);
		}
	}
}

void
slsp_ref(struct sls_process *slsp)
{
	atomic_add_int(&slsp->slsp_refcount, 1);
}

void
slsp_deref(struct sls_process *slsp)
{
	atomic_add_int(&slsp->slsp_refcount, -1);

	if (slsp->slsp_refcount == 0)
	    slsp_fini(slsp);

}
