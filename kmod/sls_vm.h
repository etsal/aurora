#ifndef _SLSVM_H_
#define _SLSVM_H_

#include <sys/types.h>

#include <sys/conf.h>
#include <sys/pcpu.h>
#include <sys/proc.h>

#include <vm/pmap.h>
#include <vm/vm.h>
#include <vm/vm_extern.h>
#include <vm/vm_map.h>
#include <vm/vm_object.h>
#include <vm/vm_page.h>
#include <vm/vm_radix.h>
#include <vm/uma.h>

#include "sls_internal.h"
#include "sls_kv.h"

#define OBJT_ISANONYMOUS(obj) \
    ((obj != NULL) && \
     ((obj->type == OBJT_DEFAULT) || \
      (obj->type == OBJT_SWAP)))

int slsvm_object_shadow(struct slskv_table *objtable, vm_object_t *objp);
void slsvm_objtable_collapse(struct slskv_table *objtable);
int slsvm_proc_shadow(struct proc *p, struct slskv_table *table, int is_fullckpt);
int slsvm_procset_shadow(slsset *procset, struct slskv_table *table, int is_fullckpt);
void slsvm_object_reftransfer(vm_object_t src, vm_object_t dst);
void slsvm_object_shadowexact(vm_object_t *objp);
void slsvm_object_copy(struct proc *p, struct vm_map_entry *entry, vm_object_t obj);
void slsvm_print_chain(vm_object_t shadow);
void slsvm_print_crc32_vmspace(struct vmspace *vm);
void slsvm_print_crc32_object(vm_object_t obj);

void slsvm_print_vmobject(struct vm_object *obj);
void slsvm_print_vmspace(struct vmspace *space);
void slsvm_print_chain(vm_object_t shadow);

#endif /* _SLSVM_H_ */

