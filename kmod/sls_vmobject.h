#ifndef _SLS_VMOBJECT_H_
#define _SLS_VMOBJECT_H_

#include <sys/types.h>

#include <sys/sbuf.h>

#include "sls_internal.h"
#include "sls_data.h"
#include "sls_kv.h"
#include "sls_load.h"
#include "sls_table.h"

int slsckpt_vmobject(struct proc *p, vm_object_t obj);
int slsrest_vmobject(struct slsvmobject *slsvmobject, struct slskv_table *objtable, struct slsdata *slsdata);

#endif /* _SLS_VMOBJECT_H_ */