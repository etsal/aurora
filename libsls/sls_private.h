#ifndef _SLS_PRIVATE_H__
#define _SLS_PRIVATE_H__

#include "sls_ioctl.h"

int sls_proc(struct proc_param *param);
int sls_ioctl(long ionum, void *args);

#endif