#ifndef _SLSFILE_H_
#define _SLSFILE_H_

#include <sys/param.h>

#include <sys/file.h>
#include <vm/vm_object.h>

#include "../memckpt.h"
#include "desc.h"

int file_read(void* addr, size_t len, int fd);
int file_write(void* addr, size_t len, int fd);
void file_dump(struct vm_map_entry_info *entries, vm_object_t *objects, 
		    size_t numentries, int fd);

#endif /* _SLSFILE_H */