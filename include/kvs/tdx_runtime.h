#ifndef KVS_TDX_RUNTIME_H
#define KVS_TDX_RUNTIME_H

#include <stdbool.h>
#include <stddef.h>

bool kvs_is_running_in_tdx_guest(void);
int kvs_enforce_tdx_requirement(bool require_tdx, char *err, size_t err_len);

#endif
