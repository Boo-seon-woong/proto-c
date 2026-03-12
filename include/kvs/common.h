#ifndef KVS_COMMON_H
#define KVS_COMMON_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define KVS_RPC_MAX_LINE (8u * 1024u * 1024u)

void kvs_errorf(char *buf, size_t len, const char *fmt, ...);
char *kvs_strdup(const char *src);
char *kvs_path_join(const char *left, const char *right);
int kvs_mkdir_p(const char *path, char *err, size_t err_len);
uint64_t kvs_sha256_hash64(const char *text);

#endif
