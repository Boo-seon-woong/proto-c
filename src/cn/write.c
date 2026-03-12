#include "internal.h"

#include <string.h>

int kvs_cn_write(kvs_cn_node *node, const char *key, const char *value, char *err, size_t err_len) {
    return kvs_cn_encrypt_and_replicate(
        node,
        key,
        (const unsigned char *) value,
        strlen(value),
        false,
        "write",
        err,
        err_len
    );
}
