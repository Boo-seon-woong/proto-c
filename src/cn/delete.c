#include "internal.h"

int kvs_cn_delete(kvs_cn_node *node, const char *key, char *err, size_t err_len) {
    return kvs_cn_encrypt_and_replicate(
        node,
        key,
        KVS_TOMBSTONE_MARKER,
        KVS_TOMBSTONE_MARKER_LEN,
        true,
        "delete",
        err,
        err_len
    );
}
