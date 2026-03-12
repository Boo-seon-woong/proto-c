#ifndef KVS_CN_INTERNAL_H
#define KVS_CN_INTERNAL_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "kvs/cn_node.h"
#include "kvs/crypto.h"
#include "kvs/models.h"
#include "kvs/rdma_rpc.h"
#include "kvs/rpc.h"

extern const unsigned char KVS_TOMBSTONE_MARKER[];
#define KVS_TOMBSTONE_MARKER_LEN (sizeof("__TDX_KVS_TOMBSTONE__") - 1u)

struct kvs_cn_node {
    kvs_cn_config config;
    kvs_aead_cipher cipher;
    bool trace_enabled;
};

typedef enum {
    KVS_SNAPSHOT_COMPLETE = 0,
    KVS_SNAPSHOT_RETRY = 1,
} kvs_snapshot_status;

typedef struct {
    const kvs_endpoint *endpoint;
    bool prime_found;
    uint32_t prime_addr;
    uint32_t prime_epoch;
    bool have_private_addr;
    int private_addr;
    bool slot_prepared;
    uint32_t slot_id;
    uint32_t slot_epoch;
    bool cas_applied;
    char failure[256];
} kvs_cn_replica_commit;

void kvs_json_decref_safe(json_t *value);
void kvs_cn_trace(kvs_cn_node *node, const char *fmt, ...);
json_t *kvs_result_object(const kvs_cache_rpc_outcome *outcome);
const kvs_endpoint *kvs_primary_replica(const kvs_cn_node *node, const char *key);
int kvs_select_replicas(
    const kvs_cn_node *node,
    const char *key,
    const kvs_endpoint ***out,
    size_t *count_out,
    char *err,
    size_t err_len
);
int kvs_cn_encrypt_and_replicate(
    kvs_cn_node *node,
    const char *key,
    const unsigned char *payload,
    size_t payload_len,
    bool tombstone,
    const char *operation,
    char *err,
    size_t err_len
);
int kvs_cn_replicate_record(
    kvs_cn_node *node,
    const char *key,
    const kvs_cipher_record *record,
    const char *operation,
    const kvs_endpoint **replicas,
    size_t replica_count,
    char *err,
    size_t err_len
);
int kvs_cn_commit_insert(
    kvs_cn_node *node,
    const kvs_cn_replica_commit *replica,
    const char *key,
    const char *phase,
    const char *operation,
    int attempt,
    char *err,
    size_t err_len
);
int kvs_cn_prepare_insert(
    kvs_cn_node *node,
    kvs_cn_replica_commit *replica,
    const kvs_cipher_record *record,
    const char *key,
    const char *operation,
    int attempt,
    char *err,
    size_t err_len
);
int kvs_cn_rollback_insert(
    kvs_cn_node *node,
    const kvs_cn_replica_commit *replica,
    const char *key,
    const char *operation,
    int attempt,
    char *err,
    size_t err_len
);
int kvs_cn_commit_update(
    kvs_cn_node *node,
    const kvs_cn_replica_commit *replica,
    const char *key,
    uint32_t expected_addr,
    uint32_t expected_epoch,
    const char *phase,
    const char *operation,
    int attempt,
    char *err,
    size_t err_len
);
int kvs_cn_prepare_update(
    kvs_cn_node *node,
    kvs_cn_replica_commit *replica,
    const kvs_cipher_record *record,
    const char *key,
    const char *operation,
    int attempt,
    char *err,
    size_t err_len
);
int kvs_cn_rollback_update(
    kvs_cn_node *node,
    const kvs_cn_replica_commit *replica,
    const char *key,
    uint32_t expected_addr,
    uint32_t expected_epoch,
    const char *operation,
    int attempt,
    char *err,
    size_t err_len
);
int kvs_cn_promote_private_to_cache(
    kvs_cn_node *node,
    const char *key,
    const kvs_cipher_record *record,
    char *err,
    size_t err_len
);
kvs_snapshot_status kvs_cn_snapshot_consensus(
    kvs_cn_node *node,
    const kvs_endpoint *endpoint,
    const char *key,
    const kvs_prime_entry_wire *prime1,
    int attempt,
    char **value_out,
    bool *found_out,
    char *err,
    size_t err_len
);

#endif
