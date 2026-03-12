#ifndef KVS_CN_NODE_H
#define KVS_CN_NODE_H

#include <jansson.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>

#include "kvs/config.h"

typedef struct kvs_cn_node kvs_cn_node;

typedef enum {
    KVS_TRANSPORT_RESULT_TCP = 0,
    KVS_TRANSPORT_RESULT_RDMA,
} kvs_transport_result;

typedef struct {
    json_t *response;
    kvs_transport_result transport;
    char *fallback_error;
} kvs_cache_rpc_outcome;

kvs_cn_node *kvs_cn_node_create(const kvs_cn_config *config, char *err, size_t err_len);
void kvs_cn_node_destroy(kvs_cn_node *node);

int kvs_cn_write(kvs_cn_node *node, const char *key, const char *value, char *err, size_t err_len);
int kvs_cn_update(kvs_cn_node *node, const char *key, const char *value, char *err, size_t err_len);
int kvs_cn_delete(kvs_cn_node *node, const char *key, char *err, size_t err_len);
int kvs_cn_read(
    kvs_cn_node *node,
    const char *key,
    char **value_out,
    bool *found_out,
    char *err,
    size_t err_len
);
int kvs_cn_debug_cluster_state(kvs_cn_node *node, json_t **state_out, char *err, size_t err_len);
int kvs_cn_verify_rdma(kvs_cn_node *node, const char *probe_key, FILE *out, char *err, size_t err_len);

int kvs_cn_cache_rpc(
    kvs_cn_node *node,
    const kvs_endpoint *endpoint,
    const char *action,
    json_t *params,
    kvs_cache_rpc_outcome *out,
    char *err,
    size_t err_len
);
void kvs_cache_rpc_outcome_cleanup(kvs_cache_rpc_outcome *outcome);
const char *kvs_cache_transport_label(const kvs_cache_rpc_outcome *outcome);

#endif
