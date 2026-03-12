#include "internal.h"

#include "kvs/common.h"

#include <stdlib.h>
#include <string.h>

void kvs_cache_rpc_outcome_cleanup(kvs_cache_rpc_outcome *outcome) {
    if (outcome == NULL) {
        return;
    }
    kvs_json_decref_safe(outcome->response);
    free(outcome->fallback_error);
    memset(outcome, 0, sizeof(*outcome));
}

const char *kvs_cache_transport_label(const kvs_cache_rpc_outcome *outcome) {
    if (outcome->fallback_error != NULL) {
        return "tcp(fallback-from-rdma)";
    }
    return outcome->transport == KVS_TRANSPORT_RESULT_RDMA ? "rdma" : "tcp";
}

json_t *kvs_result_object(const kvs_cache_rpc_outcome *outcome) {
    return json_object_get(outcome->response, "result");
}

int kvs_cn_cache_rpc(
    kvs_cn_node *node,
    const kvs_endpoint *endpoint,
    const char *action,
    json_t *params,
    kvs_cache_rpc_outcome *out,
    char *err,
    size_t err_len
) {
    char rdma_err[256] = {0};

    memset(out, 0, sizeof(*out));
    if (node->config.cache_path_transport == KVS_CACHE_TRANSPORT_RDMA ||
        (node->config.cache_path_transport == KVS_CACHE_TRANSPORT_AUTO && endpoint->has_rdma_port)) {
        kvs_rdma_endpoint rdma_endpoint = {
            .node_id = endpoint->node_id,
            .host = endpoint->rdma_host != NULL ? endpoint->rdma_host : endpoint->host,
            .port = endpoint->rdma_port,
        };
        if (!endpoint->has_rdma_port) {
            kvs_errorf(err, err_len, "%s has no rdma_port configured", endpoint->node_id);
            return -1;
        }
        if (kvs_rdma_call(&rdma_endpoint, action, params, &out->response, rdma_err, sizeof(rdma_err)) == 0) {
            out->transport = KVS_TRANSPORT_RESULT_RDMA;
            return 0;
        }
        if (node->config.cache_path_transport == KVS_CACHE_TRANSPORT_RDMA) {
            kvs_errorf(err, err_len, "%s", rdma_err);
            return -1;
        }
        out->fallback_error = kvs_strdup(rdma_err);
    }
    if (kvs_rpc_call(endpoint, action, params, 3.0, &out->response, err, err_len) != 0) {
        return -1;
    }
    out->transport = KVS_TRANSPORT_RESULT_TCP;
    return 0;
}
