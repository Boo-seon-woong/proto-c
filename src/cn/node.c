#include "internal.h"

#include "kvs/common.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

const unsigned char KVS_TOMBSTONE_MARKER[] = "__TDX_KVS_TOMBSTONE__";

static void kvs_free_endpoint_copy(kvs_endpoint *endpoint) {
    free(endpoint->node_id);
    free(endpoint->host);
    free(endpoint->rdma_host);
    memset(endpoint, 0, sizeof(*endpoint));
}

static void kvs_free_cn_config_copy(kvs_cn_config *config) {
    size_t i;
    free(config->client_id);
    free(config->encryption_key_hex);
    for (i = 0u; i < config->mn_endpoint_count; ++i) {
        kvs_free_endpoint_copy(&config->mn_endpoints[i]);
    }
    free(config->mn_endpoints);
    memset(config, 0, sizeof(*config));
}

static int kvs_copy_cn_config(const kvs_cn_config *src, kvs_cn_config *dst, char *err, size_t err_len) {
    size_t i;
    memset(dst, 0, sizeof(*dst));
    dst->client_id = kvs_strdup(src->client_id);
    dst->encryption_key_hex = kvs_strdup(src->encryption_key_hex);
    dst->mn_endpoints = calloc(src->mn_endpoint_count, sizeof(*dst->mn_endpoints));
    if (dst->client_id == NULL || dst->encryption_key_hex == NULL ||
        (src->mn_endpoint_count > 0u && dst->mn_endpoints == NULL)) {
        kvs_errorf(err, err_len, "out of memory");
        kvs_free_cn_config_copy(dst);
        return -1;
    }
    dst->replication_factor = src->replication_factor;
    dst->mn_endpoint_count = src->mn_endpoint_count;
    dst->populate_cache_on_read_miss = src->populate_cache_on_read_miss;
    dst->max_retries = src->max_retries;
    dst->require_tdx = src->require_tdx;
    dst->cache_path_transport = src->cache_path_transport;
    dst->trace_operations = src->trace_operations;
    dst->print_operation_latency = src->print_operation_latency;
    for (i = 0u; i < src->mn_endpoint_count; ++i) {
        dst->mn_endpoints[i].node_id = kvs_strdup(src->mn_endpoints[i].node_id);
        dst->mn_endpoints[i].host = kvs_strdup(src->mn_endpoints[i].host);
        dst->mn_endpoints[i].rdma_host = kvs_strdup(src->mn_endpoints[i].rdma_host);
        if (dst->mn_endpoints[i].node_id == NULL || dst->mn_endpoints[i].host == NULL ||
            (src->mn_endpoints[i].rdma_host != NULL && dst->mn_endpoints[i].rdma_host == NULL)) {
            kvs_errorf(err, err_len, "out of memory");
            kvs_free_cn_config_copy(dst);
            return -1;
        }
        dst->mn_endpoints[i].port = src->mn_endpoints[i].port;
        dst->mn_endpoints[i].has_rdma_port = src->mn_endpoints[i].has_rdma_port;
        dst->mn_endpoints[i].rdma_port = src->mn_endpoints[i].rdma_port;
    }
    return 0;
}

void kvs_cn_trace(kvs_cn_node *node, const char *fmt, ...) {
    va_list args;
    if (!node->trace_enabled) {
        return;
    }
    fprintf(stderr, "[CN-TRACE][%s] ", node->config.client_id);
    va_start(args, fmt);
    vfprintf(stderr, fmt, args);
    va_end(args);
    fputc('\n', stderr);
    fflush(stderr);
}

void kvs_json_decref_safe(json_t *value) {
    if (value != NULL) {
        json_decref(value);
    }
}

kvs_cn_node *kvs_cn_node_create(const kvs_cn_config *config, char *err, size_t err_len) {
    kvs_cn_node *node = calloc(1, sizeof(*node));
    if (node == NULL) {
        kvs_errorf(err, err_len, "out of memory");
        return NULL;
    }
    if (config->mn_endpoint_count == 0u) {
        kvs_errorf(err, err_len, "cn config must include at least one MN endpoint");
        free(node);
        return NULL;
    }
    if (config->replication_factor <= 0) {
        kvs_errorf(err, err_len, "replication_factor must be > 0");
        free(node);
        return NULL;
    }
    if (kvs_copy_cn_config(config, &node->config, err, err_len) != 0 ||
        kvs_aead_cipher_init(&node->cipher, config->encryption_key_hex, err, err_len) != 0) {
        kvs_cn_node_destroy(node);
        return NULL;
    }
    node->trace_enabled = config->trace_operations;
    return node;
}

void kvs_cn_node_destroy(kvs_cn_node *node) {
    if (node == NULL) {
        return;
    }
    kvs_aead_cipher_cleanup(&node->cipher);
    kvs_free_cn_config_copy(&node->config);
    free(node);
}

const kvs_endpoint *kvs_primary_replica(const kvs_cn_node *node, const char *key) {
    size_t count = node->config.mn_endpoint_count;
    size_t start = (size_t) (kvs_sha256_hash64(key) % count);
    return &node->config.mn_endpoints[start];
}

int kvs_select_replicas(const kvs_cn_node *node, const char *key, const kvs_endpoint ***out, size_t *count_out, char *err, size_t err_len) {
    size_t replica_count = (size_t) node->config.replication_factor;
    size_t total = node->config.mn_endpoint_count;
    size_t start = (size_t) (kvs_sha256_hash64(key) % total);
    const kvs_endpoint **replicas;
    size_t i;

    if (replica_count > total) {
        replica_count = total;
    }
    replicas = calloc(replica_count, sizeof(*replicas));
    if (replicas == NULL) {
        kvs_errorf(err, err_len, "out of memory");
        return -1;
    }
    for (i = 0u; i < replica_count; ++i) {
        replicas[i] = &node->config.mn_endpoints[(start + i) % total];
    }
    *out = replicas;
    *count_out = replica_count;
    return 0;
}

int kvs_cn_debug_cluster_state(kvs_cn_node *node, json_t **state_out, char *err, size_t err_len) {
    json_t *states = json_object();
    size_t i;
    if (states == NULL) {
        kvs_errorf(err, err_len, "out of memory");
        return -1;
    }
    for (i = 0u; i < node->config.mn_endpoint_count; ++i) {
        json_t *response = NULL;
        if (kvs_rpc_call(&node->config.mn_endpoints[i], "debug_state", json_object(), 3.0, &response, err, err_len) != 0) {
            json_decref(states);
            return -1;
        }
        if (json_object_set(states, node->config.mn_endpoints[i].node_id, json_object_get(response, "result")) != 0) {
            json_decref(response);
            json_decref(states);
            kvs_errorf(err, err_len, "out of memory");
            return -1;
        }
        json_decref(response);
    }
    *state_out = states;
    return 0;
}

int kvs_cn_verify_rdma(kvs_cn_node *node, const char *probe_key, FILE *out, char *err, size_t err_len) {
    kvs_rdma_profile profile;
    size_t i;
    bool all_rdma = true;
    kvs_rdma_transport_profile(&profile);
    fprintf(
        out,
        "RDMA runtime profile: supported=%s implementation=%s one_sided=%s mn_cpu_bypass=%s\n",
        profile.supported ? "true" : "false",
        profile.implementation,
        profile.one_sided ? "true" : "false",
        profile.mn_cpu_bypass ? "true" : "false"
    );
    if (profile.error[0] != '\0') {
        fprintf(out, "RDMA runtime detail: %s\n", profile.error);
    }
    if (!profile.one_sided) {
        fprintf(out, "RDMA strict note: current cache-path is two-sided rsocket RPC, so MN CPU participates.\n");
    }
    kvs_rdma_print_local_nics(out);
    for (i = 0u; i < node->config.mn_endpoint_count; ++i) {
        kvs_rdma_host_report report;
        kvs_cache_rpc_outcome outcome;
        json_t *params = json_pack("{s:s}", "key", probe_key);
        const char *rdma_host = node->config.mn_endpoints[i].rdma_host != NULL ?
                                node->config.mn_endpoints[i].rdma_host :
                                node->config.mn_endpoints[i].host;
        kvs_rdma_host_binding_report(rdma_host, &report);
        fprintf(
            out,
            "RDMA host binding: endpoint=%s tcp_host=%s rdma_host=%s resolved=%s %s\n",
            node->config.mn_endpoints[i].node_id,
            node->config.mn_endpoints[i].host,
            rdma_host,
            report.resolved_ip[0] != '\0' ? report.resolved_ip : "-",
            report.matched_rdma_netdev ? "matched_rdma_netdev=true" : (report.note[0] != '\0' ? report.note : "matched_rdma_netdev=false")
        );
        if (params == NULL) {
            kvs_errorf(err, err_len, "out of memory");
            return -1;
        }
        if (kvs_cn_cache_rpc(node, &node->config.mn_endpoints[i], "rdma_read_prime", params, &outcome, err, err_len) != 0) {
            fprintf(out, "RDMA probe: endpoint=%s action=rdma_read_prime error=%s\n", node->config.mn_endpoints[i].node_id, err);
            all_rdma = false;
            json_decref(params);
            continue;
        }
        fprintf(
            out,
            "RDMA probe: endpoint=%s action=rdma_read_prime transport=%s\n",
            node->config.mn_endpoints[i].node_id,
            kvs_cache_transport_label(&outcome)
        );
        if (outcome.transport != KVS_TRANSPORT_RESULT_RDMA) {
            all_rdma = false;
        }
        kvs_cache_rpc_outcome_cleanup(&outcome);
        json_decref(params);
    }
    fprintf(out, "RDMA probe summary: cache_probes_all_rdma=%s one_sided_cpu_bypass=false\n", all_rdma ? "true" : "false");
    return 0;
}
