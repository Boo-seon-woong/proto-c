#ifndef KVS_CONFIG_H
#define KVS_CONFIG_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef enum {
    KVS_ROLE_UNKNOWN = 0,
    KVS_ROLE_MN,
    KVS_ROLE_CN,
} kvs_node_role;

typedef enum {
    KVS_CACHE_TRANSPORT_AUTO = 0,
    KVS_CACHE_TRANSPORT_TCP,
    KVS_CACHE_TRANSPORT_RDMA,
} kvs_cache_transport;

typedef struct {
    char *node_id;
    char *host;
    uint16_t port;
    bool has_rdma_port;
    uint16_t rdma_port;
    char *rdma_host;
} kvs_endpoint;

typedef struct {
    char *client_id;
    char *encryption_key_hex;
    int replication_factor;
    kvs_endpoint *mn_endpoints;
    size_t mn_endpoint_count;
    bool populate_cache_on_read_miss;
    int max_retries;
    bool require_tdx;
    kvs_cache_transport cache_path_transport;
    bool trace_operations;
} kvs_cn_config;

typedef struct {
    char *node_id;
    char *listen_host;
    uint16_t listen_port;
    size_t cache_capacity;
    char *state_dir;
    bool require_tdx;
    bool enable_rdma_server;
    char *rdma_listen_host;
    bool has_rdma_listen_port;
    uint16_t rdma_listen_port;
    bool require_rdma_server;
} kvs_mn_config;

typedef struct {
    kvs_node_role role;
    bool require_tdx;
    kvs_cn_config cn;
    kvs_mn_config mn;
} kvs_node_config;

int kvs_load_config(const char *path, kvs_node_config *out, char *err, size_t err_len);
void kvs_free_config(kvs_node_config *config);
const char *kvs_role_name(kvs_node_role role);
const char *kvs_cache_transport_name(kvs_cache_transport transport);

#endif
