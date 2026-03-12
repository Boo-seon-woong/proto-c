#ifndef KVS_RDMA_RPC_H
#define KVS_RDMA_RPC_H

#include <jansson.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

typedef json_t *(*kvs_rdma_handler_fn)(void *ctx, const json_t *request);

typedef struct {
    const char *node_id;
    const char *host;
    uint16_t port;
} kvs_rdma_endpoint;

typedef struct {
    bool supported;
    const char *library;
    const char *implementation;
    bool one_sided;
    bool mn_cpu_bypass;
    char error[256];
} kvs_rdma_profile;

typedef struct {
    int listener_fd;
    pthread_t thread;
    bool started;
    bool stop_requested;
    char bind_host[128];
    uint16_t requested_port;
    uint16_t bound_port;
    kvs_rdma_handler_fn handler;
    void *handler_ctx;
} kvs_rdma_server;

typedef struct {
    char device[256];
    char netdev[256];
} kvs_rdma_match;

typedef struct {
    char host[256];
    char resolved_ip[64];
    bool matched_rdma_netdev;
    size_t match_count;
    kvs_rdma_match matches[8];
    char note[256];
    char error[256];
} kvs_rdma_host_report;

bool kvs_rdma_supported(void);
void kvs_rdma_transport_profile(kvs_rdma_profile *profile);
int kvs_rdma_call(
    const kvs_rdma_endpoint *endpoint,
    const char *action,
    json_t *params,
    json_t **response_out,
    char *err,
    size_t err_len
);
int kvs_rdma_server_start(
    kvs_rdma_server *server,
    const char *host,
    uint16_t port,
    kvs_rdma_handler_fn handler,
    void *handler_ctx,
    char *err,
    size_t err_len
);
void kvs_rdma_server_stop(kvs_rdma_server *server);
void kvs_rdma_print_local_nics(FILE *out);
void kvs_rdma_host_binding_report(const char *host, kvs_rdma_host_report *report);

#endif
