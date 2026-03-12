#ifndef KVS_RPC_H
#define KVS_RPC_H

#include <jansson.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "kvs/config.h"

typedef json_t *(*kvs_rpc_handler_fn)(void *ctx, const json_t *request);

typedef struct {
    int listener_fd;
    pthread_t thread;
    bool started;
    bool stop_requested;
    char bind_host[128];
    uint16_t requested_port;
    uint16_t bound_port;
    kvs_rpc_handler_fn handler;
    void *handler_ctx;
} kvs_tcp_server;

int kvs_rpc_call(
    const kvs_endpoint *endpoint,
    const char *action,
    json_t *params,
    double timeout_sec,
    json_t **response_out,
    char *err,
    size_t err_len
);
int kvs_tcp_server_start(
    kvs_tcp_server *server,
    const char *host,
    uint16_t port,
    kvs_rpc_handler_fn handler,
    void *handler_ctx,
    char *err,
    size_t err_len
);
void kvs_tcp_server_stop(kvs_tcp_server *server);

#endif
