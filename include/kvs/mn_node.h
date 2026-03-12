#ifndef KVS_MN_NODE_H
#define KVS_MN_NODE_H

#include <jansson.h>
#include <stdint.h>

#include "kvs/config.h"

typedef struct kvs_mn_node kvs_mn_node;

kvs_mn_node *kvs_mn_node_create(const kvs_mn_config *config, char *err, size_t err_len);
void kvs_mn_node_destroy(kvs_mn_node *node);
int kvs_mn_node_start(kvs_mn_node *node, char *err, size_t err_len);
void kvs_mn_node_stop(kvs_mn_node *node);
uint16_t kvs_mn_node_tcp_port(const kvs_mn_node *node);
uint16_t kvs_mn_node_rdma_port(const kvs_mn_node *node);
const kvs_mn_config *kvs_mn_node_config(const kvs_mn_node *node);
json_t *kvs_mn_node_handle_rpc_json(void *ctx, const json_t *request);

#endif
