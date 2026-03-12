#include "kvs/config.h"

#include "kvs/common.h"

#include <jansson.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

static void kvs_free_endpoint(kvs_endpoint *endpoint) {
    free(endpoint->node_id);
    free(endpoint->host);
    free(endpoint->rdma_host);
    memset(endpoint, 0, sizeof(*endpoint));
}

static void kvs_free_cn_config(kvs_cn_config *config) {
    size_t i;

    free(config->client_id);
    free(config->encryption_key_hex);
    for (i = 0; i < config->mn_endpoint_count; ++i) {
        kvs_free_endpoint(&config->mn_endpoints[i]);
    }
    free(config->mn_endpoints);
    memset(config, 0, sizeof(*config));
}

static void kvs_free_mn_config(kvs_mn_config *config) {
    free(config->node_id);
    free(config->listen_host);
    free(config->state_dir);
    free(config->rdma_listen_host);
    memset(config, 0, sizeof(*config));
}

void kvs_free_config(kvs_node_config *config) {
    if (config == NULL) {
        return;
    }
    kvs_free_cn_config(&config->cn);
    kvs_free_mn_config(&config->mn);
    memset(config, 0, sizeof(*config));
}

const char *kvs_role_name(kvs_node_role role) {
    switch (role) {
        case KVS_ROLE_MN:
            return "mn";
        case KVS_ROLE_CN:
            return "cn";
        default:
            return "unknown";
    }
}

const char *kvs_cache_transport_name(kvs_cache_transport transport) {
    switch (transport) {
        case KVS_CACHE_TRANSPORT_AUTO:
            return "auto";
        case KVS_CACHE_TRANSPORT_TCP:
            return "tcp";
        case KVS_CACHE_TRANSPORT_RDMA:
            return "rdma";
        default:
            return "unknown";
    }
}

static int kvs_get_bool(const json_t *obj, const char *key, bool default_value) {
    json_t *value = json_object_get(obj, key);
    if (value == NULL) {
        return default_value;
    }
    return json_is_true(value);
}

static int kvs_get_int(const json_t *obj, const char *key, int default_value, char *err, size_t err_len) {
    json_t *value = json_object_get(obj, key);
    if (value == NULL) {
        return default_value;
    }
    if (!json_is_integer(value)) {
        kvs_errorf(err, err_len, "field '%s' must be an integer", key);
        return INT_MIN;
    }
    return (int) json_integer_value(value);
}

static const char *kvs_require_string(const json_t *obj, const char *key, char *err, size_t err_len) {
    json_t *value = json_object_get(obj, key);
    if (!json_is_string(value)) {
        kvs_errorf(err, err_len, "field '%s' must be a string", key);
        return NULL;
    }
    return json_string_value(value);
}

static char *kvs_optional_string_dup(const json_t *obj, const char *key) {
    json_t *value = json_object_get(obj, key);
    if (json_is_string(value)) {
        return kvs_strdup(json_string_value(value));
    }
    return NULL;
}

static char *kvs_config_dir(const char *path) {
    const char *slash = strrchr(path, '/');
    size_t len;
    char *dir;

    if (slash == NULL) {
        return kvs_strdup(".");
    }
    len = (size_t) (slash - path);
    if (len == 0) {
        return kvs_strdup("/");
    }
    dir = malloc(len + 1);
    if (dir == NULL) {
        return NULL;
    }
    memcpy(dir, path, len);
    dir[len] = '\0';
    return dir;
}

static kvs_cache_transport kvs_parse_transport(const char *raw, char *err, size_t err_len) {
    if (strcmp(raw, "auto") == 0) {
        return KVS_CACHE_TRANSPORT_AUTO;
    }
    if (strcmp(raw, "tcp") == 0) {
        return KVS_CACHE_TRANSPORT_TCP;
    }
    if (strcmp(raw, "rdma") == 0) {
        return KVS_CACHE_TRANSPORT_RDMA;
    }
    kvs_errorf(err, err_len, "cache_path_transport must be one of: auto, tcp, rdma");
    return (kvs_cache_transport) -1;
}

static int kvs_parse_mn_config(
    const json_t *root,
    const char *config_dir,
    bool root_require_tdx,
    kvs_mn_config *out,
    char *err,
    size_t err_len
) {
    const json_t *mn = json_object_get(root, "mn");
    const char *state_dir_raw;
    int listen_port;
    int cache_capacity;
    int rdma_port;

    if (!json_is_object(mn)) {
        kvs_errorf(err, err_len, "missing 'mn' object in config");
        return -1;
    }

    memset(out, 0, sizeof(*out));
    out->node_id = kvs_strdup(kvs_require_string(mn, "node_id", err, err_len));
    out->listen_host = kvs_strdup(kvs_require_string(mn, "listen_host", err, err_len));
    if (out->node_id == NULL || out->listen_host == NULL) {
        kvs_free_mn_config(out);
        kvs_errorf(err, err_len, "out of memory");
        return -1;
    }
    listen_port = kvs_get_int(mn, "listen_port", INT_MIN, err, err_len);
    cache_capacity = kvs_get_int(mn, "cache_capacity", INT_MIN, err, err_len);
    if (listen_port == INT_MIN || cache_capacity == INT_MIN) {
        kvs_free_mn_config(out);
        return -1;
    }
    state_dir_raw = kvs_require_string(mn, "state_dir", err, err_len);
    if (state_dir_raw == NULL) {
        kvs_free_mn_config(out);
        return -1;
    }
    if (state_dir_raw[0] == '/') {
        out->state_dir = kvs_strdup(state_dir_raw);
    } else {
        out->state_dir = kvs_path_join(config_dir, state_dir_raw);
    }
    out->require_tdx = kvs_get_bool(mn, "require_tdx", root_require_tdx);
    out->listen_port = (uint16_t) listen_port;
    out->cache_capacity = (size_t) cache_capacity;
    out->enable_rdma_server = kvs_get_bool(mn, "enable_rdma_server", false);
    out->rdma_listen_host = kvs_optional_string_dup(mn, "rdma_listen_host");
    out->require_rdma_server = kvs_get_bool(mn, "require_rdma_server", false);
    rdma_port = kvs_get_int(mn, "rdma_listen_port", INT_MIN + 1, err, err_len);
    if (rdma_port == INT_MIN) {
        kvs_free_mn_config(out);
        return -1;
    }
    if (rdma_port != INT_MIN + 1) {
        out->has_rdma_listen_port = true;
        out->rdma_listen_port = (uint16_t) rdma_port;
    }
    if (out->state_dir == NULL) {
        kvs_free_mn_config(out);
        kvs_errorf(err, err_len, "out of memory");
        return -1;
    }
    return 0;
}

static int kvs_parse_cn_config(
    const json_t *root,
    bool root_require_tdx,
    kvs_cn_config *out,
    char *err,
    size_t err_len
) {
    const json_t *cn = json_object_get(root, "cn");
    const json_t *endpoints;
    size_t index;
    json_t *item;
    const char *transport;

    if (!json_is_object(cn)) {
        kvs_errorf(err, err_len, "missing 'cn' object in config");
        return -1;
    }

    memset(out, 0, sizeof(*out));
    out->client_id = kvs_strdup(kvs_require_string(cn, "client_id", err, err_len));
    out->encryption_key_hex = kvs_strdup(kvs_require_string(cn, "encryption_key_hex", err, err_len));
    if (out->client_id == NULL || out->encryption_key_hex == NULL) {
        kvs_free_cn_config(out);
        kvs_errorf(err, err_len, "out of memory");
        return -1;
    }
    out->replication_factor = kvs_get_int(cn, "replication_factor", 1, err, err_len);
    if (out->replication_factor == INT_MIN) {
        kvs_free_cn_config(out);
        return -1;
    }
    out->populate_cache_on_read_miss = kvs_get_bool(cn, "populate_cache_on_read_miss", true);
    out->max_retries = kvs_get_int(cn, "max_retries", 8, err, err_len);
    if (out->max_retries == INT_MIN) {
        kvs_free_cn_config(out);
        return -1;
    }
    out->require_tdx = kvs_get_bool(cn, "require_tdx", root_require_tdx);
    out->trace_operations = kvs_get_bool(cn, "trace_operations", true);
    out->print_operation_latency = kvs_get_bool(cn, "print_operation_latency", false);
    transport = json_string_value(json_object_get(cn, "cache_path_transport"));
    if (transport == NULL) {
        transport = "auto";
    }
    out->cache_path_transport = kvs_parse_transport(transport, err, err_len);
    if ((int) out->cache_path_transport < 0) {
        kvs_free_cn_config(out);
        return -1;
    }

    endpoints = json_object_get(cn, "mn_endpoints");
    if (!json_is_array(endpoints)) {
        kvs_errorf(err, err_len, "cn.mn_endpoints must be an array");
        kvs_free_cn_config(out);
        return -1;
    }
    out->mn_endpoint_count = json_array_size(endpoints);
    out->mn_endpoints = calloc(out->mn_endpoint_count, sizeof(*out->mn_endpoints));
    if (out->mn_endpoint_count > 0 && out->mn_endpoints == NULL) {
        kvs_free_cn_config(out);
        kvs_errorf(err, err_len, "out of memory");
        return -1;
    }

    json_array_foreach(endpoints, index, item) {
        int port;
        int rdma_port;
        kvs_endpoint *endpoint = &out->mn_endpoints[index];
        if (!json_is_object(item)) {
            kvs_errorf(err, err_len, "cn.mn_endpoints[%zu] must be an object", index);
            kvs_free_cn_config(out);
            return -1;
        }
        endpoint->node_id = kvs_strdup(kvs_require_string(item, "node_id", err, err_len));
        endpoint->host = kvs_strdup(kvs_require_string(item, "host", err, err_len));
        if (endpoint->node_id == NULL || endpoint->host == NULL) {
            kvs_free_cn_config(out);
            kvs_errorf(err, err_len, "out of memory");
            return -1;
        }
        port = kvs_get_int(item, "port", INT_MIN, err, err_len);
        if (port == INT_MIN) {
            kvs_free_cn_config(out);
            return -1;
        }
        endpoint->port = (uint16_t) port;
        endpoint->rdma_host = kvs_optional_string_dup(item, "rdma_host");
        rdma_port = kvs_get_int(item, "rdma_port", INT_MIN + 1, err, err_len);
        if (rdma_port == INT_MIN) {
            kvs_free_cn_config(out);
            return -1;
        }
        if (rdma_port != INT_MIN + 1) {
            endpoint->has_rdma_port = true;
            endpoint->rdma_port = (uint16_t) rdma_port;
        }
    }
    return 0;
}

int kvs_load_config(const char *path, kvs_node_config *out, char *err, size_t err_len) {
    json_error_t json_error;
    json_t *root = NULL;
    char *config_dir = NULL;
    const char *role;
    bool root_require_tdx;

    memset(out, 0, sizeof(*out));
    root = json_load_file(path, 0, &json_error);
    if (root == NULL) {
        kvs_errorf(err, err_len, "failed to load config %s: %s", path, json_error.text);
        return -1;
    }
    if (!json_is_object(root)) {
        kvs_errorf(err, err_len, "config root must be an object");
        json_decref(root);
        return -1;
    }

    role = kvs_require_string(root, "role", err, err_len);
    if (role == NULL) {
        json_decref(root);
        return -1;
    }
    root_require_tdx = kvs_get_bool(root, "require_tdx", false);
    config_dir = kvs_config_dir(path);
    if (config_dir == NULL) {
        kvs_errorf(err, err_len, "out of memory");
        json_decref(root);
        return -1;
    }

    out->require_tdx = root_require_tdx;
    if (strcmp(role, "mn") == 0) {
        out->role = KVS_ROLE_MN;
        if (kvs_parse_mn_config(root, config_dir, root_require_tdx, &out->mn, err, err_len) != 0) {
            free(config_dir);
            json_decref(root);
            kvs_free_config(out);
            return -1;
        }
    } else if (strcmp(role, "cn") == 0) {
        out->role = KVS_ROLE_CN;
        if (kvs_parse_cn_config(root, root_require_tdx, &out->cn, err, err_len) != 0) {
            free(config_dir);
            json_decref(root);
            kvs_free_config(out);
            return -1;
        }
    } else {
        kvs_errorf(err, err_len, "role must be either 'mn' or 'cn'");
        free(config_dir);
        json_decref(root);
        return -1;
    }

    free(config_dir);
    json_decref(root);
    return 0;
}
