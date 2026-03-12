#include "kvs/cn_node.h"
#include "kvs/common.h"
#include "kvs/config.h"
#include "kvs/mn_node.h"

#include <jansson.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define KVS_TEST_REPLICA_COUNT 3u

static int failures = 0;

typedef struct {
    kvs_cn_node *client;
    kvs_mn_node *mns[KVS_TEST_REPLICA_COUNT];
} kvs_test_cluster;

static void expect_true(bool condition, const char *message) {
    if (!condition) {
        fprintf(stderr, "FAIL: %s\n", message);
        failures += 1;
    }
}

static void expect_streq(const char *expected, const char *actual, const char *message) {
    if ((expected == NULL && actual != NULL) ||
        (expected != NULL && actual == NULL) ||
        (expected != NULL && actual != NULL && strcmp(expected, actual) != 0)) {
        fprintf(stderr, "FAIL: %s (expected=%s actual=%s)\n", message, expected, actual);
        failures += 1;
    }
}

static json_t *mn_call(kvs_mn_node *node, const char *action, json_t *params) {
    json_t *request = json_object();
    json_t *response;

    if (request == NULL) {
        if (params != NULL) {
            json_decref(params);
        }
        return NULL;
    }
    if (params == NULL) {
        params = json_object();
    }
    if (params == NULL ||
        json_object_set_new(request, "action", json_string(action)) != 0 ||
        json_object_set_new(request, "params", params) != 0) {
        json_decref(request);
        return NULL;
    }
    response = kvs_mn_node_handle_rpc_json(node, request);
    json_decref(request);
    return response;
}

static bool mn_has_prime_key(kvs_mn_node *node, const char *key) {
    json_t *params = json_pack("{s:s}", "key", key);
    json_t *response = mn_call(node, "rdma_read_prime", params);
    bool found = false;

    if (response != NULL) {
        found = json_is_true(json_object_get(json_object_get(response, "result"), "found"));
        json_decref(response);
    }
    return found;
}

static int mn_private_entries(kvs_mn_node *node) {
    json_t *response = mn_call(node, "debug_state", json_object());
    int count = -1;

    if (response != NULL) {
        count = (int) json_integer_value(json_object_get(json_object_get(response, "result"), "private_entries"));
        json_decref(response);
    }
    return count;
}

static size_t primary_index_for_key(const char *key) {
    return (size_t) (kvs_sha256_hash64(key) % KVS_TEST_REPLICA_COUNT);
}

static kvs_cn_node *start_cluster(const char *base_dir, kvs_test_cluster *cluster) {
    char mn_dirs[KVS_TEST_REPLICA_COUNT][512];
    kvs_mn_config mn_cfg[KVS_TEST_REPLICA_COUNT];
    kvs_endpoint endpoints[KVS_TEST_REPLICA_COUNT];
    kvs_cn_config cn_cfg;
    char err[256];
    size_t i;

    memset(cluster, 0, sizeof(*cluster));
    memset(mn_cfg, 0, sizeof(mn_cfg));
    memset(endpoints, 0, sizeof(endpoints));
    memset(&cn_cfg, 0, sizeof(cn_cfg));

    for (i = 0u; i < KVS_TEST_REPLICA_COUNT; ++i) {
        snprintf(mn_dirs[i], sizeof(mn_dirs[i]), "%s/mn%zu", base_dir, i + 1u);
        mn_cfg[i].node_id = (i == 0u) ? "mn-1" : (i == 1u) ? "mn-2" : "mn-3";
        mn_cfg[i].listen_host = "127.0.0.1";
        mn_cfg[i].listen_port = 0;
        mn_cfg[i].cache_capacity = 2;
        mn_cfg[i].state_dir = mn_dirs[i];
        cluster->mns[i] = kvs_mn_node_create(&mn_cfg[i], err, sizeof(err));
        expect_true(cluster->mns[i] != NULL, err);
        if (cluster->mns[i] == NULL) {
            continue;
        }
        expect_true(kvs_mn_node_start(cluster->mns[i], err, sizeof(err)) == 0, err);
        endpoints[i].node_id = (i == 0u) ? "mn-1" : (i == 1u) ? "mn-2" : "mn-3";
        endpoints[i].host = "127.0.0.1";
        endpoints[i].port = kvs_mn_node_tcp_port(cluster->mns[i]);
    }

    cn_cfg.client_id = "cn-1";
    cn_cfg.encryption_key_hex = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
    cn_cfg.replication_factor = (int) KVS_TEST_REPLICA_COUNT;
    cn_cfg.mn_endpoints = endpoints;
    cn_cfg.mn_endpoint_count = KVS_TEST_REPLICA_COUNT;
    cn_cfg.populate_cache_on_read_miss = true;
    cn_cfg.max_retries = 16;
    cn_cfg.cache_path_transport = KVS_CACHE_TRANSPORT_TCP;
    cn_cfg.trace_operations = false;

    sleep(1);
    cluster->client = kvs_cn_node_create(&cn_cfg, err, sizeof(err));
    expect_true(cluster->client != NULL, err);
    return cluster->client;
}

static void stop_cluster(kvs_test_cluster *cluster) {
    size_t i;

    kvs_cn_node_destroy(cluster->client);
    cluster->client = NULL;
    for (i = 0u; i < KVS_TEST_REPLICA_COUNT; ++i) {
        kvs_mn_node_destroy(cluster->mns[i]);
        cluster->mns[i] = NULL;
    }
}

static void test_write_read_update_delete(const char *base_dir) {
    kvs_test_cluster cluster;
    kvs_cn_node *client = start_cluster(base_dir, &cluster);
    char err[256];
    char *value = NULL;
    bool found = false;

    expect_true(kvs_cn_write(client, "alpha", "v1", err, sizeof(err)) == 0, err);
    expect_true(kvs_cn_read(client, "alpha", &value, &found, err, sizeof(err)) == 0, err);
    expect_true(found, "alpha should be found");
    expect_streq("v1", value, "alpha should equal v1");
    free(value);
    value = NULL;

    expect_true(kvs_cn_update(client, "alpha", "v2", err, sizeof(err)) == 0, err);
    expect_true(kvs_cn_read(client, "alpha", &value, &found, err, sizeof(err)) == 0, err);
    expect_true(found, "alpha should still be found");
    expect_streq("v2", value, "alpha should equal v2");
    free(value);
    value = NULL;

    expect_true(kvs_cn_delete(client, "alpha", err, sizeof(err)) == 0, err);
    expect_true(kvs_cn_read(client, "alpha", &value, &found, err, sizeof(err)) == 0, err);
    expect_true(!found, "alpha should be deleted");
    free(value);
    stop_cluster(&cluster);
}

static void test_eviction_and_private_recovery(const char *base_dir) {
    kvs_test_cluster cluster;
    kvs_cn_node *client = start_cluster(base_dir, &cluster);
    char err[256];
    char *value = NULL;
    bool found = false;
    size_t i;

    expect_true(kvs_cn_write(client, "k1", "value-1", err, sizeof(err)) == 0, err);
    expect_true(kvs_cn_write(client, "k2", "value-2", err, sizeof(err)) == 0, err);
    expect_true(kvs_cn_write(client, "k3", "value-3", err, sizeof(err)) == 0, err);
    for (i = 0u; i < KVS_TEST_REPLICA_COUNT; ++i) {
        expect_true(mn_private_entries(cluster.mns[i]) >= 1, "each replica should flush at least one cold entry");
    }
    expect_true(kvs_cn_read(client, "k1", &value, &found, err, sizeof(err)) == 0, err);
    expect_true(found, "k1 should be recovered");
    expect_streq("value-1", value, "k1 should equal original value");
    free(value);
    stop_cluster(&cluster);
}

static void test_quorum_write_survives_single_backup_failure(const char *base_dir) {
    kvs_test_cluster cluster;
    kvs_cn_node *client = start_cluster(base_dir, &cluster);
    char err[256];
    char *value = NULL;
    bool found = false;
    const char *key = "quorum-key";
    size_t primary = primary_index_for_key(key);
    size_t failed_backup = (primary + 1u) % KVS_TEST_REPLICA_COUNT;
    size_t live_backup = (primary + 2u) % KVS_TEST_REPLICA_COUNT;

    kvs_mn_node_stop(cluster.mns[failed_backup]);
    expect_true(kvs_cn_write(client, key, "quorum-value", err, sizeof(err)) == 0, err);
    expect_true(kvs_cn_read(client, key, &value, &found, err, sizeof(err)) == 0, err);
    expect_true(found, "quorum-key should be readable after one backup failure");
    expect_streq("quorum-value", value, "quorum-key should equal quorum-value");
    expect_true(mn_has_prime_key(cluster.mns[primary], key), "primary should hold committed key");
    expect_true(mn_has_prime_key(cluster.mns[live_backup], key), "surviving backup should hold committed key");
    expect_true(!mn_has_prime_key(cluster.mns[failed_backup], key), "stopped backup should not receive the write");
    free(value);
    stop_cluster(&cluster);
}

static void test_write_requires_primary_commit(const char *base_dir) {
    kvs_test_cluster cluster;
    kvs_cn_node *client = start_cluster(base_dir, &cluster);
    char err[256];
    const char *key = "primary-commit-required";
    size_t primary = primary_index_for_key(key);
    size_t backup1 = (primary + 1u) % KVS_TEST_REPLICA_COUNT;
    size_t backup2 = (primary + 2u) % KVS_TEST_REPLICA_COUNT;

    kvs_mn_node_stop(cluster.mns[primary]);
    expect_true(kvs_cn_write(client, key, "should-fail", err, sizeof(err)) != 0, "write should fail when primary is unavailable");
    expect_true(!mn_has_prime_key(cluster.mns[backup1], key), "backup one should not commit without primary");
    expect_true(!mn_has_prime_key(cluster.mns[backup2], key), "backup two should not commit without primary");
    stop_cluster(&cluster);
}

int main(void) {
    char base_template[] = "/tmp/kvs-test-XXXXXX";
    char *base_dir = mkdtemp(base_template);

    if (base_dir == NULL) {
        perror("mkdtemp");
        return 1;
    }

    test_write_read_update_delete(base_dir);
    test_eviction_and_private_recovery(base_dir);
    test_quorum_write_survives_single_backup_failure(base_dir);
    test_write_requires_primary_commit(base_dir);

    if (failures > 0) {
        fprintf(stderr, "%d test(s) failed\n", failures);
        return 1;
    }
    puts("All tests passed");
    return 0;
}
