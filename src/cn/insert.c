#include "internal.h"

#include "kvs/common.h"

#include <stdlib.h>

static int kvs_cn_prepare_insert_slot(
    kvs_cn_node *node,
    kvs_cn_replica_commit *replica,
    const kvs_cipher_record *record,
    const char *key,
    const char *operation,
    int attempt,
    char *err,
    size_t err_len
) {
    kvs_cache_rpc_outcome alloc_outcome;
    kvs_cache_rpc_outcome write_outcome;
    json_t *alloc_params = NULL;
    json_t *record_json = NULL;
    json_t *write_params = NULL;
    json_t *result;

    alloc_params = json_object();
    if (alloc_params == NULL) {
        kvs_errorf(err, err_len, "out of memory");
        return -1;
    }
    if (kvs_cn_cache_rpc(node, replica->endpoint, "rdma_alloc_slot", alloc_params, &alloc_outcome, err, err_len) != 0) {
        json_decref(alloc_params);
        return -1;
    }
    json_decref(alloc_params);
    result = kvs_result_object(&alloc_outcome);
    replica->slot_id = (uint32_t) json_integer_value(json_object_get(result, "slot_id"));
    replica->slot_epoch = (uint32_t) json_integer_value(json_object_get(result, "epoch"));

    record_json = kvs_cipher_record_to_json(record);
    if (record_json == NULL) {
        kvs_cache_rpc_outcome_cleanup(&alloc_outcome);
        kvs_errorf(err, err_len, "out of memory");
        return -1;
    }
    write_params = json_pack(
        "{sI,sI,s:o}",
        "slot_id",
        (json_int_t) replica->slot_id,
        "epoch",
        (json_int_t) replica->slot_epoch,
        "record",
        record_json
    );
    if (write_params == NULL) {
        kvs_cache_rpc_outcome_cleanup(&alloc_outcome);
        json_decref(record_json);
        kvs_errorf(err, err_len, "out of memory");
        return -1;
    }
    if (kvs_cn_cache_rpc(node, replica->endpoint, "rdma_write_slot", write_params, &write_outcome, err, err_len) != 0) {
        kvs_cache_rpc_outcome_cleanup(&alloc_outcome);
        json_decref(write_params);
        return -1;
    }
    json_decref(write_params);
    result = kvs_result_object(&write_outcome);
    if (!json_is_true(json_object_get(result, "write_ok"))) {
        kvs_errorf(
            err,
            err_len,
            "slot write rejected on %s: %s",
            replica->endpoint->node_id,
            json_string_value(json_object_get(result, "reason"))
        );
        kvs_cn_trace(
            node,
            "op=%s key=%s replica=%s attempt=%d prepare=slot-write-failed reason=%s alloc_transport=%s write_transport=%s",
            operation,
            key,
            replica->endpoint->node_id,
            attempt,
            json_string_value(json_object_get(result, "reason")),
            kvs_cache_transport_label(&alloc_outcome),
            kvs_cache_transport_label(&write_outcome)
        );
        kvs_cache_rpc_outcome_cleanup(&alloc_outcome);
        kvs_cache_rpc_outcome_cleanup(&write_outcome);
        return -1;
    }

    replica->slot_prepared = true;
    kvs_cn_trace(
        node,
        "op=%s key=%s replica=%s attempt=%d prepare=insert-ready slot=%u epoch=%u alloc_transport=%s write_transport=%s",
        operation,
        key,
        replica->endpoint->node_id,
        attempt,
        replica->slot_id,
        replica->slot_epoch,
        kvs_cache_transport_label(&alloc_outcome),
        kvs_cache_transport_label(&write_outcome)
    );
    kvs_cache_rpc_outcome_cleanup(&alloc_outcome);
    kvs_cache_rpc_outcome_cleanup(&write_outcome);
    return 0;
}

int kvs_cn_prepare_insert(
    kvs_cn_node *node,
    kvs_cn_replica_commit *replica,
    const kvs_cipher_record *record,
    const char *key,
    const char *operation,
    int attempt,
    char *err,
    size_t err_len
) {
    replica->slot_prepared = false;
    replica->cas_applied = false;
    return kvs_cn_prepare_insert_slot(node, replica, record, key, operation, attempt, err, err_len);
}

int kvs_cn_commit_insert(
    kvs_cn_node *node,
    const kvs_cn_replica_commit *replica,
    const char *key,
    const char *phase,
    const char *operation,
    int attempt,
    char *err,
    size_t err_len
) {
    kvs_cache_rpc_outcome cas_outcome;
    json_t *cas_params = NULL;
    json_t *result;

    cas_params = json_object();
    if (cas_params == NULL ||
        json_object_set_new(cas_params, "key", json_string(key)) != 0 ||
        json_object_set_new(cas_params, "expected_addr", json_null()) != 0 ||
        json_object_set_new(cas_params, "expected_epoch", json_null()) != 0 ||
        json_object_set_new(cas_params, "new_addr", json_integer((json_int_t) replica->slot_id)) != 0 ||
        json_object_set_new(cas_params, "new_epoch", json_integer((json_int_t) replica->slot_epoch)) != 0 ||
        json_object_set_new(
            cas_params,
            "private_addr",
            replica->have_private_addr ? json_integer((json_int_t) replica->private_addr) : json_null()
        ) != 0) {
        kvs_json_decref_safe(cas_params);
        kvs_errorf(err, err_len, "out of memory");
        return -1;
    }
    if (kvs_cn_cache_rpc(node, replica->endpoint, "rdma_cas_prime", cas_params, &cas_outcome, err, err_len) != 0) {
        json_decref(cas_params);
        return -1;
    }
    json_decref(cas_params);
    result = kvs_result_object(&cas_outcome);
    if (!json_is_true(json_object_get(result, "cas_ok"))) {
        kvs_errorf(
            err,
            err_len,
            "insert CAS rejected on %s during %s",
            replica->endpoint->node_id,
            phase
        );
        kvs_cn_trace(
            node,
            "op=%s key=%s replica=%s attempt=%d phase=%s cas=reject transport=%s",
            operation,
            key,
            replica->endpoint->node_id,
            attempt,
            phase,
            kvs_cache_transport_label(&cas_outcome)
        );
        kvs_cache_rpc_outcome_cleanup(&cas_outcome);
        return -1;
    }
    kvs_cn_trace(
        node,
        "op=%s key=%s replica=%s attempt=%d phase=%s cas=ok transport=%s",
        operation,
        key,
        replica->endpoint->node_id,
        attempt,
        phase,
        kvs_cache_transport_label(&cas_outcome)
    );
    kvs_cache_rpc_outcome_cleanup(&cas_outcome);
    return 0;
}

int kvs_cn_rollback_insert(
    kvs_cn_node *node,
    const kvs_cn_replica_commit *replica,
    const char *key,
    const char *operation,
    int attempt,
    char *err,
    size_t err_len
) {
    kvs_cache_rpc_outcome cas_outcome;
    json_t *cas_params = NULL;
    json_t *result;

    cas_params = json_object();
    if (cas_params == NULL ||
        json_object_set_new(cas_params, "key", json_string(key)) != 0 ||
        json_object_set_new(cas_params, "expected_addr", json_integer((json_int_t) replica->slot_id)) != 0 ||
        json_object_set_new(cas_params, "expected_epoch", json_integer((json_int_t) replica->slot_epoch)) != 0 ||
        json_object_set_new(cas_params, "new_addr", json_null()) != 0 ||
        json_object_set_new(cas_params, "new_epoch", json_null()) != 0 ||
        json_object_set_new(cas_params, "private_addr", json_null()) != 0 ||
        json_object_set_new(cas_params, "remove_on_success", json_true()) != 0) {
        kvs_json_decref_safe(cas_params);
        kvs_errorf(err, err_len, "out of memory");
        return -1;
    }
    if (kvs_cn_cache_rpc(node, replica->endpoint, "rdma_cas_prime", cas_params, &cas_outcome, err, err_len) != 0) {
        json_decref(cas_params);
        return -1;
    }
    json_decref(cas_params);
    result = kvs_result_object(&cas_outcome);
    if (!json_is_true(json_object_get(result, "cas_ok"))) {
        kvs_errorf(err, err_len, "insert rollback rejected on %s", replica->endpoint->node_id);
        kvs_cache_rpc_outcome_cleanup(&cas_outcome);
        return -1;
    }
    kvs_cn_trace(
        node,
        "op=%s key=%s replica=%s attempt=%d phase=rollback-remove cas=ok transport=%s",
        operation,
        key,
        replica->endpoint->node_id,
        attempt,
        kvs_cache_transport_label(&cas_outcome)
    );
    kvs_cache_rpc_outcome_cleanup(&cas_outcome);
    return 0;
}

int kvs_cn_promote_private_to_cache(
    kvs_cn_node *node,
    const char *key,
    const kvs_cipher_record *record,
    char *err,
    size_t err_len
) {
    const kvs_endpoint **replicas = NULL;
    size_t replica_count = 0u;
    int rc;

    if (kvs_select_replicas(node, key, &replicas, &replica_count, err, err_len) != 0) {
        return -1;
    }
    rc = kvs_cn_replicate_record(node, key, record, "read-promote", replicas, replica_count, err, err_len);
    free(replicas);
    return rc;
}
