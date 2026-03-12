#include "internal.h"

#include "kvs/common.h"

#include <stdlib.h>
#include <string.h>

kvs_snapshot_status kvs_cn_snapshot_consensus(
    kvs_cn_node *node,
    const kvs_endpoint *endpoint,
    const char *key,
    const kvs_prime_entry_wire *prime1,
    int attempt,
    char **value_out,
    bool *found_out,
    char *err,
    size_t err_len
) {
    kvs_cache_rpc_outcome slot_outcome;
    kvs_cache_rpc_outcome prime2_outcome;
    kvs_prime_entry_wire prime2;
    kvs_cache_slot slot;
    json_t *slot_params = NULL;
    json_t *prime2_params = NULL;
    unsigned char *plaintext = NULL;
    size_t plaintext_len = 0u;

    memset(&prime2, 0, sizeof(prime2));
    memset(&slot, 0, sizeof(slot));

    slot_params = json_pack("{sI}", "slot_id", (json_int_t) prime1->addr);
    prime2_params = json_pack("{s:s}", "key", key);
    if (slot_params == NULL || prime2_params == NULL) {
        kvs_json_decref_safe(slot_params);
        kvs_json_decref_safe(prime2_params);
        kvs_errorf(err, err_len, "out of memory");
        return KVS_SNAPSHOT_COMPLETE;
    }
    if (kvs_cn_cache_rpc(node, endpoint, "rdma_read_slot", slot_params, &slot_outcome, err, err_len) != 0 ||
        kvs_cn_cache_rpc(node, endpoint, "rdma_read_prime", prime2_params, &prime2_outcome, err, err_len) != 0) {
        json_decref(slot_params);
        json_decref(prime2_params);
        return KVS_SNAPSHOT_COMPLETE;
    }
    json_decref(slot_params);
    json_decref(prime2_params);

    if (!json_is_true(json_object_get(kvs_result_object(&prime2_outcome), "found"))) {
        kvs_cn_trace(
            node,
            "op=read key=%s primary=%s attempt=%d cache_lookup=hit snapshot=prime-disappeared retry=yes",
            key,
            endpoint->node_id,
            attempt
        );
        kvs_cache_rpc_outcome_cleanup(&slot_outcome);
        kvs_cache_rpc_outcome_cleanup(&prime2_outcome);
        return KVS_SNAPSHOT_RETRY;
    }
    if (kvs_prime_entry_wire_from_json(json_object_get(kvs_result_object(&prime2_outcome), "entry"), &prime2, err, err_len) != 0) {
        kvs_cache_rpc_outcome_cleanup(&slot_outcome);
        kvs_cache_rpc_outcome_cleanup(&prime2_outcome);
        return KVS_SNAPSHOT_COMPLETE;
    }
    if (prime1->addr != prime2.addr || prime1->epoch != prime2.epoch) {
        kvs_cn_trace(
            node,
            "op=read key=%s primary=%s attempt=%d cache_lookup=hit snapshot=prime-changed retry=yes",
            key,
            endpoint->node_id,
            attempt
        );
        kvs_prime_entry_wire_cleanup(&prime2);
        kvs_cache_rpc_outcome_cleanup(&slot_outcome);
        kvs_cache_rpc_outcome_cleanup(&prime2_outcome);
        return KVS_SNAPSHOT_RETRY;
    }
    if (!json_is_true(json_object_get(kvs_result_object(&slot_outcome), "found"))) {
        kvs_cn_trace(
            node,
            "op=read key=%s primary=%s attempt=%d cache_lookup=hit slot_lookup=miss retry=yes",
            key,
            endpoint->node_id,
            attempt
        );
        kvs_prime_entry_wire_cleanup(&prime2);
        kvs_cache_rpc_outcome_cleanup(&slot_outcome);
        kvs_cache_rpc_outcome_cleanup(&prime2_outcome);
        return KVS_SNAPSHOT_RETRY;
    }
    if (kvs_cache_slot_from_json(json_object_get(kvs_result_object(&slot_outcome), "slot"), &slot, err, err_len) != 0) {
        kvs_prime_entry_wire_cleanup(&prime2);
        kvs_cache_rpc_outcome_cleanup(&slot_outcome);
        kvs_cache_rpc_outcome_cleanup(&prime2_outcome);
        return KVS_SNAPSHOT_COMPLETE;
    }
    if (slot.epoch != prime1->epoch) {
        kvs_cn_trace(
            node,
            "op=read key=%s primary=%s attempt=%d cache_lookup=hit slot_epoch_mismatch retry=yes",
            key,
            endpoint->node_id,
            attempt
        );
        kvs_cache_slot_cleanup(&slot);
        kvs_prime_entry_wire_cleanup(&prime2);
        kvs_cache_rpc_outcome_cleanup(&slot_outcome);
        kvs_cache_rpc_outcome_cleanup(&prime2_outcome);
        return KVS_SNAPSHOT_RETRY;
    }
    if (slot.record.tombstone) {
        kvs_cache_slot_cleanup(&slot);
        kvs_prime_entry_wire_cleanup(&prime2);
        kvs_cache_rpc_outcome_cleanup(&slot_outcome);
        kvs_cache_rpc_outcome_cleanup(&prime2_outcome);
        *found_out = false;
        return KVS_SNAPSHOT_COMPLETE;
    }
    if (kvs_aead_decrypt(
            &node->cipher,
            &slot.record,
            (const unsigned char *) key,
            strlen(key),
            &plaintext,
            &plaintext_len,
            err,
            err_len
        ) != 0) {
        kvs_cache_slot_cleanup(&slot);
        kvs_prime_entry_wire_cleanup(&prime2);
        kvs_cache_rpc_outcome_cleanup(&slot_outcome);
        kvs_cache_rpc_outcome_cleanup(&prime2_outcome);
        return KVS_SNAPSHOT_COMPLETE;
    }
    kvs_cache_slot_cleanup(&slot);
    kvs_prime_entry_wire_cleanup(&prime2);
    kvs_cache_rpc_outcome_cleanup(&slot_outcome);
    kvs_cache_rpc_outcome_cleanup(&prime2_outcome);

    if (plaintext_len == KVS_TOMBSTONE_MARKER_LEN &&
        memcmp(plaintext, KVS_TOMBSTONE_MARKER, plaintext_len) == 0) {
        free(plaintext);
        *found_out = false;
        return KVS_SNAPSHOT_COMPLETE;
    }
    *value_out = malloc(plaintext_len + 1u);
    if (*value_out == NULL) {
        free(plaintext);
        kvs_errorf(err, err_len, "out of memory");
        return KVS_SNAPSHOT_COMPLETE;
    }
    memcpy(*value_out, plaintext, plaintext_len);
    (*value_out)[plaintext_len] = '\0';
    free(plaintext);
    *found_out = true;
    return KVS_SNAPSHOT_COMPLETE;
}
