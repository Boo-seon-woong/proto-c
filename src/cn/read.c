#include "internal.h"

#include "kvs/common.h"

#include <stdlib.h>
#include <string.h>

int kvs_cn_read(
    kvs_cn_node *node,
    const char *key,
    char **value_out,
    bool *found_out,
    char *err,
    size_t err_len
) {
    const kvs_endpoint *primary = kvs_primary_replica(node, key);
    int attempt;

    *value_out = NULL;
    *found_out = false;
    kvs_cn_trace(node, "op=read key=%s primary=%s start", key, primary->node_id);

    for (attempt = 1; attempt <= node->config.max_retries; ++attempt) {
        kvs_cache_rpc_outcome prime1_outcome;
        json_t *prime_params = json_pack("{s:s}", "key", key);

        if (prime_params == NULL) {
            kvs_errorf(err, err_len, "out of memory");
            return -1;
        }
        if (kvs_cn_cache_rpc(node, primary, "rdma_read_prime", prime_params, &prime1_outcome, err, err_len) != 0) {
            json_decref(prime_params);
            return -1;
        }
        json_decref(prime_params);

        if (!json_is_true(json_object_get(kvs_result_object(&prime1_outcome), "found"))) {
            json_t *private_params = json_pack("{s:s}", "key", key);
            json_t *private_response = NULL;
            json_t *private_result;
            kvs_cipher_record record;
            unsigned char *plaintext = NULL;
            size_t plaintext_len = 0u;

            if (private_params == NULL) {
                kvs_cache_rpc_outcome_cleanup(&prime1_outcome);
                kvs_errorf(err, err_len, "out of memory");
                return -1;
            }
            if (kvs_rpc_call(primary, "cpu_fetch_private", private_params, 3.0, &private_response, err, err_len) != 0) {
                json_decref(private_params);
                kvs_cache_rpc_outcome_cleanup(&prime1_outcome);
                return -1;
            }
            json_decref(private_params);

            private_result = json_object_get(private_response, "result");
            if (!json_is_true(json_object_get(private_result, "found"))) {
                kvs_cn_trace(
                    node,
                    "op=read key=%s primary=%s attempt=%d cache_lookup=miss transport=%s private_lookup=miss transport=tcp result=not_found",
                    key,
                    primary->node_id,
                    attempt,
                    kvs_cache_transport_label(&prime1_outcome)
                );
                json_decref(private_response);
                kvs_cache_rpc_outcome_cleanup(&prime1_outcome);
                *found_out = false;
                return 0;
            }

            kvs_cipher_record_init(&record);
            if (kvs_cipher_record_from_json(json_object_get(private_result, "record"), &record, err, err_len) != 0) {
                json_decref(private_response);
                kvs_cache_rpc_outcome_cleanup(&prime1_outcome);
                return -1;
            }
            kvs_cn_trace(
                node,
                "op=read key=%s primary=%s attempt=%d cache_lookup=miss transport=%s private_lookup=hit transport=tcp",
                key,
                primary->node_id,
                attempt,
                kvs_cache_transport_label(&prime1_outcome)
            );
            if (node->config.populate_cache_on_read_miss &&
                kvs_cn_promote_private_to_cache(node, key, &record, err, err_len) != 0) {
                kvs_cipher_record_cleanup(&record);
                json_decref(private_response);
                kvs_cache_rpc_outcome_cleanup(&prime1_outcome);
                return -1;
            }
            if (record.tombstone) {
                kvs_cipher_record_cleanup(&record);
                json_decref(private_response);
                kvs_cache_rpc_outcome_cleanup(&prime1_outcome);
                *found_out = false;
                return 0;
            }
            if (kvs_aead_decrypt(
                    &node->cipher,
                    &record,
                    (const unsigned char *) key,
                    strlen(key),
                    &plaintext,
                    &plaintext_len,
                    err,
                    err_len
                ) != 0) {
                kvs_cipher_record_cleanup(&record);
                json_decref(private_response);
                kvs_cache_rpc_outcome_cleanup(&prime1_outcome);
                return -1;
            }
            kvs_cipher_record_cleanup(&record);
            json_decref(private_response);
            kvs_cache_rpc_outcome_cleanup(&prime1_outcome);

            if (plaintext_len == KVS_TOMBSTONE_MARKER_LEN &&
                memcmp(plaintext, KVS_TOMBSTONE_MARKER, plaintext_len) == 0) {
                free(plaintext);
                *found_out = false;
                return 0;
            }
            *value_out = malloc(plaintext_len + 1u);
            if (*value_out == NULL) {
                free(plaintext);
                kvs_errorf(err, err_len, "out of memory");
                return -1;
            }
            memcpy(*value_out, plaintext, plaintext_len);
            (*value_out)[plaintext_len] = '\0';
            free(plaintext);
            *found_out = true;
            return 0;
        }

        {
            kvs_prime_entry_wire prime1;
            kvs_snapshot_status snapshot_status;
            memset(&prime1, 0, sizeof(prime1));
            if (kvs_prime_entry_wire_from_json(json_object_get(kvs_result_object(&prime1_outcome), "entry"), &prime1, err, err_len) != 0) {
                kvs_cache_rpc_outcome_cleanup(&prime1_outcome);
                return -1;
            }
            kvs_cache_rpc_outcome_cleanup(&prime1_outcome);
            snapshot_status = kvs_cn_snapshot_consensus(node, primary, key, &prime1, attempt, value_out, found_out, err, err_len);
            kvs_prime_entry_wire_cleanup(&prime1);
            if (snapshot_status == KVS_SNAPSHOT_RETRY) {
                continue;
            }
            return err[0] == '\0' ? 0 : -1;
        }
    }

    kvs_errorf(err, err_len, "snapshot read failed after max retries");
    return -1;
}
