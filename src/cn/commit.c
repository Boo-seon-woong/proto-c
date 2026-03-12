#include "internal.h"

#include "kvs/common.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void kvs_cn_set_failure(kvs_cn_replica_commit *replica, const char *fmt, ...) {
    va_list args;

    va_start(args, fmt);
    vsnprintf(replica->failure, sizeof(replica->failure), fmt, args);
    va_end(args);
}

static void kvs_cn_append_summary(char *summary, size_t summary_len, const char *text) {
    size_t used;

    if (summary == NULL || summary_len == 0u || text == NULL || text[0] == '\0') {
        return;
    }
    used = strlen(summary);
    if (used >= summary_len - 1u) {
        return;
    }
    snprintf(summary + used, summary_len - used, "%s%s", used == 0u ? "" : "; ", text);
}

static void kvs_cn_append_replica_failure(
    char *summary,
    size_t summary_len,
    const kvs_cn_replica_commit *replica
) {
    char line[320];

    if (replica->failure[0] == '\0') {
        return;
    }
    snprintf(line, sizeof(line), "%s: %s", replica->endpoint->node_id, replica->failure);
    kvs_cn_append_summary(summary, summary_len, line);
}

static int kvs_cn_load_replica_state(
    kvs_cn_node *node,
    kvs_cn_replica_commit *replica,
    const char *key,
    const char *operation,
    int attempt,
    char *err,
    size_t err_len
) {
    kvs_cache_rpc_outcome prime_outcome;
    json_t *prime_params = NULL;
    json_t *prime_result;

    prime_params = json_pack("{s:s}", "key", key);
    if (prime_params == NULL) {
        kvs_errorf(err, err_len, "out of memory");
        return -1;
    }
    if (kvs_cn_cache_rpc(node, replica->endpoint, "rdma_read_prime", prime_params, &prime_outcome, err, err_len) != 0) {
        json_decref(prime_params);
        return -1;
    }
    json_decref(prime_params);
    prime_result = kvs_result_object(&prime_outcome);
    if (json_is_true(json_object_get(prime_result, "found"))) {
        kvs_prime_entry_wire entry;

        memset(&entry, 0, sizeof(entry));
        if (kvs_prime_entry_wire_from_json(json_object_get(prime_result, "entry"), &entry, err, err_len) != 0) {
            kvs_cache_rpc_outcome_cleanup(&prime_outcome);
            return -1;
        }
        replica->prime_found = true;
        replica->prime_addr = entry.addr;
        replica->prime_epoch = entry.epoch;
        replica->have_private_addr = entry.has_private_addr;
        replica->private_addr = entry.private_addr;
        kvs_cn_trace(
            node,
            "op=%s key=%s replica=%s attempt=%d state=prime-hit addr=%u epoch=%u transport=%s",
            operation,
            key,
            replica->endpoint->node_id,
            attempt,
            replica->prime_addr,
            replica->prime_epoch,
            kvs_cache_transport_label(&prime_outcome)
        );
        kvs_prime_entry_wire_cleanup(&entry);
        kvs_cache_rpc_outcome_cleanup(&prime_outcome);
        return 0;
    }

    {
        json_t *private_params = json_pack("{s:s}", "key", key);
        json_t *private_response = NULL;
        json_t *private_result;

        if (private_params == NULL) {
            kvs_cache_rpc_outcome_cleanup(&prime_outcome);
            kvs_errorf(err, err_len, "out of memory");
            return -1;
        }
        if (kvs_rpc_call(replica->endpoint, "cpu_fetch_private", private_params, 3.0, &private_response, err, err_len) != 0) {
            json_decref(private_params);
            kvs_cache_rpc_outcome_cleanup(&prime_outcome);
            return -1;
        }
        json_decref(private_params);
        private_result = json_object_get(private_response, "result");
        if (json_is_true(json_object_get(private_result, "found"))) {
            replica->have_private_addr = true;
            replica->private_addr = (int) json_integer_value(json_object_get(private_result, "private_addr"));
        }
        kvs_cn_trace(
            node,
            "op=%s key=%s replica=%s attempt=%d state=prime-miss private_lookup=%s prime_transport=%s transport=tcp",
            operation,
            key,
            replica->endpoint->node_id,
            attempt,
            json_is_true(json_object_get(private_result, "found")) ? "hit" : "miss",
            kvs_cache_transport_label(&prime_outcome)
        );
        json_decref(private_response);
    }
    kvs_cache_rpc_outcome_cleanup(&prime_outcome);
    return 0;
}

static int kvs_cn_prepare_replica(
    kvs_cn_node *node,
    kvs_cn_replica_commit *replica,
    bool update_mode,
    const kvs_cipher_record *record,
    const char *key,
    const char *operation,
    int attempt,
    char *err,
    size_t err_len
) {
    if (update_mode) {
        return kvs_cn_prepare_update(node, replica, record, key, operation, attempt, err, err_len);
    }
    return kvs_cn_prepare_insert(node, replica, record, key, operation, attempt, err, err_len);
}

static int kvs_cn_commit_replica(
    kvs_cn_node *node,
    const kvs_cn_replica_commit *replica,
    bool update_mode,
    const char *key,
    uint32_t expected_addr,
    uint32_t expected_epoch,
    const char *phase,
    const char *operation,
    int attempt,
    char *err,
    size_t err_len
) {
    if (update_mode) {
        return kvs_cn_commit_update(
            node,
            replica,
            key,
            expected_addr,
            expected_epoch,
            phase,
            operation,
            attempt,
            err,
            err_len
        );
    }
    return kvs_cn_commit_insert(node, replica, key, phase, operation, attempt, err, err_len);
}

static void kvs_cn_rollback_backups(
    kvs_cn_node *node,
    kvs_cn_replica_commit *replicas,
    size_t replica_count,
    bool update_mode,
    const char *key,
    uint32_t expected_addr,
    uint32_t expected_epoch,
    const char *operation,
    int attempt,
    char *rollback_summary,
    size_t rollback_summary_len
) {
    size_t i;

    for (i = 1u; i < replica_count; ++i) {
        char err[256];

        if (!replicas[i].cas_applied) {
            continue;
        }
        err[0] = '\0';
        if (update_mode) {
            if (kvs_cn_rollback_update(
                    node,
                    &replicas[i],
                    key,
                    expected_addr,
                    expected_epoch,
                    operation,
                    attempt,
                    err,
                    sizeof(err)
                ) == 0) {
                replicas[i].cas_applied = false;
                continue;
            }
        } else {
            if (kvs_cn_rollback_insert(node, &replicas[i], key, operation, attempt, err, sizeof(err)) == 0) {
                replicas[i].cas_applied = false;
                continue;
            }
        }
        if (err[0] == '\0') {
            kvs_errorf(err, sizeof(err), "rollback failed on %s", replicas[i].endpoint->node_id);
        }
        kvs_cn_set_failure(&replicas[i], "%s", err);
        kvs_cn_append_replica_failure(rollback_summary, rollback_summary_len, &replicas[i]);
    }
}

static void kvs_cn_collect_failures(
    char *summary,
    size_t summary_len,
    const kvs_cn_replica_commit *replicas,
    size_t replica_count
) {
    size_t i;

    for (i = 0u; i < replica_count; ++i) {
        kvs_cn_append_replica_failure(summary, summary_len, &replicas[i]);
    }
}

int kvs_cn_replicate_record(
    kvs_cn_node *node,
    const char *key,
    const kvs_cipher_record *record,
    const char *operation,
    const kvs_endpoint **replicas,
    size_t replica_count,
    char *err,
    size_t err_len
) {
    size_t quorum = (replica_count / 2u) + 1u;
    size_t required_backup_successes = quorum > 0u ? quorum - 1u : 0u;
    char last_error[1024] = "";
    int attempt;

    for (attempt = 1; attempt <= node->config.max_retries; ++attempt) {
        kvs_cn_replica_commit *plans;
        bool update_mode;
        uint32_t expected_addr = 0;
        uint32_t expected_epoch = 0;
        size_t backup_successes = 0u;
        char summary[1024] = "";
        char rollback_summary[1024] = "";
        size_t i;

        plans = calloc(replica_count, sizeof(*plans));
        if (plans == NULL) {
            kvs_errorf(err, err_len, "out of memory");
            return -1;
        }
        for (i = 0u; i < replica_count; ++i) {
            plans[i].endpoint = replicas[i];
        }

        if (kvs_cn_load_replica_state(node, &plans[0], key, operation, attempt, err, err_len) != 0) {
            snprintf(last_error, sizeof(last_error), "attempt %d primary state fetch failed: %s", attempt, err);
            free(plans);
            continue;
        }
        update_mode = plans[0].prime_found;
        expected_addr = plans[0].prime_addr;
        expected_epoch = plans[0].prime_epoch;

        for (i = 0u; i < replica_count; ++i) {
            char phase_err[256];

            phase_err[0] = '\0';
            if (i > 0u && kvs_cn_load_replica_state(node, &plans[i], key, operation, attempt, phase_err, sizeof(phase_err)) != 0) {
                kvs_cn_set_failure(&plans[i], "%s", phase_err);
                continue;
            }
            if (kvs_cn_prepare_replica(node, &plans[i], update_mode, record, key, operation, attempt, phase_err, sizeof(phase_err)) != 0) {
                kvs_cn_set_failure(&plans[i], "%s", phase_err);
            }
        }

        if (!plans[0].slot_prepared) {
            if (plans[0].failure[0] == '\0') {
                kvs_cn_set_failure(&plans[0], "primary prepare did not complete");
            }
            kvs_cn_collect_failures(summary, sizeof(summary), plans, replica_count);
            snprintf(last_error, sizeof(last_error), "attempt %d primary prepare failed: %s", attempt, summary);
            free(plans);
            continue;
        }

        for (i = 1u; i < replica_count; ++i) {
            char phase_err[256];

            if (!plans[i].slot_prepared) {
                continue;
            }
            phase_err[0] = '\0';
            if (kvs_cn_commit_replica(
                    node,
                    &plans[i],
                    update_mode,
                    key,
                    expected_addr,
                    expected_epoch,
                    "backup",
                    operation,
                    attempt,
                    phase_err,
                    sizeof(phase_err)
                ) == 0) {
                plans[i].cas_applied = true;
                backup_successes += 1u;
            } else {
                kvs_cn_set_failure(&plans[i], "%s", phase_err);
            }
        }

        if (backup_successes < required_backup_successes) {
            kvs_cn_rollback_backups(
                node,
                plans,
                replica_count,
                update_mode,
                key,
                expected_addr,
                expected_epoch,
                operation,
                attempt,
                rollback_summary,
                sizeof(rollback_summary)
            );
            kvs_cn_collect_failures(summary, sizeof(summary), plans, replica_count);
            snprintf(
                last_error,
                sizeof(last_error),
                "attempt %d quorum failed: backups=%zu/%zu primary=%s%s%s",
                attempt,
                backup_successes,
                required_backup_successes,
                summary[0] != '\0' ? summary : "no backup details",
                rollback_summary[0] != '\0' ? " rollback=" : "",
                rollback_summary
            );
            free(plans);
            continue;
        }

        {
            char phase_err[256];

            phase_err[0] = '\0';
            if (kvs_cn_commit_replica(
                    node,
                    &plans[0],
                    update_mode,
                    key,
                    expected_addr,
                    expected_epoch,
                    "primary",
                    operation,
                    attempt,
                    phase_err,
                    sizeof(phase_err)
                ) == 0) {
                plans[0].cas_applied = true;
                kvs_cn_trace(
                    node,
                    "op=%s key=%s attempt=%d quorum=%zu/%zu commit=primary-visible",
                    operation,
                    key,
                    attempt,
                    backup_successes + 1u,
                    quorum
                );
                free(plans);
                return 0;
            }

            kvs_cn_set_failure(&plans[0], "%s", phase_err);
            kvs_cn_rollback_backups(
                node,
                plans,
                replica_count,
                update_mode,
                key,
                expected_addr,
                expected_epoch,
                operation,
                attempt,
                rollback_summary,
                sizeof(rollback_summary)
            );
            kvs_cn_collect_failures(summary, sizeof(summary), plans, replica_count);
            snprintf(
                last_error,
                sizeof(last_error),
                "attempt %d primary commit failed after quorum: backups=%zu/%zu details=%s%s%s",
                attempt,
                backup_successes,
                required_backup_successes,
                summary[0] != '\0' ? summary : "primary CAS failed",
                rollback_summary[0] != '\0' ? " rollback=" : "",
                rollback_summary
            );
        }
        free(plans);
    }

    if (last_error[0] == '\0') {
        kvs_errorf(err, err_len, "max retry exceeded for quorum commit");
    } else {
        kvs_errorf(err, err_len, "%s", last_error);
    }
    return -1;
}

int kvs_cn_encrypt_and_replicate(
    kvs_cn_node *node,
    const char *key,
    const unsigned char *payload,
    size_t payload_len,
    bool tombstone,
    const char *operation,
    char *err,
    size_t err_len
) {
    kvs_cipher_record record;
    const kvs_endpoint **replicas = NULL;
    size_t replica_count = 0u;
    int rc;

    kvs_cipher_record_init(&record);
    if (kvs_aead_encrypt(
            &node->cipher,
            payload,
            payload_len,
            (const unsigned char *) key,
            strlen(key),
            &record,
            err,
            err_len
        ) != 0) {
        return -1;
    }
    record.tombstone = tombstone;
    if (kvs_select_replicas(node, key, &replicas, &replica_count, err, err_len) != 0) {
        kvs_cipher_record_cleanup(&record);
        return -1;
    }
    kvs_cn_trace(
        node,
        "op=%s key=%s start replica_count=%zu quorum=%zu primary=%s",
        operation,
        key,
        replica_count,
        (replica_count / 2u) + 1u,
        replicas[0]->node_id
    );
    rc = kvs_cn_replicate_record(node, key, &record, operation, replicas, replica_count, err, err_len);
    kvs_cn_trace(node, "op=%s key=%s done", operation, key);
    free(replicas);
    kvs_cipher_record_cleanup(&record);
    return rc;
}
