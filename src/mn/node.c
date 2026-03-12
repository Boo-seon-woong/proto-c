#include "kvs/mn_node.h"

#include "kvs/common.h"
#include "kvs/models.h"
#include "kvs/rdma_rpc.h"
#include "kvs/rpc.h"

#include <jansson.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct kvs_prime_entry {
    char *key;
    uint32_t addr;
    uint32_t epoch;
    int private_addr;
    bool has_private_addr;
    bool valid;
    struct kvs_prime_entry *bucket_next;
    struct kvs_prime_entry *lru_prev;
    struct kvs_prime_entry *lru_next;
} kvs_prime_entry;

typedef struct kvs_private_index_entry {
    char *key;
    int addr;
    struct kvs_private_index_entry *next;
} kvs_private_index_entry;

typedef struct {
    bool occupied;
    uint32_t epoch;
    kvs_cipher_record record;
} kvs_slot_store;

typedef struct {
    bool present;
    char *key;
    kvs_cipher_record record;
} kvs_private_record;

struct kvs_mn_node {
    kvs_mn_config config;
    pthread_mutex_t lock;

    kvs_slot_store *slots;
    size_t slot_capacity;
    size_t cache_slot_count;
    uint32_t next_slot_id;
    uint32_t *free_slots;
    size_t free_slot_count;
    size_t free_slot_capacity;

    kvs_prime_entry **prime_buckets;
    size_t prime_bucket_count;
    size_t prime_count;
    kvs_prime_entry *lru_head;
    kvs_prime_entry *lru_tail;

    kvs_private_index_entry **private_buckets;
    size_t private_bucket_count;
    kvs_private_record *private_records;
    size_t private_capacity;
    size_t private_count;
    int next_private_addr;

    kvs_tcp_server tcp_server;
    kvs_rdma_server rdma_server;
    bool rdma_started;
};

static const char *KVS_RDMA_ACTIONS[] = {
    "rdma_alloc_slot",
    "rdma_write_slot",
    "rdma_read_slot",
    "rdma_read_prime",
    "rdma_cas_prime",
};

static void kvs_json_decref_safe(json_t *value) {
    if (value != NULL) {
        json_decref(value);
    }
}

static size_t kvs_bucket_index(const char *key, size_t bucket_count) {
    return (size_t) (kvs_sha256_hash64(key) % bucket_count);
}

static void kvs_free_mn_config_copy(kvs_mn_config *config) {
    free(config->node_id);
    free(config->listen_host);
    free(config->state_dir);
    free(config->rdma_listen_host);
    memset(config, 0, sizeof(*config));
}

static int kvs_copy_mn_config(const kvs_mn_config *src, kvs_mn_config *dst, char *err, size_t err_len) {
    memset(dst, 0, sizeof(*dst));
    dst->node_id = kvs_strdup(src->node_id);
    dst->listen_host = kvs_strdup(src->listen_host);
    dst->state_dir = kvs_strdup(src->state_dir);
    dst->rdma_listen_host = kvs_strdup(src->rdma_listen_host);
    if (dst->node_id == NULL || dst->listen_host == NULL || dst->state_dir == NULL ||
        (src->rdma_listen_host != NULL && dst->rdma_listen_host == NULL)) {
        kvs_free_mn_config_copy(dst);
        kvs_errorf(err, err_len, "out of memory");
        return -1;
    }
    dst->listen_port = src->listen_port;
    dst->cache_capacity = src->cache_capacity;
    dst->require_tdx = src->require_tdx;
    dst->enable_rdma_server = src->enable_rdma_server;
    dst->has_rdma_listen_port = src->has_rdma_listen_port;
    dst->rdma_listen_port = src->rdma_listen_port;
    dst->require_rdma_server = src->require_rdma_server;
    return 0;
}

static char *kvs_private_state_path(const kvs_mn_node *node) {
    return kvs_path_join(node->config.state_dir, "private_store.json");
}

static void kvs_private_record_cleanup(kvs_private_record *record) {
    if (record == NULL) {
        return;
    }
    free(record->key);
    kvs_cipher_record_cleanup(&record->record);
    memset(record, 0, sizeof(*record));
}

static void kvs_prime_entry_free(kvs_prime_entry *entry) {
    if (entry == NULL) {
        return;
    }
    free(entry->key);
    free(entry);
}

static void kvs_prime_lru_remove(kvs_mn_node *node, kvs_prime_entry *entry) {
    if (entry->lru_prev != NULL) {
        entry->lru_prev->lru_next = entry->lru_next;
    } else {
        node->lru_head = entry->lru_next;
    }
    if (entry->lru_next != NULL) {
        entry->lru_next->lru_prev = entry->lru_prev;
    } else {
        node->lru_tail = entry->lru_prev;
    }
    entry->lru_prev = NULL;
    entry->lru_next = NULL;
}

static void kvs_prime_lru_push_tail(kvs_mn_node *node, kvs_prime_entry *entry) {
    entry->lru_prev = node->lru_tail;
    entry->lru_next = NULL;
    if (node->lru_tail != NULL) {
        node->lru_tail->lru_next = entry;
    } else {
        node->lru_head = entry;
    }
    node->lru_tail = entry;
}

static void kvs_prime_touch_locked(kvs_mn_node *node, kvs_prime_entry *entry) {
    if (entry == NULL || node->lru_tail == entry) {
        return;
    }
    kvs_prime_lru_remove(node, entry);
    kvs_prime_lru_push_tail(node, entry);
}

static kvs_prime_entry *kvs_prime_find_locked(const kvs_mn_node *node, const char *key) {
    kvs_prime_entry *entry = node->prime_buckets[kvs_bucket_index(key, node->prime_bucket_count)];
    while (entry != NULL) {
        if (strcmp(entry->key, key) == 0) {
            return entry;
        }
        entry = entry->bucket_next;
    }
    return NULL;
}

static int kvs_prime_insert_locked(kvs_mn_node *node, kvs_prime_entry *entry, char *err, size_t err_len) {
    size_t bucket = kvs_bucket_index(entry->key, node->prime_bucket_count);
    if (entry->key == NULL) {
        kvs_errorf(err, err_len, "prime entry key is null");
        return -1;
    }
    entry->bucket_next = node->prime_buckets[bucket];
    node->prime_buckets[bucket] = entry;
    kvs_prime_lru_push_tail(node, entry);
    node->prime_count += 1u;
    return 0;
}

static void kvs_prime_remove_locked(kvs_mn_node *node, const char *key) {
    size_t bucket = kvs_bucket_index(key, node->prime_bucket_count);
    kvs_prime_entry *entry = node->prime_buckets[bucket];
    kvs_prime_entry *prev = NULL;
    while (entry != NULL) {
        if (strcmp(entry->key, key) == 0) {
            if (prev != NULL) {
                prev->bucket_next = entry->bucket_next;
            } else {
                node->prime_buckets[bucket] = entry->bucket_next;
            }
            kvs_prime_lru_remove(node, entry);
            node->prime_count -= 1u;
            kvs_prime_entry_free(entry);
            return;
        }
        prev = entry;
        entry = entry->bucket_next;
    }
}

static kvs_private_index_entry *kvs_private_index_find_locked(const kvs_mn_node *node, const char *key) {
    kvs_private_index_entry *entry = node->private_buckets[kvs_bucket_index(key, node->private_bucket_count)];
    while (entry != NULL) {
        if (strcmp(entry->key, key) == 0) {
            return entry;
        }
        entry = entry->next;
    }
    return NULL;
}

static int kvs_private_index_set_locked(kvs_mn_node *node, const char *key, int addr, char *err, size_t err_len) {
    size_t bucket = kvs_bucket_index(key, node->private_bucket_count);
    kvs_private_index_entry *entry = kvs_private_index_find_locked(node, key);
    if (entry != NULL) {
        entry->addr = addr;
        return 0;
    }
    entry = calloc(1, sizeof(*entry));
    if (entry == NULL) {
        kvs_errorf(err, err_len, "out of memory");
        return -1;
    }
    entry->key = kvs_strdup(key);
    if (entry->key == NULL) {
        free(entry);
        kvs_errorf(err, err_len, "out of memory");
        return -1;
    }
    entry->addr = addr;
    entry->next = node->private_buckets[bucket];
    node->private_buckets[bucket] = entry;
    return 0;
}

static void kvs_private_index_remove_locked(kvs_mn_node *node, const char *key) {
    size_t bucket = kvs_bucket_index(key, node->private_bucket_count);
    kvs_private_index_entry *entry = node->private_buckets[bucket];
    kvs_private_index_entry *prev = NULL;
    while (entry != NULL) {
        if (strcmp(entry->key, key) == 0) {
            if (prev != NULL) {
                prev->next = entry->next;
            } else {
                node->private_buckets[bucket] = entry->next;
            }
            free(entry->key);
            free(entry);
            return;
        }
        prev = entry;
        entry = entry->next;
    }
}

static int kvs_ensure_slot_capacity(kvs_mn_node *node, uint32_t slot_id, char *err, size_t err_len) {
    size_t needed = (size_t) slot_id + 1u;
    size_t capacity = node->slot_capacity;
    kvs_slot_store *grown;

    if (needed <= node->slot_capacity) {
        return 0;
    }
    if (capacity == 0u) {
        capacity = 16u;
    }
    while (capacity < needed) {
        capacity *= 2u;
    }
    grown = realloc(node->slots, capacity * sizeof(*node->slots));
    if (grown == NULL) {
        kvs_errorf(err, err_len, "out of memory");
        return -1;
    }
    memset(grown + node->slot_capacity, 0, (capacity - node->slot_capacity) * sizeof(*grown));
    node->slots = grown;
    node->slot_capacity = capacity;
    return 0;
}

static int kvs_ensure_free_slot_capacity(kvs_mn_node *node, char *err, size_t err_len) {
    size_t capacity = node->free_slot_capacity == 0u ? 16u : node->free_slot_capacity * 2u;
    uint32_t *grown;
    if (node->free_slot_count < node->free_slot_capacity) {
        return 0;
    }
    grown = realloc(node->free_slots, capacity * sizeof(*node->free_slots));
    if (grown == NULL) {
        kvs_errorf(err, err_len, "out of memory");
        return -1;
    }
    node->free_slots = grown;
    node->free_slot_capacity = capacity;
    return 0;
}

static int kvs_ensure_private_capacity(kvs_mn_node *node, int addr, char *err, size_t err_len) {
    size_t needed = (size_t) addr + 1u;
    size_t capacity = node->private_capacity;
    kvs_private_record *grown;
    if (needed <= node->private_capacity) {
        return 0;
    }
    if (capacity == 0u) {
        capacity = 16u;
    }
    while (capacity < needed) {
        capacity *= 2u;
    }
    grown = realloc(node->private_records, capacity * sizeof(*node->private_records));
    if (grown == NULL) {
        kvs_errorf(err, err_len, "out of memory");
        return -1;
    }
    memset(grown + node->private_capacity, 0, (capacity - node->private_capacity) * sizeof(*grown));
    node->private_records = grown;
    node->private_capacity = capacity;
    return 0;
}

static int kvs_persist_private_state_locked(kvs_mn_node *node, char *err, size_t err_len) {
    json_t *root = NULL;
    json_t *private_by_addr = NULL;
    json_t *private_key_index = NULL;
    char *path = NULL;
    size_t i;
    int rc = -1;

    root = json_object();
    private_by_addr = json_object();
    private_key_index = json_object();
    if (root == NULL || private_by_addr == NULL || private_key_index == NULL) {
        kvs_errorf(err, err_len, "out of memory");
        goto done;
    }

    if (json_object_set_new(root, "next_private_addr", json_integer((json_int_t) node->next_private_addr)) != 0 ||
        json_object_set_new(root, "private_by_addr", private_by_addr) != 0 ||
        json_object_set_new(root, "private_key_index", private_key_index) != 0) {
        kvs_errorf(err, err_len, "out of memory");
        goto done;
    }
    private_by_addr = NULL;
    private_key_index = NULL;

    for (i = 1u; i < node->private_capacity; ++i) {
        kvs_private_record *record = &node->private_records[i];
        char key_text[32];
        json_t *payload;
        json_t *record_json;
        if (!record->present) {
            continue;
        }
        record_json = kvs_cipher_record_to_json(&record->record);
        payload = json_object();
        if (record_json == NULL || payload == NULL ||
            json_object_set_new(payload, "key", json_string(record->key)) != 0 ||
            json_object_set_new(payload, "record", record_json) != 0) {
            json_decref(record_json);
            json_decref(payload);
            kvs_errorf(err, err_len, "out of memory");
            goto done;
        }
        snprintf(key_text, sizeof(key_text), "%zu", i);
        if (json_object_set_new(json_object_get(root, "private_by_addr"), key_text, payload) != 0 ||
            json_object_set_new(json_object_get(root, "private_key_index"), record->key, json_integer((json_int_t) i)) != 0) {
            json_decref(payload);
            kvs_errorf(err, err_len, "out of memory");
            goto done;
        }
    }

    path = kvs_private_state_path(node);
    if (path == NULL) {
        kvs_errorf(err, err_len, "out of memory");
        goto done;
    }
    if (json_dump_file(root, path, JSON_INDENT(2) | JSON_SORT_KEYS) != 0) {
        kvs_errorf(err, err_len, "failed to persist %s", path);
        goto done;
    }
    rc = 0;

done:
    free(path);
    kvs_json_decref_safe(root);
    kvs_json_decref_safe(private_by_addr);
    kvs_json_decref_safe(private_key_index);
    return rc;
}

static int kvs_release_slot_locked(kvs_mn_node *node, uint32_t slot_id, char *err, size_t err_len) {
    kvs_slot_store *slot;
    if ((size_t) slot_id >= node->slot_capacity) {
        return 0;
    }
    slot = &node->slots[slot_id];
    if (!slot->occupied) {
        return 0;
    }
    kvs_cipher_record_cleanup(&slot->record);
    slot->occupied = false;
    node->cache_slot_count -= 1u;
    if (kvs_ensure_free_slot_capacity(node, err, err_len) != 0) {
        return -1;
    }
    node->free_slots[node->free_slot_count++] = slot_id;
    return 0;
}

static int kvs_collect_orphan_slots_locked(kvs_mn_node *node, char *err, size_t err_len) {
    bool *referenced;
    size_t i;
    size_t bucket;

    if (node->slot_capacity == 0u) {
        return 0;
    }
    referenced = calloc(node->slot_capacity, sizeof(*referenced));
    if (referenced == NULL) {
        kvs_errorf(err, err_len, "out of memory");
        return -1;
    }
    for (bucket = 0u; bucket < node->prime_bucket_count; ++bucket) {
        kvs_prime_entry *entry = node->prime_buckets[bucket];
        while (entry != NULL) {
            if ((size_t) entry->addr < node->slot_capacity) {
                referenced[entry->addr] = true;
            }
            entry = entry->bucket_next;
        }
    }
    for (i = 0u; i < node->slot_capacity; ++i) {
        if (node->slots[i].occupied && !referenced[i]) {
            if (kvs_release_slot_locked(node, (uint32_t) i, err, err_len) != 0) {
                free(referenced);
                return -1;
            }
        }
    }
    free(referenced);
    return 0;
}

static int kvs_write_private_locked(
    kvs_mn_node *node,
    const char *key,
    const kvs_cipher_record *record,
    int *addr_out,
    char *err,
    size_t err_len
) {
    kvs_private_index_entry *index_entry = kvs_private_index_find_locked(node, key);
    int addr = index_entry != NULL ? index_entry->addr : node->next_private_addr;
    kvs_private_record *target;

    if (index_entry == NULL) {
        node->next_private_addr += 1;
    }
    if (kvs_ensure_private_capacity(node, addr, err, err_len) != 0) {
        return -1;
    }
    target = &node->private_records[addr];
    if (!target->present) {
        node->private_count += 1u;
    } else {
        kvs_private_record_cleanup(target);
    }
    target->key = kvs_strdup(key);
    if (target->key == NULL) {
        kvs_errorf(err, err_len, "out of memory");
        return -1;
    }
    if (kvs_cipher_record_copy(&target->record, record, err, err_len) != 0) {
        kvs_private_record_cleanup(target);
        return -1;
    }
    target->present = true;
    if (kvs_private_index_set_locked(node, key, addr, err, err_len) != 0) {
        kvs_private_record_cleanup(target);
        return -1;
    }
    if (kvs_persist_private_state_locked(node, err, err_len) != 0) {
        return -1;
    }
    *addr_out = addr;
    return 0;
}

static int kvs_delete_private_locked(kvs_mn_node *node, const char *key, char *err, size_t err_len) {
    kvs_private_index_entry *entry = kvs_private_index_find_locked(node, key);
    if (entry == NULL) {
        return 0;
    }
    if ((size_t) entry->addr < node->private_capacity && node->private_records[entry->addr].present) {
        kvs_private_record_cleanup(&node->private_records[entry->addr]);
        node->private_count -= 1u;
    }
    kvs_private_index_remove_locked(node, key);
    return kvs_persist_private_state_locked(node, err, err_len);
}

static int kvs_evict_one_locked(kvs_mn_node *node, char *err, size_t err_len) {
    kvs_prime_entry *victim = NULL;
    kvs_slot_store *slot = NULL;
    char *key_copy = NULL;
    int rc = -1;

    if (kvs_collect_orphan_slots_locked(node, err, err_len) != 0) {
        return -1;
    }
    victim = node->lru_head;
    if (victim == NULL) {
        kvs_errorf(err, err_len, "cannot evict: no authoritative prime entries");
        return -1;
    }
    key_copy = kvs_strdup(victim->key);
    if (key_copy == NULL) {
        kvs_errorf(err, err_len, "out of memory");
        return -1;
    }
    if ((size_t) victim->addr < node->slot_capacity) {
        slot = &node->slots[victim->addr];
    }
    if (slot != NULL && slot->occupied) {
        if (slot->record.tombstone) {
            if (kvs_delete_private_locked(node, key_copy, err, err_len) != 0) {
                goto done;
            }
        } else {
            int ignored_addr = 0;
            if (kvs_write_private_locked(node, key_copy, &slot->record, &ignored_addr, err, err_len) != 0) {
                goto done;
            }
        }
        if (kvs_release_slot_locked(node, victim->addr, err, err_len) != 0) {
            goto done;
        }
    }
    kvs_prime_remove_locked(node, key_copy);
    rc = 0;

done:
    free(key_copy);
    return rc;
}

static int kvs_allocate_slot_locked(kvs_mn_node *node, uint32_t *slot_id_out, uint32_t *epoch_out, char *err, size_t err_len) {
    uint32_t slot_id;
    uint32_t epoch;
    kvs_slot_store *slot;

    if (kvs_collect_orphan_slots_locked(node, err, err_len) != 0) {
        return -1;
    }
    if (node->cache_slot_count >= node->config.cache_capacity) {
        if (kvs_evict_one_locked(node, err, err_len) != 0) {
            return -1;
        }
        if (kvs_collect_orphan_slots_locked(node, err, err_len) != 0) {
            return -1;
        }
        if (node->cache_slot_count >= node->config.cache_capacity) {
            kvs_errorf(err, err_len, "cache remains full after eviction");
            return -1;
        }
    }

    if (node->free_slot_count > 0u) {
        slot_id = node->free_slots[--node->free_slot_count];
        epoch = node->slots[slot_id].epoch + 1u;
    } else {
        slot_id = node->next_slot_id++;
        epoch = 1u;
    }

    if (kvs_ensure_slot_capacity(node, slot_id, err, err_len) != 0) {
        return -1;
    }
    slot = &node->slots[slot_id];
    kvs_cipher_record_cleanup(&slot->record);
    slot->occupied = true;
    slot->epoch = epoch;
    kvs_cipher_record_init(&slot->record);
    node->cache_slot_count += 1u;
    *slot_id_out = slot_id;
    *epoch_out = epoch;
    return 0;
}

static int kvs_load_private_state(kvs_mn_node *node, char *err, size_t err_len) {
    char *path = NULL;
    json_t *root = NULL;
    json_t *private_by_addr;
    const char *addr_key;
    json_t *payload;
    json_error_t json_error;

    if (kvs_mkdir_p(node->config.state_dir, err, err_len) != 0) {
        return -1;
    }
    path = kvs_private_state_path(node);
    if (path == NULL) {
        kvs_errorf(err, err_len, "out of memory");
        return -1;
    }
    root = json_load_file(path, 0, &json_error);
    if (root == NULL) {
        free(path);
        return 0;
    }
    free(path);

    node->next_private_addr = (int) json_integer_value(json_object_get(root, "next_private_addr"));
    if (node->next_private_addr <= 0) {
        node->next_private_addr = 1;
    }
    private_by_addr = json_object_get(root, "private_by_addr");
    if (json_is_object(private_by_addr)) {
        json_object_foreach(private_by_addr, addr_key, payload) {
            int addr = atoi(addr_key);
            const char *key;
            json_t *record_json;
            kvs_private_record *record;
            if (addr <= 0 || !json_is_object(payload)) {
                continue;
            }
            key = json_string_value(json_object_get(payload, "key"));
            record_json = json_object_get(payload, "record");
            if (key == NULL || record_json == NULL) {
                continue;
            }
            if (kvs_ensure_private_capacity(node, addr, err, err_len) != 0) {
                json_decref(root);
                return -1;
            }
            record = &node->private_records[addr];
            record->key = kvs_strdup(key);
            if (record->key == NULL) {
                kvs_errorf(err, err_len, "out of memory");
                json_decref(root);
                return -1;
            }
            if (kvs_cipher_record_from_json(record_json, &record->record, err, err_len) != 0) {
                json_decref(root);
                return -1;
            }
            record->present = true;
            node->private_count += 1u;
            if (kvs_private_index_set_locked(node, key, addr, err, err_len) != 0) {
                json_decref(root);
                return -1;
            }
        }
    }
    json_decref(root);
    return 0;
}

static json_t *kvs_ok_response(json_t *result) {
    json_t *response = json_object();
    if (response == NULL) {
        kvs_json_decref_safe(result);
        return NULL;
    }
    if (json_object_set_new(response, "ok", json_true()) != 0 ||
        json_object_set_new(response, "result", result) != 0) {
        kvs_json_decref_safe(result);
        kvs_json_decref_safe(response);
        return NULL;
    }
    return response;
}

static json_t *kvs_error_response(const char *message) {
    json_t *response = json_object();
    if (response == NULL) {
        return NULL;
    }
    if (json_object_set_new(response, "ok", json_false()) != 0 ||
        json_object_set_new(response, "error", json_string(message)) != 0) {
        json_decref(response);
        return NULL;
    }
    return response;
}

static json_t *kvs_mn_rpc_rdma_alloc_slot(kvs_mn_node *node, char *err, size_t err_len) {
    json_t *result = NULL;
    uint32_t slot_id;
    uint32_t epoch;

    pthread_mutex_lock(&node->lock);
    if (kvs_allocate_slot_locked(node, &slot_id, &epoch, err, err_len) == 0) {
        result = json_pack("{sI,sI}", "slot_id", (json_int_t) slot_id, "epoch", (json_int_t) epoch);
    }
    pthread_mutex_unlock(&node->lock);
    return result;
}

static json_t *kvs_mn_rpc_rdma_write_slot(kvs_mn_node *node, const json_t *params, char *err, size_t err_len) {
    json_int_t slot_id;
    json_int_t epoch;
    const json_t *record_json;
    kvs_cipher_record record;
    kvs_slot_store *slot;
    json_t *result;

    slot_id = json_integer_value(json_object_get(params, "slot_id"));
    epoch = json_integer_value(json_object_get(params, "epoch"));
    record_json = json_object_get(params, "record");
    if (!json_is_integer(json_object_get(params, "slot_id")) ||
        !json_is_integer(json_object_get(params, "epoch")) ||
        !json_is_object(record_json)) {
        kvs_errorf(err, err_len, "rdma_write_slot requires slot_id, epoch, record");
        return NULL;
    }
    kvs_cipher_record_init(&record);
    if (kvs_cipher_record_from_json(record_json, &record, err, err_len) != 0) {
        return NULL;
    }

    pthread_mutex_lock(&node->lock);
    if ((size_t) slot_id >= node->slot_capacity || !node->slots[slot_id].occupied) {
        pthread_mutex_unlock(&node->lock);
        kvs_cipher_record_cleanup(&record);
        return json_pack("{s:b,s:s}", "write_ok", 0, "reason", "slot_not_found");
    }
    slot = &node->slots[slot_id];
    if (slot->epoch != (uint32_t) epoch) {
        pthread_mutex_unlock(&node->lock);
        kvs_cipher_record_cleanup(&record);
        return json_pack(
            "{s:b,s:s,sI}",
            "write_ok",
            0,
            "reason",
            "stale_epoch",
            "actual_epoch",
            (json_int_t) slot->epoch
        );
    }
    kvs_cipher_record_cleanup(&slot->record);
    slot->record = record;
    pthread_mutex_unlock(&node->lock);
    result = json_pack("{s:b}", "write_ok", 1);
    return result;
}

static json_t *kvs_mn_rpc_rdma_read_slot(kvs_mn_node *node, const json_t *params, char *err, size_t err_len) {
    json_int_t slot_id;
    json_t *slot_json;
    kvs_cache_slot slot;
    slot_id = json_integer_value(json_object_get(params, "slot_id"));
    if (!json_is_integer(json_object_get(params, "slot_id"))) {
        kvs_errorf(err, err_len, "rdma_read_slot requires slot_id");
        return NULL;
    }
    pthread_mutex_lock(&node->lock);
    if ((size_t) slot_id >= node->slot_capacity || !node->slots[slot_id].occupied) {
        pthread_mutex_unlock(&node->lock);
        return json_pack("{s:b}", "found", 0);
    }
    memset(&slot, 0, sizeof(slot));
    slot.slot_id = (uint32_t) slot_id;
    slot.epoch = node->slots[slot_id].epoch;
    if (kvs_cipher_record_copy(&slot.record, &node->slots[slot_id].record, err, err_len) != 0) {
        pthread_mutex_unlock(&node->lock);
        return NULL;
    }
    pthread_mutex_unlock(&node->lock);
    slot_json = kvs_cache_slot_to_json(&slot);
    kvs_cache_slot_cleanup(&slot);
    if (slot_json == NULL) {
        kvs_errorf(err, err_len, "out of memory");
        return NULL;
    }
    return json_pack("{s:b,s:o}", "found", 1, "slot", slot_json);
}

static json_t *kvs_mn_rpc_rdma_read_prime(kvs_mn_node *node, const json_t *params, char *err, size_t err_len) {
    const char *key = json_string_value(json_object_get(params, "key"));
    kvs_prime_entry *entry;
    kvs_prime_entry_wire wire;
    json_t *entry_json;
    if (key == NULL) {
        kvs_errorf(err, err_len, "rdma_read_prime requires key");
        return NULL;
    }
    pthread_mutex_lock(&node->lock);
    entry = kvs_prime_find_locked(node, key);
    if (entry == NULL) {
        pthread_mutex_unlock(&node->lock);
        return json_pack("{s:b}", "found", 0);
    }
    kvs_prime_touch_locked(node, entry);
    memset(&wire, 0, sizeof(wire));
    wire.key = kvs_strdup(entry->key);
    wire.addr = entry->addr;
    wire.epoch = entry->epoch;
    wire.valid = entry->valid;
    wire.has_private_addr = entry->has_private_addr;
    wire.private_addr = entry->private_addr;
    pthread_mutex_unlock(&node->lock);
    if (wire.key == NULL) {
        kvs_errorf(err, err_len, "out of memory");
        return NULL;
    }
    entry_json = kvs_prime_entry_wire_to_json(&wire);
    kvs_prime_entry_wire_cleanup(&wire);
    if (entry_json == NULL) {
        kvs_errorf(err, err_len, "out of memory");
        return NULL;
    }
    return json_pack("{s:b,s:o}", "found", 1, "entry", entry_json);
}

static json_t *kvs_mn_rpc_rdma_cas_prime(kvs_mn_node *node, const json_t *params, char *err, size_t err_len) {
    const char *key = json_string_value(json_object_get(params, "key"));
    json_t *expected_addr_json = json_object_get(params, "expected_addr");
    json_t *expected_epoch_json = json_object_get(params, "expected_epoch");
    json_t *new_addr_json = json_object_get(params, "new_addr");
    json_t *new_epoch_json = json_object_get(params, "new_epoch");
    json_t *private_addr_json = json_object_get(params, "private_addr");
    bool remove_on_success = json_is_true(json_object_get(params, "remove_on_success"));
    kvs_prime_entry *current;
    bool expected_match;
    kvs_prime_entry_wire wire;
    json_t *current_json = NULL;
    json_t *response_entry = NULL;

    if (key == NULL) {
        kvs_errorf(err, err_len, "rdma_cas_prime requires key");
        return NULL;
    }
    if ((json_is_integer(expected_addr_json) && !json_is_integer(expected_epoch_json)) ||
        (!json_is_integer(expected_addr_json) && expected_addr_json != NULL && !json_is_null(expected_addr_json))) {
        kvs_errorf(err, err_len, "rdma_cas_prime requires integer expected_epoch when expected_addr is present");
        return NULL;
    }
    if (!remove_on_success && (!json_is_integer(new_addr_json) || !json_is_integer(new_epoch_json))) {
        kvs_errorf(err, err_len, "rdma_cas_prime requires new_addr and new_epoch unless remove_on_success is set");
        return NULL;
    }

    pthread_mutex_lock(&node->lock);
    current = kvs_prime_find_locked(node, key);
    if (json_is_null(expected_addr_json) || expected_addr_json == NULL) {
        expected_match = current == NULL;
    } else {
        expected_match = current != NULL &&
                         current->addr == (uint32_t) json_integer_value(expected_addr_json) &&
                         current->epoch == (uint32_t) json_integer_value(expected_epoch_json);
    }
    if (!expected_match) {
        memset(&wire, 0, sizeof(wire));
        if (current != NULL) {
            wire.key = kvs_strdup(current->key);
            wire.addr = current->addr;
            wire.epoch = current->epoch;
            wire.valid = current->valid;
            wire.has_private_addr = current->has_private_addr;
            wire.private_addr = current->private_addr;
            current_json = kvs_prime_entry_wire_to_json(&wire);
            kvs_prime_entry_wire_cleanup(&wire);
        }
        pthread_mutex_unlock(&node->lock);
        return json_pack("{s:b,s:o}", "cas_ok", 0, "current", current_json != NULL ? current_json : json_null());
    }

    if (remove_on_success) {
        kvs_prime_remove_locked(node, key);
        pthread_mutex_unlock(&node->lock);
        return json_pack("{s:b,s:b,s:o}", "cas_ok", 1, "removed", 1, "entry", json_null());
    }

    if ((size_t) json_integer_value(new_addr_json) >= node->slot_capacity ||
        !node->slots[json_integer_value(new_addr_json)].occupied ||
        node->slots[json_integer_value(new_addr_json)].epoch != (uint32_t) json_integer_value(new_epoch_json)) {
        pthread_mutex_unlock(&node->lock);
        return json_pack("{s:b,s:s}", "cas_ok", 0, "reason", "new_pointer_not_available");
    }
    if (current == NULL) {
        current = calloc(1, sizeof(*current));
        if (current == NULL) {
            pthread_mutex_unlock(&node->lock);
            kvs_errorf(err, err_len, "out of memory");
            return NULL;
        }
        current->key = kvs_strdup(key);
        if (current->key == NULL || kvs_prime_insert_locked(node, current, err, err_len) != 0) {
            pthread_mutex_unlock(&node->lock);
            kvs_prime_entry_free(current);
            return NULL;
        }
    }
    if (private_addr_json != NULL && json_is_integer(private_addr_json)) {
        current->private_addr = (int) json_integer_value(private_addr_json);
        current->has_private_addr = true;
    }
    current->addr = (uint32_t) json_integer_value(new_addr_json);
    current->epoch = (uint32_t) json_integer_value(new_epoch_json);
    current->valid = true;
    kvs_prime_touch_locked(node, current);
    pthread_mutex_unlock(&node->lock);
    response_entry = json_object();
    if (response_entry == NULL ||
        json_object_set_new(response_entry, "key", json_string(key)) != 0 ||
        json_object_set_new(response_entry, "addr", json_integer(json_integer_value(new_addr_json))) != 0 ||
        json_object_set_new(response_entry, "epoch", json_integer(json_integer_value(new_epoch_json))) != 0 ||
        json_object_set_new(
            response_entry,
            "private_addr",
            (private_addr_json != NULL && json_is_integer(private_addr_json)) ? json_integer(json_integer_value(private_addr_json)) : json_null()
        ) != 0 ||
        json_object_set_new(response_entry, "valid", json_true()) != 0) {
        kvs_json_decref_safe(response_entry);
        kvs_errorf(err, err_len, "out of memory");
        return NULL;
    }
    return json_pack("{s:b,s:o}", "cas_ok", 1, "entry", response_entry);
}

static json_t *kvs_mn_rpc_cpu_fetch_private(kvs_mn_node *node, const json_t *params, char *err, size_t err_len) {
    const char *key = json_string_value(json_object_get(params, "key"));
    kvs_private_index_entry *index_entry;
    kvs_cipher_record record_copy;
    int private_addr;
    json_t *record_json;
    if (key == NULL) {
        kvs_errorf(err, err_len, "cpu_fetch_private requires key");
        return NULL;
    }
    kvs_cipher_record_init(&record_copy);
    pthread_mutex_lock(&node->lock);
    index_entry = kvs_private_index_find_locked(node, key);
    if (index_entry == NULL || (size_t) index_entry->addr >= node->private_capacity ||
        !node->private_records[index_entry->addr].present) {
        pthread_mutex_unlock(&node->lock);
        return json_pack("{s:b}", "found", 0);
    }
    private_addr = index_entry->addr;
    if (kvs_cipher_record_copy(&record_copy, &node->private_records[index_entry->addr].record, err, err_len) != 0) {
        pthread_mutex_unlock(&node->lock);
        return NULL;
    }
    record_json = kvs_cipher_record_to_json(&record_copy);
    pthread_mutex_unlock(&node->lock);
    kvs_cipher_record_cleanup(&record_copy);
    if (record_json == NULL) {
        kvs_errorf(err, err_len, "out of memory");
        return NULL;
    }
    return json_pack(
        "{s:b,sI,s:o}",
        "found",
        1,
        "private_addr",
        (json_int_t) private_addr,
        "record",
        record_json
    );
}

static json_t *kvs_mn_rpc_cpu_delete_private(kvs_mn_node *node, const json_t *params, char *err, size_t err_len) {
    const char *key = json_string_value(json_object_get(params, "key"));
    if (key == NULL) {
        kvs_errorf(err, err_len, "cpu_delete_private requires key");
        return NULL;
    }
    pthread_mutex_lock(&node->lock);
    if (kvs_delete_private_locked(node, key, err, err_len) != 0) {
        pthread_mutex_unlock(&node->lock);
        return NULL;
    }
    pthread_mutex_unlock(&node->lock);
    return json_pack("{s:b}", "deleted", 1);
}

static json_t *kvs_mn_rpc_debug_state(kvs_mn_node *node) {
    json_t *prime_keys = json_array();
    json_t *result = NULL;
    size_t bucket;

    if (prime_keys == NULL) {
        return NULL;
    }
    pthread_mutex_lock(&node->lock);
    for (bucket = 0u; bucket < node->prime_bucket_count; ++bucket) {
        kvs_prime_entry *entry = node->prime_buckets[bucket];
        while (entry != NULL) {
            if (json_array_append_new(prime_keys, json_string(entry->key)) != 0) {
                pthread_mutex_unlock(&node->lock);
                json_decref(prime_keys);
                return NULL;
            }
            entry = entry->bucket_next;
        }
    }
    result = json_object();
    if (result != NULL) {
        if (json_object_set_new(result, "node_id", json_string(node->config.node_id)) != 0 ||
            json_object_set_new(result, "cache_slots", json_integer((json_int_t) node->cache_slot_count)) != 0 ||
            json_object_set_new(result, "prime_entries", json_integer((json_int_t) node->prime_count)) != 0 ||
            json_object_set_new(result, "private_entries", json_integer((json_int_t) node->private_count)) != 0 ||
            json_object_set_new(result, "prime_keys", prime_keys) != 0) {
            json_decref(result);
            result = NULL;
        } else {
            prime_keys = NULL;
        }
    }
    pthread_mutex_unlock(&node->lock);
    kvs_json_decref_safe(prime_keys);
    return result;
}

static bool kvs_is_rdma_action(const char *action) {
    size_t i;
    for (i = 0u; i < sizeof(KVS_RDMA_ACTIONS) / sizeof(KVS_RDMA_ACTIONS[0]); ++i) {
        if (strcmp(action, KVS_RDMA_ACTIONS[i]) == 0) {
            return true;
        }
    }
    return false;
}

static json_t *kvs_mn_dispatch_rpc(kvs_mn_node *node, const json_t *request, bool rdma_only) {
    const char *action;
    json_t *params;
    json_t *result = NULL;
    char err[256] = {0};

    if (!json_is_object(request)) {
        return kvs_error_response("request must be an object");
    }
    action = json_string_value(json_object_get(request, "action"));
    params = json_object_get(request, "params");
    if (action == NULL) {
        return kvs_error_response("missing action");
    }
    if (params == NULL) {
        params = json_object();
    }
    if (!json_is_object(params)) {
        return kvs_error_response("params must be object");
    }
    if (rdma_only && !kvs_is_rdma_action(action)) {
        return kvs_error_response("unsupported rdma action");
    }

    if (strcmp(action, "rdma_alloc_slot") == 0) {
        result = kvs_mn_rpc_rdma_alloc_slot(node, err, sizeof(err));
    } else if (strcmp(action, "rdma_write_slot") == 0) {
        result = kvs_mn_rpc_rdma_write_slot(node, params, err, sizeof(err));
    } else if (strcmp(action, "rdma_read_slot") == 0) {
        result = kvs_mn_rpc_rdma_read_slot(node, params, err, sizeof(err));
    } else if (strcmp(action, "rdma_read_prime") == 0) {
        result = kvs_mn_rpc_rdma_read_prime(node, params, err, sizeof(err));
    } else if (strcmp(action, "rdma_cas_prime") == 0) {
        result = kvs_mn_rpc_rdma_cas_prime(node, params, err, sizeof(err));
    } else if (strcmp(action, "cpu_fetch_private") == 0) {
        result = kvs_mn_rpc_cpu_fetch_private(node, params, err, sizeof(err));
    } else if (strcmp(action, "cpu_delete_private") == 0) {
        result = kvs_mn_rpc_cpu_delete_private(node, params, err, sizeof(err));
    } else if (strcmp(action, "debug_state") == 0) {
        result = kvs_mn_rpc_debug_state(node);
    } else {
        kvs_errorf(err, sizeof(err), "unknown action: %s", action);
    }

    if (result == NULL) {
        return kvs_error_response(err[0] != '\0' ? err : "internal error");
    }
    return kvs_ok_response(result);
}

json_t *kvs_mn_node_handle_rpc_json(void *ctx, const json_t *request) {
    return kvs_mn_dispatch_rpc((kvs_mn_node *) ctx, request, false);
}

static json_t *kvs_mn_node_handle_rdma_json(void *ctx, const json_t *request) {
    return kvs_mn_dispatch_rpc((kvs_mn_node *) ctx, request, true);
}

kvs_mn_node *kvs_mn_node_create(const kvs_mn_config *config, char *err, size_t err_len) {
    kvs_mn_node *node = calloc(1, sizeof(*node));
    if (node == NULL) {
        kvs_errorf(err, err_len, "out of memory");
        return NULL;
    }
    node->prime_bucket_count = 1024u;
    node->private_bucket_count = 1024u;
    node->next_private_addr = 1;
    node->prime_buckets = calloc(node->prime_bucket_count, sizeof(*node->prime_buckets));
    node->private_buckets = calloc(node->private_bucket_count, sizeof(*node->private_buckets));
    if (node->prime_buckets == NULL || node->private_buckets == NULL) {
        kvs_errorf(err, err_len, "out of memory");
        kvs_mn_node_destroy(node);
        return NULL;
    }
    if (pthread_mutex_init(&node->lock, NULL) != 0) {
        kvs_errorf(err, err_len, "pthread_mutex_init failed");
        kvs_mn_node_destroy(node);
        return NULL;
    }
    if (kvs_copy_mn_config(config, &node->config, err, err_len) != 0 ||
        kvs_load_private_state(node, err, err_len) != 0) {
        kvs_mn_node_destroy(node);
        return NULL;
    }
    return node;
}

void kvs_mn_node_stop(kvs_mn_node *node) {
    if (node == NULL) {
        return;
    }
    if (node->rdma_started) {
        kvs_rdma_server_stop(&node->rdma_server);
        node->rdma_started = false;
    }
    kvs_tcp_server_stop(&node->tcp_server);
}

void kvs_mn_node_destroy(kvs_mn_node *node) {
    size_t i;
    if (node == NULL) {
        return;
    }
    kvs_mn_node_stop(node);
    for (i = 0u; i < node->slot_capacity; ++i) {
        kvs_cipher_record_cleanup(&node->slots[i].record);
    }
    for (i = 0u; i < node->private_capacity; ++i) {
        kvs_private_record_cleanup(&node->private_records[i]);
    }
    for (i = 0u; i < node->prime_bucket_count; ++i) {
        kvs_prime_entry *entry = node->prime_buckets[i];
        while (entry != NULL) {
            kvs_prime_entry *next = entry->bucket_next;
            kvs_prime_entry_free(entry);
            entry = next;
        }
    }
    for (i = 0u; i < node->private_bucket_count; ++i) {
        kvs_private_index_entry *entry = node->private_buckets[i];
        while (entry != NULL) {
            kvs_private_index_entry *next = entry->next;
            free(entry->key);
            free(entry);
            entry = next;
        }
    }
    free(node->slots);
    free(node->free_slots);
    free(node->prime_buckets);
    free(node->private_buckets);
    free(node->private_records);
    kvs_free_mn_config_copy(&node->config);
    pthread_mutex_destroy(&node->lock);
    free(node);
}

int kvs_mn_node_start(kvs_mn_node *node, char *err, size_t err_len) {
    const char *rdma_host;
    uint16_t rdma_port;
    if (kvs_tcp_server_start(
            &node->tcp_server,
            node->config.listen_host,
            node->config.listen_port,
            kvs_mn_node_handle_rpc_json,
            node,
            err,
            err_len
        ) != 0) {
        return -1;
    }
    if (!node->config.enable_rdma_server) {
        return 0;
    }
    if (!kvs_rdma_supported()) {
        if (node->config.require_rdma_server) {
            kvs_errorf(err, err_len, "RDMA server requested but rdma transport is unavailable on this host");
            kvs_tcp_server_stop(&node->tcp_server);
            return -1;
        }
        return 0;
    }
    rdma_host = node->config.rdma_listen_host != NULL ? node->config.rdma_listen_host : node->config.listen_host;
    rdma_port = node->config.has_rdma_listen_port ? node->config.rdma_listen_port : (uint16_t) (node->config.listen_port + 100u);
    if (kvs_rdma_server_start(&node->rdma_server, rdma_host, rdma_port, kvs_mn_node_handle_rdma_json, node, err, err_len) != 0) {
        kvs_tcp_server_stop(&node->tcp_server);
        return -1;
    }
    node->rdma_started = true;
    return 0;
}

uint16_t kvs_mn_node_tcp_port(const kvs_mn_node *node) {
    return node->tcp_server.bound_port;
}

uint16_t kvs_mn_node_rdma_port(const kvs_mn_node *node) {
    return node->rdma_server.bound_port;
}

const kvs_mn_config *kvs_mn_node_config(const kvs_mn_node *node) {
    return &node->config;
}
