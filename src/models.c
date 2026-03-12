#include "kvs/models.h"

#include "kvs/common.h"

#include <stdlib.h>
#include <string.h>

void kvs_cipher_record_init(kvs_cipher_record *record) {
    if (record == NULL) {
        return;
    }
    memset(record, 0, sizeof(*record));
}

void kvs_cipher_record_cleanup(kvs_cipher_record *record) {
    if (record == NULL) {
        return;
    }
    free(record->ciphertext_b64);
    free(record->nonce_b64);
    free(record->tag_b64);
    free(record->algorithm);
    kvs_cipher_record_init(record);
}

int kvs_cipher_record_copy(
    kvs_cipher_record *dst,
    const kvs_cipher_record *src,
    char *err,
    size_t err_len
) {
    kvs_cipher_record_init(dst);
    dst->ciphertext_b64 = kvs_strdup(src->ciphertext_b64);
    dst->nonce_b64 = kvs_strdup(src->nonce_b64);
    dst->tag_b64 = kvs_strdup(src->tag_b64);
    dst->algorithm = kvs_strdup(src->algorithm);
    dst->tombstone = src->tombstone;
    if (dst->ciphertext_b64 == NULL || dst->nonce_b64 == NULL || dst->tag_b64 == NULL || dst->algorithm == NULL) {
        kvs_cipher_record_cleanup(dst);
        kvs_errorf(err, err_len, "out of memory");
        return -1;
    }
    return 0;
}

json_t *kvs_cipher_record_to_json(const kvs_cipher_record *record) {
    json_t *obj = json_object();
    if (obj == NULL) {
        return NULL;
    }
    if (json_object_set_new(obj, "ciphertext_b64", json_string(record->ciphertext_b64 ? record->ciphertext_b64 : "")) != 0 ||
        json_object_set_new(obj, "nonce_b64", json_string(record->nonce_b64 ? record->nonce_b64 : "")) != 0 ||
        json_object_set_new(obj, "tag_b64", json_string(record->tag_b64 ? record->tag_b64 : "")) != 0 ||
        json_object_set_new(obj, "algorithm", json_string(record->algorithm ? record->algorithm : "")) != 0 ||
        json_object_set_new(obj, "tombstone", json_boolean(record->tombstone)) != 0) {
        json_decref(obj);
        return NULL;
    }
    return obj;
}

static const char *kvs_json_string_required(const json_t *obj, const char *key, char *err, size_t err_len) {
    json_t *value = json_object_get(obj, key);
    if (!json_is_string(value)) {
        kvs_errorf(err, err_len, "missing string field '%s'", key);
        return NULL;
    }
    return json_string_value(value);
}

int kvs_cipher_record_from_json(
    const json_t *json,
    kvs_cipher_record *out,
    char *err,
    size_t err_len
) {
    const char *ciphertext;
    const char *nonce;
    const char *tag;
    const char *algorithm;
    json_t *tombstone;

    if (!json_is_object(json)) {
        kvs_errorf(err, err_len, "cipher record must be an object");
        return -1;
    }

    ciphertext = kvs_json_string_required(json, "ciphertext_b64", err, err_len);
    if (ciphertext == NULL) {
        return -1;
    }
    nonce = kvs_json_string_required(json, "nonce_b64", err, err_len);
    if (nonce == NULL) {
        return -1;
    }
    tag = kvs_json_string_required(json, "tag_b64", err, err_len);
    if (tag == NULL) {
        return -1;
    }
    algorithm = kvs_json_string_required(json, "algorithm", err, err_len);
    if (algorithm == NULL) {
        return -1;
    }

    kvs_cipher_record_init(out);
    out->ciphertext_b64 = kvs_strdup(ciphertext);
    out->nonce_b64 = kvs_strdup(nonce);
    out->tag_b64 = kvs_strdup(tag);
    out->algorithm = kvs_strdup(algorithm);
    if (out->ciphertext_b64 == NULL || out->nonce_b64 == NULL || out->tag_b64 == NULL || out->algorithm == NULL) {
        kvs_cipher_record_cleanup(out);
        kvs_errorf(err, err_len, "out of memory");
        return -1;
    }

    tombstone = json_object_get(json, "tombstone");
    out->tombstone = json_is_true(tombstone);
    return 0;
}

void kvs_cache_slot_cleanup(kvs_cache_slot *slot) {
    if (slot == NULL) {
        return;
    }
    kvs_cipher_record_cleanup(&slot->record);
    memset(slot, 0, sizeof(*slot));
}

json_t *kvs_cache_slot_to_json(const kvs_cache_slot *slot) {
    json_t *record = kvs_cipher_record_to_json(&slot->record);
    json_t *obj;
    if (record == NULL) {
        return NULL;
    }
    obj = json_object();
    if (obj == NULL) {
        json_decref(record);
        return NULL;
    }
    if (json_object_set_new(obj, "slot_id", json_integer((json_int_t) slot->slot_id)) != 0 ||
        json_object_set_new(obj, "epoch", json_integer((json_int_t) slot->epoch)) != 0 ||
        json_object_set_new(obj, "record", record) != 0) {
        json_decref(record);
        json_decref(obj);
        return NULL;
    }
    return obj;
}

int kvs_cache_slot_from_json(
    const json_t *json,
    kvs_cache_slot *out,
    char *err,
    size_t err_len
) {
    json_t *slot_id;
    json_t *epoch;
    json_t *record;

    memset(out, 0, sizeof(*out));
    if (!json_is_object(json)) {
        kvs_errorf(err, err_len, "cache slot must be an object");
        return -1;
    }

    slot_id = json_object_get(json, "slot_id");
    epoch = json_object_get(json, "epoch");
    record = json_object_get(json, "record");
    if (!json_is_integer(slot_id) || !json_is_integer(epoch)) {
        kvs_errorf(err, err_len, "cache slot is missing integer pointer fields");
        return -1;
    }

    out->slot_id = (uint32_t) json_integer_value(slot_id);
    out->epoch = (uint32_t) json_integer_value(epoch);
    if (kvs_cipher_record_from_json(record, &out->record, err, err_len) != 0) {
        return -1;
    }
    return 0;
}

void kvs_prime_entry_wire_cleanup(kvs_prime_entry_wire *entry) {
    if (entry == NULL) {
        return;
    }
    free(entry->key);
    memset(entry, 0, sizeof(*entry));
}

json_t *kvs_prime_entry_wire_to_json(const kvs_prime_entry_wire *entry) {
    json_t *obj = json_object();
    if (obj == NULL) {
        return NULL;
    }
    if (json_object_set_new(obj, "key", json_string(entry->key ? entry->key : "")) != 0 ||
        json_object_set_new(obj, "addr", json_integer((json_int_t) entry->addr)) != 0 ||
        json_object_set_new(obj, "epoch", json_integer((json_int_t) entry->epoch)) != 0 ||
        json_object_set_new(obj, "valid", json_boolean(entry->valid)) != 0) {
        json_decref(obj);
        return NULL;
    }
    if (entry->has_private_addr) {
        if (json_object_set_new(obj, "private_addr", json_integer((json_int_t) entry->private_addr)) != 0) {
            json_decref(obj);
            return NULL;
        }
    } else if (json_object_set_new(obj, "private_addr", json_null()) != 0) {
        json_decref(obj);
        return NULL;
    }
    return obj;
}

int kvs_prime_entry_wire_from_json(
    const json_t *json,
    kvs_prime_entry_wire *out,
    char *err,
    size_t err_len
) {
    json_t *key;
    json_t *addr;
    json_t *epoch;
    json_t *private_addr;
    json_t *valid;

    memset(out, 0, sizeof(*out));
    if (!json_is_object(json)) {
        kvs_errorf(err, err_len, "prime entry must be an object");
        return -1;
    }

    key = json_object_get(json, "key");
    addr = json_object_get(json, "addr");
    epoch = json_object_get(json, "epoch");
    private_addr = json_object_get(json, "private_addr");
    valid = json_object_get(json, "valid");
    if (!json_is_string(key) || !json_is_integer(addr) || !json_is_integer(epoch)) {
        kvs_errorf(err, err_len, "prime entry has invalid pointer fields");
        return -1;
    }

    out->key = kvs_strdup(json_string_value(key));
    if (out->key == NULL) {
        kvs_errorf(err, err_len, "out of memory");
        return -1;
    }
    out->addr = (uint32_t) json_integer_value(addr);
    out->epoch = (uint32_t) json_integer_value(epoch);
    out->valid = valid == NULL ? true : json_is_true(valid);
    if (json_is_integer(private_addr)) {
        out->has_private_addr = true;
        out->private_addr = (int) json_integer_value(private_addr);
    }
    return 0;
}
