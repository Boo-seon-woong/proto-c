#ifndef KVS_MODELS_H
#define KVS_MODELS_H

#include <jansson.h>
#include <stdbool.h>
#include <stdint.h>

typedef struct {
    char *ciphertext_b64;
    char *nonce_b64;
    char *tag_b64;
    char *algorithm;
    bool tombstone;
} kvs_cipher_record;

typedef struct {
    uint32_t slot_id;
    uint32_t epoch;
    kvs_cipher_record record;
} kvs_cache_slot;

typedef struct {
    char *key;
    uint32_t addr;
    uint32_t epoch;
    int private_addr;
    bool has_private_addr;
    bool valid;
} kvs_prime_entry_wire;

void kvs_cipher_record_init(kvs_cipher_record *record);
void kvs_cipher_record_cleanup(kvs_cipher_record *record);
int kvs_cipher_record_copy(
    kvs_cipher_record *dst,
    const kvs_cipher_record *src,
    char *err,
    size_t err_len
);
json_t *kvs_cipher_record_to_json(const kvs_cipher_record *record);
int kvs_cipher_record_from_json(
    const json_t *json,
    kvs_cipher_record *out,
    char *err,
    size_t err_len
);

void kvs_cache_slot_cleanup(kvs_cache_slot *slot);
json_t *kvs_cache_slot_to_json(const kvs_cache_slot *slot);
int kvs_cache_slot_from_json(
    const json_t *json,
    kvs_cache_slot *out,
    char *err,
    size_t err_len
);

void kvs_prime_entry_wire_cleanup(kvs_prime_entry_wire *entry);
json_t *kvs_prime_entry_wire_to_json(const kvs_prime_entry_wire *entry);
int kvs_prime_entry_wire_from_json(
    const json_t *json,
    kvs_prime_entry_wire *out,
    char *err,
    size_t err_len
);

#endif
