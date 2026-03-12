#ifndef KVS_CRYPTO_H
#define KVS_CRYPTO_H

#include <stdbool.h>
#include <stddef.h>

#include "kvs/models.h"

typedef struct {
    unsigned char key[32];
    size_t key_len;
    bool aes_gcm_available;
} kvs_aead_cipher;

int kvs_aead_cipher_init(
    kvs_aead_cipher *cipher,
    const char *key_hex,
    char *err,
    size_t err_len
);
void kvs_aead_cipher_cleanup(kvs_aead_cipher *cipher);
const char *kvs_aead_preferred_algorithm(const kvs_aead_cipher *cipher);
int kvs_aead_encrypt(
    const kvs_aead_cipher *cipher,
    const unsigned char *plaintext,
    size_t plaintext_len,
    const unsigned char *aad,
    size_t aad_len,
    kvs_cipher_record *out,
    char *err,
    size_t err_len
);
int kvs_aead_decrypt(
    const kvs_aead_cipher *cipher,
    const kvs_cipher_record *record,
    const unsigned char *aad,
    size_t aad_len,
    unsigned char **plaintext_out,
    size_t *plaintext_len_out,
    char *err,
    size_t err_len
);

#endif
