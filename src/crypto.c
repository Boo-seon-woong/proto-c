#include "kvs/crypto.h"

#include "kvs/common.h"

#include <openssl/core_names.h>
#include <openssl/evp.h>
#include <openssl/params.h>
#include <openssl/rand.h>
#include <openssl/sha.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int kvs_decode_hex(const char *hex, unsigned char *out, size_t *out_len, char *err, size_t err_len) {
    size_t len = strlen(hex);
    size_t i;

    if ((len % 2u) != 0u) {
        kvs_errorf(err, err_len, "encryption_key_hex must have even length");
        return -1;
    }
    *out_len = len / 2u;
    if (*out_len != 16u && *out_len != 24u && *out_len != 32u) {
        kvs_errorf(err, err_len, "encryption_key_hex must decode to 16/24/32 bytes");
        return -1;
    }
    for (i = 0; i < *out_len; ++i) {
        unsigned int byte;
        if (sscanf(hex + (i * 2u), "%2x", &byte) != 1) {
            kvs_errorf(err, err_len, "invalid hex key");
            return -1;
        }
        out[i] = (unsigned char) byte;
    }
    return 0;
}

static const EVP_CIPHER *kvs_aes_gcm_cipher(size_t key_len) {
    switch (key_len) {
        case 16u:
            return EVP_aes_128_gcm();
        case 24u:
            return EVP_aes_192_gcm();
        case 32u:
            return EVP_aes_256_gcm();
        default:
            return NULL;
    }
}

static int kvs_base64_encode(const unsigned char *data, size_t data_len, char **out, char *err, size_t err_len) {
    int encoded_len;
    char *buffer;

    encoded_len = 4 * (int) ((data_len + 2u) / 3u);
    buffer = malloc((size_t) encoded_len + 1u);
    if (buffer == NULL) {
        kvs_errorf(err, err_len, "out of memory");
        return -1;
    }
    if (EVP_EncodeBlock((unsigned char *) buffer, data, (int) data_len) < 0) {
        free(buffer);
        kvs_errorf(err, err_len, "base64 encode failed");
        return -1;
    }
    buffer[encoded_len] = '\0';
    *out = buffer;
    return 0;
}

static int kvs_base64_decode(const char *text, unsigned char **out, size_t *out_len, char *err, size_t err_len) {
    size_t len = strlen(text);
    size_t pad = 0;
    unsigned char *buffer;
    int decoded_len;

    if ((len % 4u) != 0u) {
        kvs_errorf(err, err_len, "invalid base64 length");
        return -1;
    }
    if (len > 0u && text[len - 1u] == '=') {
        pad += 1u;
    }
    if (len > 1u && text[len - 2u] == '=') {
        pad += 1u;
    }

    buffer = malloc((len / 4u) * 3u + 1u);
    if (buffer == NULL) {
        kvs_errorf(err, err_len, "out of memory");
        return -1;
    }
    decoded_len = EVP_DecodeBlock(buffer, (const unsigned char *) text, (int) len);
    if (decoded_len < 0) {
        free(buffer);
        kvs_errorf(err, err_len, "base64 decode failed");
        return -1;
    }
    decoded_len -= (int) pad;
    *out = buffer;
    *out_len = (size_t) decoded_len;
    return 0;
}

int kvs_aead_cipher_init(
    kvs_aead_cipher *cipher,
    const char *key_hex,
    char *err,
    size_t err_len
) {
    memset(cipher, 0, sizeof(*cipher));
    if (kvs_decode_hex(key_hex, cipher->key, &cipher->key_len, err, err_len) != 0) {
        return -1;
    }
    cipher->aes_gcm_available = kvs_aes_gcm_cipher(cipher->key_len) != NULL;
    return 0;
}

void kvs_aead_cipher_cleanup(kvs_aead_cipher *cipher) {
    if (cipher == NULL) {
        return;
    }
    OPENSSL_cleanse(cipher->key, sizeof(cipher->key));
    memset(cipher, 0, sizeof(*cipher));
}

const char *kvs_aead_preferred_algorithm(const kvs_aead_cipher *cipher) {
    if (cipher->aes_gcm_available) {
        return "aes-gcm";
    }
    return "hmac-stream-v1";
}

static int kvs_encrypt_aes_gcm(
    const kvs_aead_cipher *cipher,
    const unsigned char *plaintext,
    size_t plaintext_len,
    const unsigned char *aad,
    size_t aad_len,
    kvs_cipher_record *out,
    char *err,
    size_t err_len
) {
    EVP_CIPHER_CTX *ctx = NULL;
    const EVP_CIPHER *evp_cipher = kvs_aes_gcm_cipher(cipher->key_len);
    unsigned char nonce[12];
    unsigned char tag[16];
    unsigned char *ciphertext = NULL;
    int len = 0;
    int total = 0;
    int final_len = 0;
    int rc = -1;

    if (RAND_bytes(nonce, (int) sizeof(nonce)) != 1) {
        kvs_errorf(err, err_len, "RAND_bytes failed");
        return -1;
    }

    ctx = EVP_CIPHER_CTX_new();
    ciphertext = malloc(plaintext_len == 0u ? 1u : plaintext_len);
    if (ctx == NULL || ciphertext == NULL) {
        kvs_errorf(err, err_len, "out of memory");
        goto done;
    }
    if (EVP_EncryptInit_ex(ctx, evp_cipher, NULL, NULL, NULL) != 1 ||
        EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_IVLEN, (int) sizeof(nonce), NULL) != 1 ||
        EVP_EncryptInit_ex(ctx, NULL, NULL, cipher->key, nonce) != 1) {
        kvs_errorf(err, err_len, "EVP_EncryptInit_ex failed");
        goto done;
    }
    if (aad_len > 0u && EVP_EncryptUpdate(ctx, NULL, &len, aad, (int) aad_len) != 1) {
        kvs_errorf(err, err_len, "EVP_EncryptUpdate(aad) failed");
        goto done;
    }
    if (EVP_EncryptUpdate(ctx, ciphertext, &len, plaintext, (int) plaintext_len) != 1) {
        kvs_errorf(err, err_len, "EVP_EncryptUpdate failed");
        goto done;
    }
    total = len;
    if (EVP_EncryptFinal_ex(ctx, ciphertext + total, &final_len) != 1) {
        kvs_errorf(err, err_len, "EVP_EncryptFinal_ex failed");
        goto done;
    }
    total += final_len;
    if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_GET_TAG, (int) sizeof(tag), tag) != 1) {
        kvs_errorf(err, err_len, "EVP_CTRL_GCM_GET_TAG failed");
        goto done;
    }

    kvs_cipher_record_init(out);
    if (kvs_base64_encode(ciphertext, (size_t) total, &out->ciphertext_b64, err, err_len) != 0 ||
        kvs_base64_encode(nonce, sizeof(nonce), &out->nonce_b64, err, err_len) != 0 ||
        kvs_base64_encode(tag, sizeof(tag), &out->tag_b64, err, err_len) != 0) {
        kvs_cipher_record_cleanup(out);
        goto done;
    }
    out->algorithm = kvs_strdup("aes-gcm");
    if (out->algorithm == NULL) {
        kvs_cipher_record_cleanup(out);
        kvs_errorf(err, err_len, "out of memory");
        goto done;
    }
    rc = 0;

done:
    if (ctx != NULL) {
        EVP_CIPHER_CTX_free(ctx);
    }
    free(ciphertext);
    return rc;
}

static int kvs_stream_keystream(
    const kvs_aead_cipher *cipher,
    const unsigned char *nonce,
    size_t nonce_len,
    size_t length,
    unsigned char **stream_out,
    char *err,
    size_t err_len
) {
    unsigned char *stream;
    size_t produced = 0;
    uint32_t counter = 0;

    stream = malloc(length == 0u ? 1u : length);
    if (stream == NULL) {
        kvs_errorf(err, err_len, "out of memory");
        return -1;
    }

    while (produced < length) {
        unsigned char block[SHA256_DIGEST_LENGTH];
        unsigned char counter_be[4];
        unsigned char *input = NULL;
        size_t remaining = length - produced;
        size_t copy_len = remaining < sizeof(block) ? remaining : sizeof(block);
        size_t input_len = cipher->key_len + nonce_len + sizeof(counter_be);
        unsigned int digest_len = 0;

        counter_be[0] = (unsigned char) ((counter >> 24) & 0xffu);
        counter_be[1] = (unsigned char) ((counter >> 16) & 0xffu);
        counter_be[2] = (unsigned char) ((counter >> 8) & 0xffu);
        counter_be[3] = (unsigned char) (counter & 0xffu);
        input = malloc(input_len);
        if (input == NULL) {
            free(stream);
            kvs_errorf(err, err_len, "out of memory");
            return -1;
        }
        memcpy(input, cipher->key, cipher->key_len);
        memcpy(input + cipher->key_len, nonce, nonce_len);
        memcpy(input + cipher->key_len + nonce_len, counter_be, sizeof(counter_be));
        if (EVP_Digest(input, input_len, block, &digest_len, EVP_sha256(), NULL) != 1 || digest_len != SHA256_DIGEST_LENGTH) {
            free(input);
            free(stream);
            kvs_errorf(err, err_len, "EVP_Digest failed");
            return -1;
        }
        free(input);
        memcpy(stream + produced, block, copy_len);
        produced += copy_len;
        counter += 1u;
    }

    *stream_out = stream;
    return 0;
}

static int kvs_hmac_sha256(
    const unsigned char *key,
    size_t key_len,
    const unsigned char *part1,
    size_t part1_len,
    const unsigned char *part2,
    size_t part2_len,
    const unsigned char *part3,
    size_t part3_len,
    unsigned char *out,
    size_t out_len,
    char *err,
    size_t err_len
) {
    EVP_MAC *mac = NULL;
    EVP_MAC_CTX *ctx = NULL;
    OSSL_PARAM params[2];
    size_t produced = 0;
    int rc = -1;

    params[0] = OSSL_PARAM_construct_utf8_string(OSSL_MAC_PARAM_DIGEST, "SHA256", 0);
    params[1] = OSSL_PARAM_construct_end();

    mac = EVP_MAC_fetch(NULL, "HMAC", NULL);
    if (mac == NULL) {
        kvs_errorf(err, err_len, "EVP_MAC_fetch failed");
        goto done;
    }
    ctx = EVP_MAC_CTX_new(mac);
    if (ctx == NULL ||
        EVP_MAC_init(ctx, key, key_len, params) != 1 ||
        (part1_len > 0u && EVP_MAC_update(ctx, part1, part1_len) != 1) ||
        (part2_len > 0u && EVP_MAC_update(ctx, part2, part2_len) != 1) ||
        (part3_len > 0u && EVP_MAC_update(ctx, part3, part3_len) != 1) ||
        EVP_MAC_final(ctx, out, &produced, out_len) != 1) {
        kvs_errorf(err, err_len, "EVP_MAC failure");
        goto done;
    }
    rc = 0;

done:
    EVP_MAC_CTX_free(ctx);
    EVP_MAC_free(mac);
    return rc;
}

static int kvs_encrypt_hmac_stream(
    const kvs_aead_cipher *cipher,
    const unsigned char *plaintext,
    size_t plaintext_len,
    const unsigned char *aad,
    size_t aad_len,
    kvs_cipher_record *out,
    char *err,
    size_t err_len
) {
    unsigned char nonce[16];
    unsigned char *stream = NULL;
    unsigned char *ciphertext = NULL;
    unsigned char tag[32];
    size_t i;
    int rc = -1;

    if (RAND_bytes(nonce, (int) sizeof(nonce)) != 1) {
        kvs_errorf(err, err_len, "RAND_bytes failed");
        return -1;
    }
    if (kvs_stream_keystream(cipher, nonce, sizeof(nonce), plaintext_len, &stream, err, err_len) != 0) {
        return -1;
    }
    ciphertext = malloc(plaintext_len == 0u ? 1u : plaintext_len);
    if (ciphertext == NULL) {
        kvs_errorf(err, err_len, "out of memory");
        goto done;
    }
    for (i = 0; i < plaintext_len; ++i) {
        ciphertext[i] = plaintext[i] ^ stream[i];
    }

    if (kvs_hmac_sha256(
            cipher->key,
            cipher->key_len,
            nonce,
            sizeof(nonce),
            aad,
            aad_len,
            ciphertext,
            plaintext_len,
            tag,
            sizeof(tag),
            err,
            err_len
        ) != 0) {
        goto done;
    }

    kvs_cipher_record_init(out);
    if (kvs_base64_encode(ciphertext, plaintext_len, &out->ciphertext_b64, err, err_len) != 0 ||
        kvs_base64_encode(nonce, sizeof(nonce), &out->nonce_b64, err, err_len) != 0 ||
        kvs_base64_encode(tag, 16u, &out->tag_b64, err, err_len) != 0) {
        kvs_cipher_record_cleanup(out);
        goto done;
    }
    out->algorithm = kvs_strdup("hmac-stream-v1");
    if (out->algorithm == NULL) {
        kvs_cipher_record_cleanup(out);
        kvs_errorf(err, err_len, "out of memory");
        goto done;
    }
    rc = 0;

done:
    free(stream);
    free(ciphertext);
    return rc;
}

int kvs_aead_encrypt(
    const kvs_aead_cipher *cipher,
    const unsigned char *plaintext,
    size_t plaintext_len,
    const unsigned char *aad,
    size_t aad_len,
    kvs_cipher_record *out,
    char *err,
    size_t err_len
) {
    if (cipher->aes_gcm_available) {
        return kvs_encrypt_aes_gcm(cipher, plaintext, plaintext_len, aad, aad_len, out, err, err_len);
    }
    return kvs_encrypt_hmac_stream(cipher, plaintext, plaintext_len, aad, aad_len, out, err, err_len);
}

static int kvs_decrypt_aes_gcm(
    const kvs_aead_cipher *cipher,
    const kvs_cipher_record *record,
    const unsigned char *aad,
    size_t aad_len,
    unsigned char **plaintext_out,
    size_t *plaintext_len_out,
    char *err,
    size_t err_len
) {
    EVP_CIPHER_CTX *ctx = NULL;
    const EVP_CIPHER *evp_cipher = kvs_aes_gcm_cipher(cipher->key_len);
    unsigned char *ciphertext = NULL;
    unsigned char *nonce = NULL;
    unsigned char *tag = NULL;
    unsigned char *plaintext = NULL;
    size_t ciphertext_len = 0;
    size_t nonce_len = 0;
    size_t tag_len = 0;
    int len = 0;
    int total = 0;
    int rc = -1;

    if (kvs_base64_decode(record->ciphertext_b64, &ciphertext, &ciphertext_len, err, err_len) != 0 ||
        kvs_base64_decode(record->nonce_b64, &nonce, &nonce_len, err, err_len) != 0 ||
        kvs_base64_decode(record->tag_b64, &tag, &tag_len, err, err_len) != 0) {
        goto done;
    }
    if (tag_len != 16u) {
        kvs_errorf(err, err_len, "invalid aes-gcm tag length");
        goto done;
    }

    ctx = EVP_CIPHER_CTX_new();
    plaintext = malloc(ciphertext_len == 0u ? 1u : ciphertext_len);
    if (ctx == NULL || plaintext == NULL) {
        kvs_errorf(err, err_len, "out of memory");
        goto done;
    }
    if (EVP_DecryptInit_ex(ctx, evp_cipher, NULL, NULL, NULL) != 1 ||
        EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_IVLEN, (int) nonce_len, NULL) != 1 ||
        EVP_DecryptInit_ex(ctx, NULL, NULL, cipher->key, nonce) != 1) {
        kvs_errorf(err, err_len, "EVP_DecryptInit_ex failed");
        goto done;
    }
    if (aad_len > 0u && EVP_DecryptUpdate(ctx, NULL, &len, aad, (int) aad_len) != 1) {
        kvs_errorf(err, err_len, "EVP_DecryptUpdate(aad) failed");
        goto done;
    }
    if (EVP_DecryptUpdate(ctx, plaintext, &len, ciphertext, (int) ciphertext_len) != 1) {
        kvs_errorf(err, err_len, "EVP_DecryptUpdate failed");
        goto done;
    }
    total = len;
    if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_TAG, (int) tag_len, tag) != 1 ||
        EVP_DecryptFinal_ex(ctx, plaintext + total, &len) != 1) {
        kvs_errorf(err, err_len, "ciphertext authentication failed");
        goto done;
    }
    total += len;
    *plaintext_out = plaintext;
    *plaintext_len_out = (size_t) total;
    plaintext = NULL;
    rc = 0;

done:
    if (ctx != NULL) {
        EVP_CIPHER_CTX_free(ctx);
    }
    free(ciphertext);
    free(nonce);
    free(tag);
    free(plaintext);
    return rc;
}

static int kvs_decrypt_hmac_stream(
    const kvs_aead_cipher *cipher,
    const kvs_cipher_record *record,
    const unsigned char *aad,
    size_t aad_len,
    unsigned char **plaintext_out,
    size_t *plaintext_len_out,
    char *err,
    size_t err_len
) {
    unsigned char *ciphertext = NULL;
    unsigned char *nonce = NULL;
    unsigned char *tag = NULL;
    unsigned char *plaintext = NULL;
    unsigned char *stream = NULL;
    unsigned char expected_tag[32];
    size_t ciphertext_len = 0;
    size_t nonce_len = 0;
    size_t tag_len = 0;
    size_t i;
    int rc = -1;

    if (kvs_base64_decode(record->ciphertext_b64, &ciphertext, &ciphertext_len, err, err_len) != 0 ||
        kvs_base64_decode(record->nonce_b64, &nonce, &nonce_len, err, err_len) != 0 ||
        kvs_base64_decode(record->tag_b64, &tag, &tag_len, err, err_len) != 0) {
        goto done;
    }
    if (kvs_hmac_sha256(
            cipher->key,
            cipher->key_len,
            nonce,
            nonce_len,
            aad,
            aad_len,
            ciphertext,
            ciphertext_len,
            expected_tag,
            sizeof(expected_tag),
            err,
            err_len
        ) != 0) {
        goto done;
    }
    if (tag_len != 16u || CRYPTO_memcmp(expected_tag, tag, 16u) != 0) {
        kvs_errorf(err, err_len, "ciphertext authentication failed");
        goto done;
    }
    if (kvs_stream_keystream(cipher, nonce, nonce_len, ciphertext_len, &stream, err, err_len) != 0) {
        goto done;
    }
    plaintext = malloc(ciphertext_len == 0u ? 1u : ciphertext_len);
    if (plaintext == NULL) {
        kvs_errorf(err, err_len, "out of memory");
        goto done;
    }
    for (i = 0; i < ciphertext_len; ++i) {
        plaintext[i] = ciphertext[i] ^ stream[i];
    }
    *plaintext_out = plaintext;
    *plaintext_len_out = ciphertext_len;
    plaintext = NULL;
    rc = 0;

done:
    free(ciphertext);
    free(nonce);
    free(tag);
    free(stream);
    free(plaintext);
    return rc;
}

int kvs_aead_decrypt(
    const kvs_aead_cipher *cipher,
    const kvs_cipher_record *record,
    const unsigned char *aad,
    size_t aad_len,
    unsigned char **plaintext_out,
    size_t *plaintext_len_out,
    char *err,
    size_t err_len
) {
    *plaintext_out = NULL;
    *plaintext_len_out = 0;
    if (strcmp(record->algorithm, "aes-gcm") == 0) {
        if (!cipher->aes_gcm_available) {
            kvs_errorf(err, err_len, "received aes-gcm record but AES-GCM support is unavailable");
            return -1;
        }
        return kvs_decrypt_aes_gcm(cipher, record, aad, aad_len, plaintext_out, plaintext_len_out, err, err_len);
    }
    if (strcmp(record->algorithm, "hmac-stream-v1") != 0) {
        kvs_errorf(err, err_len, "unsupported algorithm: %s", record->algorithm);
        return -1;
    }
    return kvs_decrypt_hmac_stream(cipher, record, aad, aad_len, plaintext_out, plaintext_len_out, err, err_len);
}
