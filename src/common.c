#include "kvs/common.h"

#include <errno.h>
#include <openssl/sha.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

void kvs_errorf(char *buf, size_t len, const char *fmt, ...) {
    va_list args;

    if (buf == NULL || len == 0) {
        return;
    }

    va_start(args, fmt);
    vsnprintf(buf, len, fmt, args);
    va_end(args);
}

char *kvs_strdup(const char *src) {
    size_t len;
    char *copy;

    if (src == NULL) {
        return NULL;
    }
    len = strlen(src);
    copy = malloc(len + 1);
    if (copy == NULL) {
        return NULL;
    }
    memcpy(copy, src, len + 1);
    return copy;
}

char *kvs_path_join(const char *left, const char *right) {
    size_t left_len;
    size_t right_len;
    bool add_sep;
    char *out;

    if (left == NULL || right == NULL) {
        return NULL;
    }

    left_len = strlen(left);
    right_len = strlen(right);
    add_sep = left_len > 0 && left[left_len - 1] != '/';
    out = malloc(left_len + right_len + (add_sep ? 2u : 1u));
    if (out == NULL) {
        return NULL;
    }

    memcpy(out, left, left_len);
    if (add_sep) {
        out[left_len] = '/';
        memcpy(out + left_len + 1, right, right_len + 1);
    } else {
        memcpy(out + left_len, right, right_len + 1);
    }

    return out;
}

static int kvs_mkdir_single(const char *path) {
    if (mkdir(path, 0777) == 0) {
        return 0;
    }
    if (errno == EEXIST) {
        return 0;
    }
    return -1;
}

int kvs_mkdir_p(const char *path, char *err, size_t err_len) {
    char *copy;
    char *p;

    if (path == NULL || *path == '\0') {
        kvs_errorf(err, err_len, "path is empty");
        return -1;
    }

    copy = kvs_strdup(path);
    if (copy == NULL) {
        kvs_errorf(err, err_len, "out of memory");
        return -1;
    }

    for (p = copy + 1; *p != '\0'; ++p) {
        if (*p != '/') {
            continue;
        }
        *p = '\0';
        if (kvs_mkdir_single(copy) != 0) {
            kvs_errorf(err, err_len, "mkdir(%s) failed: %s", copy, strerror(errno));
            free(copy);
            return -1;
        }
        *p = '/';
    }

    if (kvs_mkdir_single(copy) != 0) {
        kvs_errorf(err, err_len, "mkdir(%s) failed: %s", copy, strerror(errno));
        free(copy);
        return -1;
    }

    free(copy);
    return 0;
}

uint64_t kvs_sha256_hash64(const char *text) {
    unsigned char digest[SHA256_DIGEST_LENGTH];

    SHA256((const unsigned char *) text, strlen(text), digest);
    return ((uint64_t) digest[0] << 56) |
           ((uint64_t) digest[1] << 48) |
           ((uint64_t) digest[2] << 40) |
           ((uint64_t) digest[3] << 32) |
           ((uint64_t) digest[4] << 24) |
           ((uint64_t) digest[5] << 16) |
           ((uint64_t) digest[6] << 8) |
           (uint64_t) digest[7];
}
