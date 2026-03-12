#ifndef MINI_JANSSON_H
#define MINI_JANSSON_H

#include <stddef.h>
#include <stdio.h>

typedef long long json_int_t;

typedef struct json_t json_t;

typedef struct {
    int line;
    int column;
    int position;
    char text[160];
} json_error_t;

#define JSON_COMPACT ((size_t) 1u << 8)
#define JSON_SORT_KEYS ((size_t) 1u << 9)
#define JSON_INDENT(n) ((size_t) ((n) & 0xffu))

json_t *json_object(void);
json_t *json_array(void);
json_t *json_string(const char *value);
json_t *json_integer(json_int_t value);
json_t *json_boolean(int value);
json_t *json_true(void);
json_t *json_false(void);
json_t *json_null(void);

json_t *json_pack(const char *fmt, ...);

json_t *json_incref(json_t *json);
void json_decref(json_t *json);

int json_object_set_new(json_t *object, const char *key, json_t *value);
int json_object_set(json_t *object, const char *key, json_t *value);
json_t *json_object_get(const json_t *object, const char *key);

int json_array_append_new(json_t *array, json_t *value);
size_t json_array_size(const json_t *array);
json_t *json_array_get(const json_t *array, size_t index);

int json_is_object(const json_t *json);
int json_is_array(const json_t *json);
int json_is_string(const json_t *json);
int json_is_integer(const json_t *json);
int json_is_true(const json_t *json);
int json_is_false(const json_t *json);
int json_is_boolean(const json_t *json);
int json_is_null(const json_t *json);

const char *json_string_value(const json_t *json);
json_int_t json_integer_value(const json_t *json);

char *json_dumps(const json_t *json, size_t flags);
int json_dumpf(const json_t *json, FILE *output, size_t flags);
int json_dump_file(const json_t *json, const char *path, size_t flags);
json_t *json_loads(const char *input, size_t flags, json_error_t *error);
json_t *json_load_file(const char *path, size_t flags, json_error_t *error);

void *json_object_iter(const json_t *object);
void *json_object_iter_next(const json_t *object, void *iter);
const char *json_object_iter_key(void *iter);
json_t *json_object_iter_value(void *iter);

#define json_array_foreach(array, index, value) \
    for ((index) = 0; \
         (index) < json_array_size(array) && (((value) = json_array_get((array), (index))) || 1); \
         ++(index))

#define json_object_foreach(object, key, value) \
    for (void *_json_iter = json_object_iter(object); \
         _json_iter != NULL && (((key) = json_object_iter_key(_json_iter)), ((value) = json_object_iter_value(_json_iter)), 1); \
         _json_iter = json_object_iter_next((object), _json_iter))

#endif
