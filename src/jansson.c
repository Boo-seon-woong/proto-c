#include "jansson.h"

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef enum {
    JSON_KIND_OBJECT = 1,
    JSON_KIND_ARRAY,
    JSON_KIND_STRING,
    JSON_KIND_INTEGER,
    JSON_KIND_TRUE,
    JSON_KIND_FALSE,
    JSON_KIND_NULL,
} json_kind;

typedef struct {
    char *key;
    json_t *value;
} json_object_entry;

struct json_t {
    json_kind kind;
    size_t refcount;
    union {
        struct {
            json_object_entry *items;
            size_t count;
            size_t capacity;
        } object;
        struct {
            json_t **items;
            size_t count;
            size_t capacity;
        } array;
        char *string;
        json_int_t integer;
    } as;
};

typedef struct {
    char *data;
    size_t len;
    size_t capacity;
} json_buffer;

typedef struct {
    const char *input;
    size_t length;
    size_t position;
    int line;
    int column;
    json_error_t *error;
} json_parser;

static char *json_strdup_local(const char *value) {
    size_t len;
    char *copy;

    if (value == NULL) {
        return NULL;
    }
    len = strlen(value);
    copy = malloc(len + 1u);
    if (copy == NULL) {
        return NULL;
    }
    memcpy(copy, value, len + 1u);
    return copy;
}

static void json_set_error(json_error_t *error, int line, int column, int position, const char *fmt, ...) {
    va_list args;

    if (error == NULL) {
        return;
    }
    error->line = line;
    error->column = column;
    error->position = position;
    va_start(args, fmt);
    vsnprintf(error->text, sizeof(error->text), fmt, args);
    va_end(args);
}

static void json_parser_error(json_parser *parser, const char *fmt, ...) {
    va_list args;

    if (parser == NULL || parser->error == NULL) {
        return;
    }
    parser->error->line = parser->line;
    parser->error->column = parser->column;
    parser->error->position = parser->position < (size_t) INT_MAX ? (int) parser->position : INT_MAX;
    va_start(args, fmt);
    vsnprintf(parser->error->text, sizeof(parser->error->text), fmt, args);
    va_end(args);
}

static json_t *json_alloc(json_kind kind) {
    json_t *json = calloc(1, sizeof(*json));
    if (json == NULL) {
        return NULL;
    }
    json->kind = kind;
    json->refcount = 1u;
    return json;
}

static bool json_buffer_reserve(json_buffer *buffer, size_t extra) {
    size_t required;
    size_t capacity;
    char *grown;

    required = buffer->len + extra + 1u;
    if (required <= buffer->capacity) {
        return true;
    }
    capacity = buffer->capacity == 0u ? 128u : buffer->capacity;
    while (capacity < required) {
        if (capacity > SIZE_MAX / 2u) {
            capacity = required;
            break;
        }
        capacity *= 2u;
    }
    grown = realloc(buffer->data, capacity);
    if (grown == NULL) {
        return false;
    }
    buffer->data = grown;
    buffer->capacity = capacity;
    return true;
}

static bool json_buffer_append_mem(json_buffer *buffer, const char *data, size_t len) {
    if (len == 0u) {
        return true;
    }
    if (!json_buffer_reserve(buffer, len)) {
        return false;
    }
    memcpy(buffer->data + buffer->len, data, len);
    buffer->len += len;
    buffer->data[buffer->len] = '\0';
    return true;
}

static bool json_buffer_append_cstr(json_buffer *buffer, const char *text) {
    return json_buffer_append_mem(buffer, text, strlen(text));
}

static bool json_buffer_append_char(json_buffer *buffer, char ch) {
    if (!json_buffer_reserve(buffer, 1u)) {
        return false;
    }
    buffer->data[buffer->len++] = ch;
    buffer->data[buffer->len] = '\0';
    return true;
}

static bool json_buffer_append_indent(json_buffer *buffer, size_t indent, int depth) {
    size_t total = indent * (size_t) depth;
    size_t i;

    if (!json_buffer_append_char(buffer, '\n')) {
        return false;
    }
    for (i = 0u; i < total; ++i) {
        if (!json_buffer_append_char(buffer, ' ')) {
            return false;
        }
    }
    return true;
}

static bool json_buffer_append_int(json_buffer *buffer, json_int_t value) {
    char text[64];
    int written = snprintf(text, sizeof(text), "%lld", (long long) value);

    if (written < 0) {
        return false;
    }
    return json_buffer_append_mem(buffer, text, (size_t) written);
}

static bool json_buffer_append_utf8(json_buffer *buffer, unsigned int codepoint) {
    char out[4];
    size_t len = 0u;

    if (codepoint <= 0x7fu) {
        out[len++] = (char) codepoint;
    } else if (codepoint <= 0x7ffu) {
        out[len++] = (char) (0xc0u | ((codepoint >> 6u) & 0x1fu));
        out[len++] = (char) (0x80u | (codepoint & 0x3fu));
    } else if (codepoint <= 0xffffu) {
        out[len++] = (char) (0xe0u | ((codepoint >> 12u) & 0x0fu));
        out[len++] = (char) (0x80u | ((codepoint >> 6u) & 0x3fu));
        out[len++] = (char) (0x80u | (codepoint & 0x3fu));
    } else if (codepoint <= 0x10ffffu) {
        out[len++] = (char) (0xf0u | ((codepoint >> 18u) & 0x07u));
        out[len++] = (char) (0x80u | ((codepoint >> 12u) & 0x3fu));
        out[len++] = (char) (0x80u | ((codepoint >> 6u) & 0x3fu));
        out[len++] = (char) (0x80u | (codepoint & 0x3fu));
    } else {
        return false;
    }
    return json_buffer_append_mem(buffer, out, len);
}

static bool json_dump_string(json_buffer *buffer, const char *value) {
    const unsigned char *cursor = (const unsigned char *) value;

    if (!json_buffer_append_char(buffer, '"')) {
        return false;
    }
    while (*cursor != '\0') {
        switch (*cursor) {
            case '\"':
                if (!json_buffer_append_cstr(buffer, "\\\"")) {
                    return false;
                }
                break;
            case '\\':
                if (!json_buffer_append_cstr(buffer, "\\\\")) {
                    return false;
                }
                break;
            case '\b':
                if (!json_buffer_append_cstr(buffer, "\\b")) {
                    return false;
                }
                break;
            case '\f':
                if (!json_buffer_append_cstr(buffer, "\\f")) {
                    return false;
                }
                break;
            case '\n':
                if (!json_buffer_append_cstr(buffer, "\\n")) {
                    return false;
                }
                break;
            case '\r':
                if (!json_buffer_append_cstr(buffer, "\\r")) {
                    return false;
                }
                break;
            case '\t':
                if (!json_buffer_append_cstr(buffer, "\\t")) {
                    return false;
                }
                break;
            default:
                if (*cursor < 0x20u) {
                    char escape[7];
                    int written = snprintf(escape, sizeof(escape), "\\u%04x", (unsigned int) *cursor);
                    if (written < 0 || !json_buffer_append_mem(buffer, escape, (size_t) written)) {
                        return false;
                    }
                } else if (!json_buffer_append_char(buffer, (char) *cursor)) {
                    return false;
                }
                break;
        }
        cursor += 1u;
    }
    return json_buffer_append_char(buffer, '"');
}

static int json_entry_compare(const void *lhs, const void *rhs) {
    const json_object_entry *const *left = lhs;
    const json_object_entry *const *right = rhs;
    return strcmp((*left)->key, (*right)->key);
}

static int json_dump_value(json_buffer *buffer, const json_t *json, size_t flags, int depth);

static int json_dump_object(json_buffer *buffer, const json_t *json, size_t flags, int depth) {
    bool compact = (flags & JSON_COMPACT) != 0u;
    size_t indent = flags & 0xffu;
    bool pretty = !compact && indent > 0u;
    json_object_entry **order = NULL;
    size_t i;

    if (json->as.object.count == 0u) {
        return json_buffer_append_cstr(buffer, "{}") ? 0 : -1;
    }
    if (!json_buffer_append_char(buffer, '{')) {
        return -1;
    }
    if ((flags & JSON_SORT_KEYS) != 0u && json->as.object.count > 1u) {
        order = malloc(json->as.object.count * sizeof(*order));
        if (order == NULL) {
            return -1;
        }
        for (i = 0u; i < json->as.object.count; ++i) {
            order[i] = &json->as.object.items[i];
        }
        qsort(order, json->as.object.count, sizeof(*order), json_entry_compare);
    }
    for (i = 0u; i < json->as.object.count; ++i) {
        const json_object_entry *entry = order != NULL ? order[i] : &json->as.object.items[i];
        if (pretty && !json_buffer_append_indent(buffer, indent, depth + 1)) {
            free(order);
            return -1;
        }
        if (!json_dump_string(buffer, entry->key) ||
            !json_buffer_append_char(buffer, ':') ||
            (!compact && indent > 0u && !json_buffer_append_char(buffer, ' ')) ||
            json_dump_value(buffer, entry->value, flags, depth + 1) != 0) {
            free(order);
            return -1;
        }
        if (i + 1u < json->as.object.count && !json_buffer_append_char(buffer, ',')) {
            free(order);
            return -1;
        }
    }
    free(order);
    if (pretty && !json_buffer_append_indent(buffer, indent, depth)) {
        return -1;
    }
    return json_buffer_append_char(buffer, '}') ? 0 : -1;
}

static int json_dump_array(json_buffer *buffer, const json_t *json, size_t flags, int depth) {
    bool compact = (flags & JSON_COMPACT) != 0u;
    size_t indent = flags & 0xffu;
    bool pretty = !compact && indent > 0u;
    size_t i;

    if (json->as.array.count == 0u) {
        return json_buffer_append_cstr(buffer, "[]") ? 0 : -1;
    }
    if (!json_buffer_append_char(buffer, '[')) {
        return -1;
    }
    for (i = 0u; i < json->as.array.count; ++i) {
        if (pretty && !json_buffer_append_indent(buffer, indent, depth + 1)) {
            return -1;
        }
        if (json_dump_value(buffer, json->as.array.items[i], flags, depth + 1) != 0) {
            return -1;
        }
        if (i + 1u < json->as.array.count && !json_buffer_append_char(buffer, ',')) {
            return -1;
        }
    }
    if (pretty && !json_buffer_append_indent(buffer, indent, depth)) {
        return -1;
    }
    return json_buffer_append_char(buffer, ']') ? 0 : -1;
}

static int json_dump_value(json_buffer *buffer, const json_t *json, size_t flags, int depth) {
    if (json == NULL) {
        return -1;
    }
    switch (json->kind) {
        case JSON_KIND_OBJECT:
            return json_dump_object(buffer, json, flags, depth);
        case JSON_KIND_ARRAY:
            return json_dump_array(buffer, json, flags, depth);
        case JSON_KIND_STRING:
            return json_dump_string(buffer, json->as.string) ? 0 : -1;
        case JSON_KIND_INTEGER:
            return json_buffer_append_int(buffer, json->as.integer) ? 0 : -1;
        case JSON_KIND_TRUE:
            return json_buffer_append_cstr(buffer, "true") ? 0 : -1;
        case JSON_KIND_FALSE:
            return json_buffer_append_cstr(buffer, "false") ? 0 : -1;
        case JSON_KIND_NULL:
            return json_buffer_append_cstr(buffer, "null") ? 0 : -1;
        default:
            return -1;
    }
}

static bool json_parser_done(const json_parser *parser) {
    return parser->position >= parser->length;
}

static char json_parser_peek(const json_parser *parser) {
    if (json_parser_done(parser)) {
        return '\0';
    }
    return parser->input[parser->position];
}

static void json_parser_advance(json_parser *parser) {
    if (json_parser_done(parser)) {
        return;
    }
    if (parser->input[parser->position] == '\n') {
        parser->line += 1;
        parser->column = 1;
    } else {
        parser->column += 1;
    }
    parser->position += 1u;
}

static void json_parser_skip_ws(json_parser *parser) {
    while (!json_parser_done(parser) && isspace((unsigned char) json_parser_peek(parser))) {
        json_parser_advance(parser);
    }
}

static unsigned int json_parser_parse_hex4(json_parser *parser, bool *ok) {
    unsigned int value = 0u;
    int i;

    *ok = false;
    for (i = 0; i < 4; ++i) {
        char ch = json_parser_peek(parser);
        value <<= 4u;
        if (ch >= '0' && ch <= '9') {
            value |= (unsigned int) (ch - '0');
        } else if (ch >= 'a' && ch <= 'f') {
            value |= (unsigned int) (ch - 'a' + 10);
        } else if (ch >= 'A' && ch <= 'F') {
            value |= (unsigned int) (ch - 'A' + 10);
        } else {
            json_parser_error(parser, "invalid unicode escape");
            return 0u;
        }
        json_parser_advance(parser);
    }
    *ok = true;
    return value;
}

static char *json_parse_string_raw(json_parser *parser) {
    json_buffer buffer = {0};

    if (json_parser_peek(parser) != '\"') {
        json_parser_error(parser, "expected string");
        return NULL;
    }
    json_parser_advance(parser);
    while (!json_parser_done(parser)) {
        char ch = json_parser_peek(parser);
        if (ch == '\"') {
            json_parser_advance(parser);
            return buffer.data != NULL ? buffer.data : json_strdup_local("");
        }
        if ((unsigned char) ch < 0x20u) {
            json_parser_error(parser, "unexpected control character in string");
            free(buffer.data);
            return NULL;
        }
        if (ch == '\\') {
            bool ok = false;
            json_parser_advance(parser);
            ch = json_parser_peek(parser);
            switch (ch) {
                case '\"':
                case '\\':
                case '/':
                    if (!json_buffer_append_char(&buffer, ch)) {
                        free(buffer.data);
                        return NULL;
                    }
                    json_parser_advance(parser);
                    break;
                case 'b':
                    if (!json_buffer_append_char(&buffer, '\b')) {
                        free(buffer.data);
                        return NULL;
                    }
                    json_parser_advance(parser);
                    break;
                case 'f':
                    if (!json_buffer_append_char(&buffer, '\f')) {
                        free(buffer.data);
                        return NULL;
                    }
                    json_parser_advance(parser);
                    break;
                case 'n':
                    if (!json_buffer_append_char(&buffer, '\n')) {
                        free(buffer.data);
                        return NULL;
                    }
                    json_parser_advance(parser);
                    break;
                case 'r':
                    if (!json_buffer_append_char(&buffer, '\r')) {
                        free(buffer.data);
                        return NULL;
                    }
                    json_parser_advance(parser);
                    break;
                case 't':
                    if (!json_buffer_append_char(&buffer, '\t')) {
                        free(buffer.data);
                        return NULL;
                    }
                    json_parser_advance(parser);
                    break;
                case 'u': {
                    unsigned int codepoint;
                    json_parser_advance(parser);
                    codepoint = json_parser_parse_hex4(parser, &ok);
                    if (!ok || !json_buffer_append_utf8(&buffer, codepoint)) {
                        free(buffer.data);
                        return NULL;
                    }
                    break;
                }
                default:
                    json_parser_error(parser, "invalid escape sequence");
                    free(buffer.data);
                    return NULL;
            }
        } else {
            if (!json_buffer_append_char(&buffer, ch)) {
                free(buffer.data);
                return NULL;
            }
            json_parser_advance(parser);
        }
    }
    json_parser_error(parser, "unterminated string");
    free(buffer.data);
    return NULL;
}

static int json_object_insert_internal(json_t *object, const char *key, bool steal_key, json_t *value) {
    size_t i;
    char *owned_key = (char *) key;

    if (object == NULL || object->kind != JSON_KIND_OBJECT || key == NULL || value == NULL) {
        if (steal_key) {
            free(owned_key);
        }
        json_decref(value);
        return -1;
    }
    for (i = 0u; i < object->as.object.count; ++i) {
        if (strcmp(object->as.object.items[i].key, key) == 0) {
            if (steal_key) {
                free(owned_key);
            }
            json_decref(object->as.object.items[i].value);
            object->as.object.items[i].value = value;
            return 0;
        }
    }
    if (object->as.object.count == object->as.object.capacity) {
        size_t new_capacity = object->as.object.capacity == 0u ? 8u : object->as.object.capacity * 2u;
        json_object_entry *grown = realloc(object->as.object.items, new_capacity * sizeof(*grown));
        if (grown == NULL) {
            if (steal_key) {
                free(owned_key);
            }
            json_decref(value);
            return -1;
        }
        object->as.object.items = grown;
        object->as.object.capacity = new_capacity;
    }
    if (!steal_key) {
        owned_key = json_strdup_local(key);
        if (owned_key == NULL) {
            json_decref(value);
            return -1;
        }
    }
    object->as.object.items[object->as.object.count].key = owned_key;
    object->as.object.items[object->as.object.count].value = value;
    object->as.object.count += 1u;
    return 0;
}

static json_t *json_parse_value(json_parser *parser);

static json_t *json_parse_object(json_parser *parser) {
    json_t *object = json_object();

    if (object == NULL) {
        return NULL;
    }
    json_parser_advance(parser);
    json_parser_skip_ws(parser);
    if (json_parser_peek(parser) == '}') {
        json_parser_advance(parser);
        return object;
    }
    while (!json_parser_done(parser)) {
        char *key;
        json_t *value;

        json_parser_skip_ws(parser);
        key = json_parse_string_raw(parser);
        if (key == NULL) {
            json_decref(object);
            return NULL;
        }
        json_parser_skip_ws(parser);
        if (json_parser_peek(parser) != ':') {
            free(key);
            json_parser_error(parser, "expected ':'");
            json_decref(object);
            return NULL;
        }
        json_parser_advance(parser);
        json_parser_skip_ws(parser);
        value = json_parse_value(parser);
        if (value == NULL) {
            free(key);
            json_decref(object);
            return NULL;
        }
        if (json_object_insert_internal(object, key, true, value) != 0) {
            json_decref(object);
            return NULL;
        }
        json_parser_skip_ws(parser);
        if (json_parser_peek(parser) == '}') {
            json_parser_advance(parser);
            return object;
        }
        if (json_parser_peek(parser) != ',') {
            json_parser_error(parser, "expected ',' or '}'");
            json_decref(object);
            return NULL;
        }
        json_parser_advance(parser);
    }
    json_parser_error(parser, "unterminated object");
    json_decref(object);
    return NULL;
}

static json_t *json_parse_array(json_parser *parser) {
    json_t *array = json_array();

    if (array == NULL) {
        return NULL;
    }
    json_parser_advance(parser);
    json_parser_skip_ws(parser);
    if (json_parser_peek(parser) == ']') {
        json_parser_advance(parser);
        return array;
    }
    while (!json_parser_done(parser)) {
        json_t *value;

        json_parser_skip_ws(parser);
        value = json_parse_value(parser);
        if (value == NULL) {
            json_decref(array);
            return NULL;
        }
        if (json_array_append_new(array, value) != 0) {
            json_decref(array);
            return NULL;
        }
        json_parser_skip_ws(parser);
        if (json_parser_peek(parser) == ']') {
            json_parser_advance(parser);
            return array;
        }
        if (json_parser_peek(parser) != ',') {
            json_parser_error(parser, "expected ',' or ']'");
            json_decref(array);
            return NULL;
        }
        json_parser_advance(parser);
    }
    json_parser_error(parser, "unterminated array");
    json_decref(array);
    return NULL;
}

static json_t *json_parse_number(json_parser *parser) {
    size_t start = parser->position;
    size_t len;
    char text[64];
    long long value;

    if (json_parser_peek(parser) == '-') {
        json_parser_advance(parser);
    }
    if (!isdigit((unsigned char) json_parser_peek(parser))) {
        json_parser_error(parser, "invalid number");
        return NULL;
    }
    if (json_parser_peek(parser) == '0') {
        json_parser_advance(parser);
    } else {
        while (isdigit((unsigned char) json_parser_peek(parser))) {
            json_parser_advance(parser);
        }
    }
    if (json_parser_peek(parser) == '.' || json_parser_peek(parser) == 'e' || json_parser_peek(parser) == 'E') {
        json_parser_error(parser, "only integer numbers are supported");
        return NULL;
    }
    len = parser->position - start;
    if (len == 0u || len >= sizeof(text)) {
        json_parser_error(parser, "invalid number");
        return NULL;
    }
    memcpy(text, parser->input + start, len);
    text[len] = '\0';
    errno = 0;
    value = strtoll(text, NULL, 10);
    if (errno == ERANGE) {
        json_parser_error(parser, "integer out of range");
        return NULL;
    }
    return json_integer((json_int_t) value);
}

static json_t *json_parse_value(json_parser *parser) {
    char ch;

    json_parser_skip_ws(parser);
    ch = json_parser_peek(parser);
    if (ch == '{') {
        return json_parse_object(parser);
    }
    if (ch == '[') {
        return json_parse_array(parser);
    }
    if (ch == '"') {
        char *raw = json_parse_string_raw(parser);
        json_t *json;
        if (raw == NULL) {
            return NULL;
        }
        json = json_string(raw);
        free(raw);
        return json;
    }
    if (ch == '-' || isdigit((unsigned char) ch)) {
        return json_parse_number(parser);
    }
    if (parser->length - parser->position >= 4u &&
        strncmp(parser->input + parser->position, "true", 4u) == 0) {
        parser->position += 4u;
        parser->column += 4;
        return json_true();
    }
    if (parser->length - parser->position >= 5u &&
        strncmp(parser->input + parser->position, "false", 5u) == 0) {
        parser->position += 5u;
        parser->column += 5;
        return json_false();
    }
    if (parser->length - parser->position >= 4u &&
        strncmp(parser->input + parser->position, "null", 4u) == 0) {
        parser->position += 4u;
        parser->column += 4;
        return json_null();
    }
    json_parser_error(parser, "unexpected token");
    return NULL;
}

json_t *json_object(void) {
    return json_alloc(JSON_KIND_OBJECT);
}

json_t *json_array(void) {
    return json_alloc(JSON_KIND_ARRAY);
}

json_t *json_string(const char *value) {
    json_t *json;

    if (value == NULL) {
        return NULL;
    }
    json = json_alloc(JSON_KIND_STRING);
    if (json == NULL) {
        return NULL;
    }
    json->as.string = json_strdup_local(value);
    if (json->as.string == NULL) {
        free(json);
        return NULL;
    }
    return json;
}

json_t *json_integer(json_int_t value) {
    json_t *json = json_alloc(JSON_KIND_INTEGER);
    if (json == NULL) {
        return NULL;
    }
    json->as.integer = value;
    return json;
}

json_t *json_true(void) {
    return json_alloc(JSON_KIND_TRUE);
}

json_t *json_false(void) {
    return json_alloc(JSON_KIND_FALSE);
}

json_t *json_null(void) {
    return json_alloc(JSON_KIND_NULL);
}

json_t *json_boolean(int value) {
    return value ? json_true() : json_false();
}

json_t *json_incref(json_t *json) {
    if (json != NULL) {
        json->refcount += 1u;
    }
    return json;
}

void json_decref(json_t *json) {
    size_t i;

    if (json == NULL) {
        return;
    }
    if (json->refcount > 1u) {
        json->refcount -= 1u;
        return;
    }
    switch (json->kind) {
        case JSON_KIND_OBJECT:
            for (i = 0u; i < json->as.object.count; ++i) {
                free(json->as.object.items[i].key);
                json_decref(json->as.object.items[i].value);
            }
            free(json->as.object.items);
            break;
        case JSON_KIND_ARRAY:
            for (i = 0u; i < json->as.array.count; ++i) {
                json_decref(json->as.array.items[i]);
            }
            free(json->as.array.items);
            break;
        case JSON_KIND_STRING:
            free(json->as.string);
            break;
        default:
            break;
    }
    free(json);
}

int json_object_set_new(json_t *object, const char *key, json_t *value) {
    return json_object_insert_internal(object, key, false, value);
}

int json_object_set(json_t *object, const char *key, json_t *value) {
    if (value == NULL) {
        return -1;
    }
    return json_object_set_new(object, key, json_incref(value));
}

json_t *json_object_get(const json_t *object, const char *key) {
    size_t i;

    if (object == NULL || object->kind != JSON_KIND_OBJECT || key == NULL) {
        return NULL;
    }
    for (i = 0u; i < object->as.object.count; ++i) {
        if (strcmp(object->as.object.items[i].key, key) == 0) {
            return object->as.object.items[i].value;
        }
    }
    return NULL;
}

int json_array_append_new(json_t *array, json_t *value) {
    if (array == NULL || array->kind != JSON_KIND_ARRAY || value == NULL) {
        json_decref(value);
        return -1;
    }
    if (array->as.array.count == array->as.array.capacity) {
        size_t new_capacity = array->as.array.capacity == 0u ? 8u : array->as.array.capacity * 2u;
        json_t **grown = realloc(array->as.array.items, new_capacity * sizeof(*grown));
        if (grown == NULL) {
            json_decref(value);
            return -1;
        }
        array->as.array.items = grown;
        array->as.array.capacity = new_capacity;
    }
    array->as.array.items[array->as.array.count++] = value;
    return 0;
}

size_t json_array_size(const json_t *array) {
    if (array == NULL || array->kind != JSON_KIND_ARRAY) {
        return 0u;
    }
    return array->as.array.count;
}

json_t *json_array_get(const json_t *array, size_t index) {
    if (array == NULL || array->kind != JSON_KIND_ARRAY || index >= array->as.array.count) {
        return NULL;
    }
    return array->as.array.items[index];
}

int json_is_object(const json_t *json) {
    return json != NULL && json->kind == JSON_KIND_OBJECT;
}

int json_is_array(const json_t *json) {
    return json != NULL && json->kind == JSON_KIND_ARRAY;
}

int json_is_string(const json_t *json) {
    return json != NULL && json->kind == JSON_KIND_STRING;
}

int json_is_integer(const json_t *json) {
    return json != NULL && json->kind == JSON_KIND_INTEGER;
}

int json_is_true(const json_t *json) {
    return json != NULL && json->kind == JSON_KIND_TRUE;
}

int json_is_false(const json_t *json) {
    return json != NULL && json->kind == JSON_KIND_FALSE;
}

int json_is_boolean(const json_t *json) {
    return json_is_true(json) || json_is_false(json);
}

int json_is_null(const json_t *json) {
    return json != NULL && json->kind == JSON_KIND_NULL;
}

const char *json_string_value(const json_t *json) {
    if (!json_is_string(json)) {
        return NULL;
    }
    return json->as.string;
}

json_int_t json_integer_value(const json_t *json) {
    if (!json_is_integer(json)) {
        return 0;
    }
    return json->as.integer;
}

char *json_dumps(const json_t *json, size_t flags) {
    json_buffer buffer = {0};

    if (json == NULL) {
        return NULL;
    }
    if (json_dump_value(&buffer, json, flags, 0) != 0) {
        free(buffer.data);
        return NULL;
    }
    if (buffer.data == NULL) {
        return json_strdup_local("");
    }
    return buffer.data;
}

int json_dumpf(const json_t *json, FILE *output, size_t flags) {
    char *text;
    int rc = -1;

    if (output == NULL) {
        return -1;
    }
    text = json_dumps(json, flags);
    if (text == NULL) {
        return -1;
    }
    if (fputs(text, output) >= 0) {
        rc = 0;
    }
    free(text);
    return rc;
}

int json_dump_file(const json_t *json, const char *path, size_t flags) {
    FILE *fp;
    int rc;

    if (path == NULL) {
        return -1;
    }
    fp = fopen(path, "w");
    if (fp == NULL) {
        return -1;
    }
    rc = json_dumpf(json, fp, flags);
    if (fclose(fp) != 0) {
        return -1;
    }
    return rc;
}

json_t *json_loads(const char *input, size_t flags, json_error_t *error) {
    json_parser parser;
    json_t *json;

    (void) flags;
    if (error != NULL) {
        memset(error, 0, sizeof(*error));
    }
    if (input == NULL) {
        json_set_error(error, 0, 0, 0, "input is null");
        return NULL;
    }
    parser.input = input;
    parser.length = strlen(input);
    parser.position = 0u;
    parser.line = 1;
    parser.column = 1;
    parser.error = error;

    json = json_parse_value(&parser);
    if (json == NULL) {
        return NULL;
    }
    json_parser_skip_ws(&parser);
    if (!json_parser_done(&parser)) {
        json_parser_error(&parser, "unexpected trailing data");
        json_decref(json);
        return NULL;
    }
    return json;
}

json_t *json_load_file(const char *path, size_t flags, json_error_t *error) {
    FILE *fp;
    long size;
    char *buffer;
    size_t read_len;
    json_t *json;

    (void) flags;
    if (error != NULL) {
        memset(error, 0, sizeof(*error));
    }
    if (path == NULL) {
        json_set_error(error, 0, 0, 0, "path is null");
        return NULL;
    }
    fp = fopen(path, "rb");
    if (fp == NULL) {
        json_set_error(error, 0, 0, 0, "failed to open %s: %s", path, strerror(errno));
        return NULL;
    }
    if (fseek(fp, 0, SEEK_END) != 0) {
        fclose(fp);
        json_set_error(error, 0, 0, 0, "failed to seek %s", path);
        return NULL;
    }
    size = ftell(fp);
    if (size < 0) {
        fclose(fp);
        json_set_error(error, 0, 0, 0, "failed to stat %s", path);
        return NULL;
    }
    rewind(fp);
    buffer = malloc((size_t) size + 1u);
    if (buffer == NULL) {
        fclose(fp);
        json_set_error(error, 0, 0, 0, "out of memory");
        return NULL;
    }
    read_len = fread(buffer, 1u, (size_t) size, fp);
    if (read_len != (size_t) size) {
        free(buffer);
        fclose(fp);
        json_set_error(error, 0, 0, 0, "failed to read %s", path);
        return NULL;
    }
    buffer[read_len] = '\0';
    fclose(fp);
    json = json_loads(buffer, 0, error);
    free(buffer);
    return json;
}

void *json_object_iter(const json_t *object) {
    if (!json_is_object(object) || object->as.object.count == 0u) {
        return NULL;
    }
    return &object->as.object.items[0];
}

void *json_object_iter_next(const json_t *object, void *iter) {
    json_object_entry *entry = iter;
    ptrdiff_t index;

    if (!json_is_object(object) || iter == NULL) {
        return NULL;
    }
    index = entry - object->as.object.items;
    if (index < 0 || (size_t) index + 1u >= object->as.object.count) {
        return NULL;
    }
    return &object->as.object.items[index + 1];
}

const char *json_object_iter_key(void *iter) {
    if (iter == NULL) {
        return NULL;
    }
    return ((json_object_entry *) iter)->key;
}

json_t *json_object_iter_value(void *iter) {
    if (iter == NULL) {
        return NULL;
    }
    return ((json_object_entry *) iter)->value;
}

static void json_pack_skip_ws(const char **fmt) {
    while (**fmt != '\0' && isspace((unsigned char) **fmt)) {
        *fmt += 1;
    }
}

json_t *json_pack(const char *fmt, ...) {
    va_list args;
    json_t *object;

    if (fmt == NULL) {
        return NULL;
    }
    json_pack_skip_ws(&fmt);
    if (*fmt != '{') {
        return NULL;
    }
    fmt += 1;
    object = json_object();
    if (object == NULL) {
        return NULL;
    }
    va_start(args, fmt);
    json_pack_skip_ws(&fmt);
    if (*fmt == '}') {
        va_end(args);
        return object;
    }
    while (*fmt != '\0') {
        const char *key;
        char type;
        json_t *value = NULL;

        json_pack_skip_ws(&fmt);
        if (*fmt != 's') {
            break;
        }
        fmt += 1;
        json_pack_skip_ws(&fmt);
        if (*fmt == ':') {
            fmt += 1;
            json_pack_skip_ws(&fmt);
        }
        type = *fmt;
        if (type == '\0') {
            break;
        }
        fmt += 1;
        key = va_arg(args, const char *);
        switch (type) {
            case 's':
                value = json_string(va_arg(args, const char *));
                break;
            case 'I':
                value = json_integer(va_arg(args, json_int_t));
                break;
            case 'b':
                value = json_boolean(va_arg(args, int));
                break;
            case 'o':
                value = va_arg(args, json_t *);
                break;
            default:
                break;
        }
        if (value == NULL) {
            va_end(args);
            json_decref(object);
            return NULL;
        }
        if (json_object_set_new(object, key, value) != 0) {
            va_end(args);
            json_decref(object);
            return NULL;
        }
        json_pack_skip_ws(&fmt);
        if (*fmt == ',') {
            fmt += 1;
            continue;
        }
        if (*fmt == '}') {
            va_end(args);
            return object;
        }
        break;
    }
    va_end(args);
    json_decref(object);
    return NULL;
}
