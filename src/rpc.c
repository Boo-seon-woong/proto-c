#include "kvs/rpc.h"

#include "kvs/common.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

typedef struct {
    int fd;
    kvs_tcp_server *server;
} kvs_tcp_conn_args;

static int kvs_socket_sendall(int fd, const char *buf, size_t len, char *err, size_t err_len) {
    size_t sent = 0;

    while (sent < len) {
        ssize_t rc = send(fd, buf + sent, len - sent, 0);
        if (rc < 0) {
            if (errno == EINTR) {
                continue;
            }
            kvs_errorf(err, err_len, "send failed: %s", strerror(errno));
            return -1;
        }
        if (rc == 0) {
            kvs_errorf(err, err_len, "peer closed during send");
            return -1;
        }
        sent += (size_t) rc;
    }
    return 0;
}

static int kvs_socket_recv_line(int fd, char **line_out, char *err, size_t err_len) {
    size_t capacity = 4096;
    size_t len = 0;
    char *buffer = malloc(capacity);

    if (buffer == NULL) {
        kvs_errorf(err, err_len, "out of memory");
        return -1;
    }

    while (len < KVS_RPC_MAX_LINE) {
        ssize_t rc = recv(fd, buffer + len, capacity - len, 0);
        char *newline;
        if (rc < 0) {
            if (errno == EINTR) {
                continue;
            }
            free(buffer);
            kvs_errorf(err, err_len, "recv failed: %s", strerror(errno));
            return -1;
        }
        if (rc == 0) {
            break;
        }
        len += (size_t) rc;
        newline = memchr(buffer, '\n', len);
        if (newline != NULL) {
            size_t line_len = (size_t) (newline - buffer) + 1u;
            buffer[line_len] = '\0';
            *line_out = buffer;
            return 0;
        }
        if (len == capacity) {
            char *grown;
            if (capacity >= KVS_RPC_MAX_LINE) {
                free(buffer);
                kvs_errorf(err, err_len, "RPC line exceeded limit");
                return -1;
            }
            capacity *= 2u;
            if (capacity > KVS_RPC_MAX_LINE) {
                capacity = KVS_RPC_MAX_LINE;
            }
            grown = realloc(buffer, capacity + 1u);
            if (grown == NULL) {
                free(buffer);
                kvs_errorf(err, err_len, "out of memory");
                return -1;
            }
            buffer = grown;
        }
    }

    if (len == 0u) {
        free(buffer);
        kvs_errorf(err, err_len, "connection closed without response");
        return -1;
    }
    buffer[len] = '\0';
    *line_out = buffer;
    return 0;
}

static int kvs_tcp_connect(const char *host, uint16_t port, double timeout_sec, char *err, size_t err_len) {
    struct addrinfo hints;
    struct addrinfo *result = NULL;
    struct addrinfo *rp;
    char port_text[16];
    int fd = -1;
    int rc;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    snprintf(port_text, sizeof(port_text), "%u", (unsigned int) port);
    rc = getaddrinfo(host, port_text, &hints, &result);
    if (rc != 0) {
        kvs_errorf(err, err_len, "getaddrinfo(%s:%u) failed: %s", host, (unsigned int) port, gai_strerror(rc));
        return -1;
    }

    for (rp = result; rp != NULL; rp = rp->ai_next) {
        struct timeval timeout;
        fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (fd < 0) {
            continue;
        }
        timeout.tv_sec = (time_t) timeout_sec;
        timeout.tv_usec = (suseconds_t) ((timeout_sec - (double) timeout.tv_sec) * 1000000.0);
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
        if (connect(fd, rp->ai_addr, rp->ai_addrlen) == 0) {
            freeaddrinfo(result);
            return fd;
        }
        close(fd);
        fd = -1;
    }

    kvs_errorf(err, err_len, "connect(%s:%u) failed: %s", host, (unsigned int) port, strerror(errno));
    freeaddrinfo(result);
    return -1;
}

int kvs_rpc_call(
    const kvs_endpoint *endpoint,
    const char *action,
    json_t *params,
    double timeout_sec,
    json_t **response_out,
    char *err,
    size_t err_len
) {
    int fd = -1;
    json_t *request = NULL;
    json_t *response = NULL;
    char *encoded = NULL;
    char *line = NULL;
    json_error_t json_error;
    size_t encoded_len;
    int rc = -1;

    *response_out = NULL;
    request = json_object();
    if (request == NULL) {
        kvs_errorf(err, err_len, "out of memory");
        goto done;
    }
    if (json_object_set_new(request, "action", json_string(action)) != 0) {
        kvs_errorf(err, err_len, "out of memory");
        goto done;
    }
    if (params == NULL) {
        params = json_object();
        if (params == NULL) {
            kvs_errorf(err, err_len, "out of memory");
            goto done;
        }
        if (json_object_set_new(request, "params", params) != 0) {
            kvs_errorf(err, err_len, "out of memory");
            goto done;
        }
    } else if (json_object_set(request, "params", params) != 0) {
        kvs_errorf(err, err_len, "out of memory");
        goto done;
    }

    encoded = json_dumps(request, JSON_COMPACT);
    if (encoded == NULL) {
        kvs_errorf(err, err_len, "json_dumps failed");
        goto done;
    }
    encoded_len = strlen(encoded);
    fd = kvs_tcp_connect(endpoint->host, endpoint->port, timeout_sec, err, err_len);
    if (fd < 0) {
        goto done;
    }
    if (kvs_socket_sendall(fd, encoded, encoded_len, err, err_len) != 0 ||
        kvs_socket_sendall(fd, "\n", 1u, err, err_len) != 0 ||
        kvs_socket_recv_line(fd, &line, err, err_len) != 0) {
        goto done;
    }

    response = json_loads(line, 0, &json_error);
    if (response == NULL || !json_is_object(response)) {
        kvs_errorf(err, err_len, "%s returned invalid json: %s", endpoint->node_id, json_error.text);
        goto done;
    }
    if (!json_is_true(json_object_get(response, "ok"))) {
        json_t *error_json = json_object_get(response, "error");
        kvs_errorf(
            err,
            err_len,
            "%s error: %s",
            endpoint->node_id,
            json_is_string(error_json) ? json_string_value(error_json) : "unknown"
        );
        goto done;
    }
    *response_out = response;
    response = NULL;
    rc = 0;

done:
    if (fd >= 0) {
        close(fd);
    }
    free(encoded);
    free(line);
    json_decref(request);
    json_decref(response);
    return rc;
}

static json_t *kvs_rpc_error_response(const char *message) {
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

static void *kvs_tcp_handle_conn(void *arg) {
    kvs_tcp_conn_args *conn = arg;
    char err[256] = {0};
    char *line = NULL;
    json_t *request = NULL;
    json_t *response = NULL;
    char *encoded = NULL;

    if (kvs_socket_recv_line(conn->fd, &line, err, sizeof(err)) != 0) {
        goto done;
    }
    request = json_loads(line, 0, NULL);
    if (request == NULL) {
        response = kvs_rpc_error_response("invalid json");
    } else {
        response = conn->server->handler(conn->server->handler_ctx, request);
    }
    if (response == NULL) {
        response = kvs_rpc_error_response("internal server error");
    }
    encoded = json_dumps(response, JSON_COMPACT);
    if (encoded != NULL) {
        kvs_socket_sendall(conn->fd, encoded, strlen(encoded), err, sizeof(err));
        kvs_socket_sendall(conn->fd, "\n", 1u, err, sizeof(err));
    }

done:
    if (conn->fd >= 0) {
        close(conn->fd);
    }
    free(line);
    free(encoded);
    json_decref(request);
    json_decref(response);
    free(conn);
    return NULL;
}

static void *kvs_tcp_accept_loop(void *arg) {
    kvs_tcp_server *server = arg;

    while (!server->stop_requested) {
        int fd = accept(server->listener_fd, NULL, NULL);
        if (fd < 0) {
            if (server->stop_requested || errno == EBADF || errno == EINVAL) {
                break;
            }
            if (errno == EINTR) {
                continue;
            }
            continue;
        }
        kvs_tcp_conn_args *conn = malloc(sizeof(*conn));
        pthread_t thread;
        if (conn == NULL) {
            close(fd);
            continue;
        }
        conn->fd = fd;
        conn->server = server;
        if (pthread_create(&thread, NULL, kvs_tcp_handle_conn, conn) != 0) {
            close(fd);
            free(conn);
            continue;
        }
        pthread_detach(thread);
    }
    return NULL;
}

int kvs_tcp_server_start(
    kvs_tcp_server *server,
    const char *host,
    uint16_t port,
    kvs_rpc_handler_fn handler,
    void *handler_ctx,
    char *err,
    size_t err_len
) {
    struct addrinfo hints;
    struct addrinfo *result = NULL;
    struct addrinfo *rp;
    char port_text[16];
    int yes = 1;
    int rc;

    memset(server, 0, sizeof(*server));
    server->listener_fd = -1;
    server->handler = handler;
    server->handler_ctx = handler_ctx;
    server->requested_port = port;
    snprintf(server->bind_host, sizeof(server->bind_host), "%s", host);

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;
    snprintf(port_text, sizeof(port_text), "%u", (unsigned int) port);
    rc = getaddrinfo(host, port_text, &hints, &result);
    if (rc != 0) {
        kvs_errorf(err, err_len, "getaddrinfo(%s:%u) failed: %s", host, (unsigned int) port, gai_strerror(rc));
        return -1;
    }

    for (rp = result; rp != NULL; rp = rp->ai_next) {
        struct sockaddr_in addr;
        socklen_t addr_len = sizeof(addr);
        server->listener_fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (server->listener_fd < 0) {
            continue;
        }
        setsockopt(server->listener_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
        if (bind(server->listener_fd, rp->ai_addr, rp->ai_addrlen) == 0 && listen(server->listener_fd, 256) == 0) {
            if (getsockname(server->listener_fd, (struct sockaddr *) &addr, &addr_len) == 0) {
                server->bound_port = ntohs(addr.sin_port);
            }
            break;
        }
        close(server->listener_fd);
        server->listener_fd = -1;
    }
    freeaddrinfo(result);

    if (server->listener_fd < 0) {
        kvs_errorf(err, err_len, "bind/listen failed for %s:%u", host, (unsigned int) port);
        return -1;
    }
    if (pthread_create(&server->thread, NULL, kvs_tcp_accept_loop, server) != 0) {
        kvs_errorf(err, err_len, "pthread_create failed");
        close(server->listener_fd);
        server->listener_fd = -1;
        return -1;
    }
    server->started = true;
    return 0;
}

void kvs_tcp_server_stop(kvs_tcp_server *server) {
    if (server == NULL || !server->started) {
        return;
    }
    server->stop_requested = true;
    if (server->listener_fd >= 0) {
        shutdown(server->listener_fd, SHUT_RDWR);
        close(server->listener_fd);
        server->listener_fd = -1;
    }
    pthread_join(server->thread, NULL);
    server->started = false;
}
