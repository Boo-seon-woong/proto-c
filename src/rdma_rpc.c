#include "kvs/rdma_rpc.h"

#include "kvs/common.h"

#include <arpa/inet.h>
#include <dirent.h>
#include <errno.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <netinet/in.h>
#include <rdma/rsocket.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

typedef struct {
    int fd;
    kvs_rdma_server *server;
} kvs_rdma_conn_args;

static int kvs_rdma_sendall(int fd, const char *buf, size_t len, char *err, size_t err_len) {
    size_t sent = 0;

    while (sent < len) {
        ssize_t rc = rsend(fd, buf + sent, len - sent, 0);
        if (rc < 0) {
            if (errno == EINTR) {
                continue;
            }
            kvs_errorf(err, err_len, "rsend failed: %s", strerror(errno));
            return -1;
        }
        if (rc == 0) {
            kvs_errorf(err, err_len, "peer closed during rsend");
            return -1;
        }
        sent += (size_t) rc;
    }
    return 0;
}

static int kvs_rdma_recv_line(int fd, char **line_out, char *err, size_t err_len) {
    size_t capacity = 4096;
    size_t len = 0;
    char *buffer = malloc(capacity);

    if (buffer == NULL) {
        kvs_errorf(err, err_len, "out of memory");
        return -1;
    }

    while (len < KVS_RPC_MAX_LINE) {
        ssize_t rc = rrecv(fd, buffer + len, capacity - len, 0);
        char *newline;
        if (rc < 0) {
            if (errno == EINTR) {
                continue;
            }
            free(buffer);
            kvs_errorf(err, err_len, "rrecv failed: %s", strerror(errno));
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
                kvs_errorf(err, err_len, "RDMA message exceeded limit");
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
        kvs_errorf(err, err_len, "RDMA peer closed without response");
        return -1;
    }
    buffer[len] = '\0';
    *line_out = buffer;
    return 0;
}

static int kvs_resolve_ipv4(const char *host, uint16_t port, struct sockaddr_in *addr, char *err, size_t err_len) {
    struct addrinfo hints;
    struct addrinfo *result = NULL;
    char port_text[16];
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
    memcpy(addr, result->ai_addr, sizeof(*addr));
    freeaddrinfo(result);
    return 0;
}

bool kvs_rdma_supported(void) {
    return true;
}

void kvs_rdma_transport_profile(kvs_rdma_profile *profile) {
    memset(profile, 0, sizeof(*profile));
    profile->supported = true;
    profile->library = "librdmacm.so.1";
    profile->implementation = "rsocket-rpc";
    profile->one_sided = false;
    profile->mn_cpu_bypass = false;
}

int kvs_rdma_call(
    const kvs_rdma_endpoint *endpoint,
    const char *action,
    json_t *params,
    json_t **response_out,
    char *err,
    size_t err_len
) {
    int fd = -1;
    struct sockaddr_in addr;
    json_t *request = NULL;
    json_t *response = NULL;
    char *encoded = NULL;
    char *line = NULL;
    json_error_t json_error;
    int rc = -1;

    *response_out = NULL;
    if (kvs_resolve_ipv4(endpoint->host, endpoint->port, &addr, err, err_len) != 0) {
        return -1;
    }

    request = json_object();
    if (request == NULL ||
        json_object_set_new(request, "action", json_string(action)) != 0) {
        kvs_errorf(err, err_len, "out of memory");
        goto done;
    }
    if (params == NULL) {
        params = json_object();
        if (params == NULL || json_object_set_new(request, "params", params) != 0) {
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

    fd = rsocket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        kvs_errorf(err, err_len, "rsocket failed: %s", strerror(errno));
        goto done;
    }
    if (rconnect(fd, (struct sockaddr *) &addr, sizeof(addr)) < 0) {
        kvs_errorf(err, err_len, "rconnect failed: %s", strerror(errno));
        goto done;
    }
    if (kvs_rdma_sendall(fd, encoded, strlen(encoded), err, err_len) != 0 ||
        kvs_rdma_sendall(fd, "\n", 1u, err, err_len) != 0 ||
        kvs_rdma_recv_line(fd, &line, err, err_len) != 0) {
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
        rclose(fd);
    }
    free(encoded);
    free(line);
    json_decref(request);
    json_decref(response);
    return rc;
}

static json_t *kvs_rdma_error_response(const char *message) {
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

static void *kvs_rdma_handle_conn(void *arg) {
    kvs_rdma_conn_args *conn = arg;
    char err[256] = {0};
    char *line = NULL;
    json_t *request = NULL;
    json_t *response = NULL;
    char *encoded = NULL;

    if (kvs_rdma_recv_line(conn->fd, &line, err, sizeof(err)) != 0) {
        goto done;
    }
    request = json_loads(line, 0, NULL);
    if (request == NULL) {
        response = kvs_rdma_error_response("invalid json");
    } else {
        response = conn->server->handler(conn->server->handler_ctx, request);
    }
    if (response == NULL) {
        response = kvs_rdma_error_response("internal server error");
    }
    encoded = json_dumps(response, JSON_COMPACT);
    if (encoded != NULL) {
        kvs_rdma_sendall(conn->fd, encoded, strlen(encoded), err, sizeof(err));
        kvs_rdma_sendall(conn->fd, "\n", 1u, err, sizeof(err));
    }

done:
    if (conn->fd >= 0) {
        rclose(conn->fd);
    }
    free(line);
    free(encoded);
    json_decref(request);
    json_decref(response);
    free(conn);
    return NULL;
}

static void *kvs_rdma_accept_loop(void *arg) {
    kvs_rdma_server *server = arg;

    while (!server->stop_requested) {
        int fd = raccept(server->listener_fd, NULL, NULL);
        if (fd < 0) {
            if (server->stop_requested || errno == EBADF || errno == EINVAL) {
                break;
            }
            if (errno == EINTR) {
                continue;
            }
            continue;
        }
        kvs_rdma_conn_args *conn = malloc(sizeof(*conn));
        pthread_t thread;
        if (conn == NULL) {
            rclose(fd);
            continue;
        }
        conn->fd = fd;
        conn->server = server;
        if (pthread_create(&thread, NULL, kvs_rdma_handle_conn, conn) != 0) {
            rclose(fd);
            free(conn);
            continue;
        }
        pthread_detach(thread);
    }
    return NULL;
}

int kvs_rdma_server_start(
    kvs_rdma_server *server,
    const char *host,
    uint16_t port,
    kvs_rdma_handler_fn handler,
    void *handler_ctx,
    char *err,
    size_t err_len
) {
    struct sockaddr_in addr;

    memset(server, 0, sizeof(*server));
    server->listener_fd = -1;
    server->handler = handler;
    server->handler_ctx = handler_ctx;
    server->requested_port = port;
    snprintf(server->bind_host, sizeof(server->bind_host), "%s", host);

    if (kvs_resolve_ipv4(host, port, &addr, err, err_len) != 0) {
        return -1;
    }
    server->listener_fd = rsocket(AF_INET, SOCK_STREAM, 0);
    if (server->listener_fd < 0) {
        kvs_errorf(err, err_len, "rsocket(listener) failed: %s", strerror(errno));
        return -1;
    }
    if (rbind(server->listener_fd, (struct sockaddr *) &addr, sizeof(addr)) < 0) {
        kvs_errorf(err, err_len, "rbind failed: %s", strerror(errno));
        rclose(server->listener_fd);
        server->listener_fd = -1;
        return -1;
    }
    if (rlisten(server->listener_fd, 256) < 0) {
        kvs_errorf(err, err_len, "rlisten failed: %s", strerror(errno));
        rclose(server->listener_fd);
        server->listener_fd = -1;
        return -1;
    }
    server->bound_port = port;
    if (pthread_create(&server->thread, NULL, kvs_rdma_accept_loop, server) != 0) {
        kvs_errorf(err, err_len, "pthread_create failed");
        rclose(server->listener_fd);
        server->listener_fd = -1;
        return -1;
    }
    server->started = true;
    return 0;
}

void kvs_rdma_server_stop(kvs_rdma_server *server) {
    if (server == NULL || !server->started) {
        return;
    }
    server->stop_requested = true;
    if (server->listener_fd >= 0) {
        rclose(server->listener_fd);
        server->listener_fd = -1;
    }
    pthread_join(server->thread, NULL);
    server->started = false;
}

static bool kvs_rdma_device_is_active(const char *device) {
    char path[1024];
    DIR *ports = NULL;
    struct dirent *entry;
    bool active = false;

    snprintf(path, sizeof(path), "/sys/class/infiniband/%s/ports", device);
    ports = opendir(path);
    if (ports == NULL) {
        return false;
    }
    while ((entry = readdir(ports)) != NULL) {
        FILE *fp;
        char *port_path = NULL;
        char *state_path = NULL;
        char state[128];
        if (entry->d_name[0] == '.') {
            continue;
        }
        port_path = kvs_path_join(path, entry->d_name);
        state_path = kvs_path_join(port_path, "state");
        free(port_path);
        fp = state_path != NULL ? fopen(state_path, "r") : NULL;
        free(state_path);
        if (fp == NULL) {
            continue;
        }
        if (fgets(state, sizeof(state), fp) != NULL && strstr(state, "ACTIVE") != NULL) {
            active = true;
            fclose(fp);
            break;
        }
        fclose(fp);
    }
    closedir(ports);
    return active;
}

void kvs_rdma_print_local_nics(FILE *out) {
    DIR *base = opendir("/sys/class/infiniband");
    struct dirent *entry;

    if (base == NULL) {
        fprintf(out, "RDMA NIC inventory: none detected\n");
        return;
    }
    while ((entry = readdir(base)) != NULL) {
        char ports_path[1024];
        char net_path[1024];
        DIR *ports = NULL;
        DIR *nets = NULL;
        struct dirent *sub;
        bool active;
        char netdevs[1024] = "";
        snprintf(ports_path, sizeof(ports_path), "/sys/class/infiniband/%s/ports", entry->d_name);
        snprintf(net_path, sizeof(net_path), "/sys/class/infiniband/%s/device/net", entry->d_name);
        if (entry->d_name[0] == '.') {
            continue;
        }
        active = kvs_rdma_device_is_active(entry->d_name);
        nets = opendir(net_path);
        if (nets != NULL) {
            bool first = true;
            while ((sub = readdir(nets)) != NULL) {
                if (sub->d_name[0] == '.') {
                    continue;
                }
                if (!first) {
                    strncat(netdevs, ",", sizeof(netdevs) - strlen(netdevs) - 1u);
                }
                snprintf(netdevs + strlen(netdevs), sizeof(netdevs) - strlen(netdevs), "%s", sub->d_name);
                first = false;
            }
            closedir(nets);
        }
        fprintf(
            out,
            "RDMA NIC: device=%s active=%s netdevs=%s\n",
            entry->d_name,
            active ? "true" : "false",
            netdevs[0] == '\0' ? "-" : netdevs
        );
        ports = opendir(ports_path);
        if (ports == NULL) {
            continue;
        }
        while ((sub = readdir(ports)) != NULL) {
            FILE *fp;
            char *port_path = NULL;
            char *state_path = NULL;
            char *link_path = NULL;
            char state[128] = "";
            char link_layer[128] = "";
            if (sub->d_name[0] == '.') {
                continue;
            }
            port_path = kvs_path_join(ports_path, sub->d_name);
            state_path = kvs_path_join(port_path, "state");
            link_path = kvs_path_join(port_path, "link_layer");
            free(port_path);
            fp = state_path != NULL ? fopen(state_path, "r") : NULL;
            free(state_path);
            if (fp != NULL) {
                if (fgets(state, sizeof(state), fp) == NULL) {
                    state[0] = '\0';
                }
                fclose(fp);
            }
            fp = link_path != NULL ? fopen(link_path, "r") : NULL;
            free(link_path);
            if (fp != NULL) {
                if (fgets(link_layer, sizeof(link_layer), fp) == NULL) {
                    link_layer[0] = '\0';
                }
                fclose(fp);
            }
            state[strcspn(state, "\n")] = '\0';
            link_layer[strcspn(link_layer, "\n")] = '\0';
            fprintf(
                out,
                "RDMA NIC port: device=%s port=%s state=%s link_layer=%s\n",
                entry->d_name,
                sub->d_name,
                state[0] == '\0' ? "-" : strchr(state, ':') ? strchr(state, ':') + 1 : state,
                link_layer[0] == '\0' ? "-" : link_layer
            );
        }
        closedir(ports);
    }
    closedir(base);
}

void kvs_rdma_host_binding_report(const char *host, kvs_rdma_host_report *report) {
    struct addrinfo hints;
    struct addrinfo *result = NULL;
    struct ifaddrs *ifaddr = NULL;
    struct ifaddrs *ifa;
    DIR *base = NULL;
    struct dirent *entry;

    memset(report, 0, sizeof(*report));
    snprintf(report->host, sizeof(report->host), "%s", host);
    if (strcmp(host, "") == 0 || strcmp(host, "0.0.0.0") == 0 || strcmp(host, "::") == 0) {
        snprintf(report->note, sizeof(report->note), "wildcard host cannot prove RDMA netdev binding");
        return;
    }

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    if (getaddrinfo(host, NULL, &hints, &result) != 0 || result == NULL) {
        snprintf(report->error, sizeof(report->error), "resolve failed");
        return;
    }
    inet_ntop(AF_INET, &((struct sockaddr_in *) result->ai_addr)->sin_addr, report->resolved_ip, sizeof(report->resolved_ip));
    freeaddrinfo(result);
    if (strncmp(report->resolved_ip, "127.", 4) == 0) {
        snprintf(report->note, sizeof(report->note), "loopback host is not an RDMA NIC path");
        return;
    }
    if (getifaddrs(&ifaddr) != 0) {
        snprintf(report->note, sizeof(report->note), "interface enumeration failed");
        return;
    }

    base = opendir("/sys/class/infiniband");
    if (base == NULL) {
        freeifaddrs(ifaddr);
        snprintf(report->note, sizeof(report->note), "no active RDMA netdev owns this host IP");
        return;
    }

    while ((entry = readdir(base)) != NULL) {
        char net_path[1024];
        DIR *nets;
        struct dirent *net;
        if (entry->d_name[0] == '.' || !kvs_rdma_device_is_active(entry->d_name)) {
            continue;
        }
        snprintf(net_path, sizeof(net_path), "/sys/class/infiniband/%s/device/net", entry->d_name);
        nets = opendir(net_path);
        if (nets == NULL) {
            continue;
        }
        while ((net = readdir(nets)) != NULL) {
            if (net->d_name[0] == '.') {
                continue;
            }
            for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
                char ip[NI_MAXHOST];
                if (ifa->ifa_addr == NULL || ifa->ifa_addr->sa_family != AF_INET) {
                    continue;
                }
                if (strcmp(ifa->ifa_name, net->d_name) != 0) {
                    continue;
                }
                if (getnameinfo(ifa->ifa_addr, sizeof(struct sockaddr_in), ip, sizeof(ip), NULL, 0, NI_NUMERICHOST) != 0) {
                    continue;
                }
                if (strcmp(ip, report->resolved_ip) == 0 && report->match_count < sizeof(report->matches) / sizeof(report->matches[0])) {
                    kvs_rdma_match *match = &report->matches[report->match_count++];
                    snprintf(match->device, sizeof(match->device), "%s", entry->d_name);
                    snprintf(match->netdev, sizeof(match->netdev), "%s", net->d_name);
                    report->matched_rdma_netdev = true;
                }
            }
        }
        closedir(nets);
    }
    closedir(base);
    freeifaddrs(ifaddr);
    if (!report->matched_rdma_netdev && report->note[0] == '\0') {
        snprintf(report->note, sizeof(report->note), "no active RDMA netdev owns this host IP");
    }
}
