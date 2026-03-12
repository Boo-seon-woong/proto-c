#include "kvs/cn_node.h"
#include "kvs/config.h"
#include "kvs/mn_node.h"
#include "kvs/rdma_rpc.h"
#include "kvs/tdx_runtime.h"

#include <jansson.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <wordexp.h>

static volatile sig_atomic_t kvs_stop_requested = 0;

static void kvs_on_signal(int signo) {
    (void) signo;
    kvs_stop_requested = 1;
}

static void kvs_print_usage(FILE *out) {
    fprintf(out, "Usage: kvs [--config path] <command> [args]\n");
    fprintf(out, "Commands:\n");
    fprintf(out, "  serve\n");
    fprintf(out, "  write <key> <value>\n");
    fprintf(out, "  update <key> <value>\n");
    fprintf(out, "  read <key>\n");
    fprintf(out, "  delete <key>\n");
    fprintf(out, "  state\n");
    fprintf(out, "  verify-rdma [--probe-key key]\n");
    fprintf(out, "  repl\n");
}

static int kvs_run_repl(kvs_cn_node *node) {
    char *line = NULL;
    size_t line_cap = 0;
    ssize_t line_len;
    fprintf(stdout, "CN REPL commands: write <k> <v>, update <k> <v>, read <k>, delete <k>, state, verify-rdma, quit\n");
    while ((line_len = getline(&line, &line_cap, stdin)) >= 0) {
        wordexp_t words;
        char err[256];
        line[strcspn(line, "\n")] = '\0';
        if (line[0] == '\0') {
            continue;
        }
        if (strcmp(line, "quit") == 0 || strcmp(line, "exit") == 0) {
            break;
        }
        if (wordexp(line, &words, 0) != 0 || words.we_wordc == 0u) {
            fprintf(stdout, "Invalid command\n");
            if (words.we_wordv != NULL) {
                wordfree(&words);
            }
            continue;
        }
        if (strcmp(words.we_wordv[0], "write") == 0 && words.we_wordc >= 3u) {
            if (kvs_cn_write(node, words.we_wordv[1], words.we_wordv[2], err, sizeof(err)) != 0) {
                fprintf(stdout, "ERROR: %s\n", err);
            } else {
                fprintf(stdout, "OK\n");
            }
        } else if (strcmp(words.we_wordv[0], "update") == 0 && words.we_wordc >= 3u) {
            if (kvs_cn_update(node, words.we_wordv[1], words.we_wordv[2], err, sizeof(err)) != 0) {
                fprintf(stdout, "ERROR: %s\n", err);
            } else {
                fprintf(stdout, "OK\n");
            }
        } else if (strcmp(words.we_wordv[0], "read") == 0 && words.we_wordc == 2u) {
            char *value = NULL;
            bool found = false;
            if (kvs_cn_read(node, words.we_wordv[1], &value, &found, err, sizeof(err)) != 0) {
                fprintf(stdout, "ERROR: %s\n", err);
            } else {
                fprintf(stdout, "%s\n", found ? value : "NOT_FOUND");
            }
            free(value);
        } else if (strcmp(words.we_wordv[0], "delete") == 0 && words.we_wordc == 2u) {
            if (kvs_cn_delete(node, words.we_wordv[1], err, sizeof(err)) != 0) {
                fprintf(stdout, "ERROR: %s\n", err);
            } else {
                fprintf(stdout, "OK\n");
            }
        } else if (strcmp(words.we_wordv[0], "state") == 0 && words.we_wordc == 1u) {
            json_t *state = NULL;
            if (kvs_cn_debug_cluster_state(node, &state, err, sizeof(err)) != 0) {
                fprintf(stdout, "ERROR: %s\n", err);
            } else {
                json_dumpf(state, stdout, JSON_INDENT(2) | JSON_SORT_KEYS);
                fputc('\n', stdout);
            }
            json_decref(state);
        } else if (strcmp(words.we_wordv[0], "verify-rdma") == 0 && words.we_wordc == 1u) {
            if (kvs_cn_verify_rdma(node, "__rdma_probe__", stdout, err, sizeof(err)) != 0) {
                fprintf(stdout, "ERROR: %s\n", err);
            }
        } else {
            fprintf(stdout, "Invalid command\n");
        }
        wordfree(&words);
    }
    free(line);
    return 0;
}

int main(int argc, char **argv) {
    const char *config_path = "build/config.json";
    const char *command = NULL;
    char err[256];
    kvs_node_config config;
    int argi = 1;
    int rc = 1;

    memset(&config, 0, sizeof(config));
    while (argi < argc) {
        if (strcmp(argv[argi], "--config") == 0) {
            if (argi + 1 >= argc) {
                fprintf(stderr, "--config requires a path\n");
                return 1;
            }
            config_path = argv[argi + 1];
            argi += 2;
            continue;
        }
        command = argv[argi++];
        break;
    }

    if (kvs_load_config(config_path, &config, err, sizeof(err)) != 0) {
        fprintf(stderr, "%s\n", err);
        return 1;
    }
    if (kvs_enforce_tdx_requirement(
            config.role == KVS_ROLE_MN ? config.mn.require_tdx : config.cn.require_tdx,
            err,
            sizeof(err)
        ) != 0) {
        fprintf(stderr, "%s\n", err);
        kvs_free_config(&config);
        return 1;
    }

    if (config.role == KVS_ROLE_MN) {
        kvs_mn_node *node;
        kvs_rdma_host_report report;
        const char *rdma_host;
        kvs_rdma_profile profile;
        if (command != NULL && strcmp(command, "serve") != 0) {
            fprintf(stderr, "MN role only supports: serve\n");
            kvs_free_config(&config);
            return 1;
        }
        node = kvs_mn_node_create(&config.mn, err, sizeof(err));
        if (node == NULL) {
            fprintf(stderr, "%s\n", err);
            kvs_free_config(&config);
            return 1;
        }
        if (kvs_mn_node_start(node, err, sizeof(err)) != 0) {
            fprintf(stderr, "%s\n", err);
            kvs_mn_node_destroy(node);
            kvs_free_config(&config);
            return 1;
        }
        fprintf(stdout, "MN node %s control(TCP) listening on %s:%u\n", config.mn.node_id, config.mn.listen_host, kvs_mn_node_tcp_port(node));
        if (config.mn.enable_rdma_server) {
            rdma_host = config.mn.rdma_listen_host != NULL ? config.mn.rdma_listen_host : config.mn.listen_host;
            fprintf(stdout, "MN node %s cache-path(RDMA) listening on %s:%u\n", config.mn.node_id, rdma_host, kvs_mn_node_rdma_port(node));
            kvs_rdma_transport_profile(&profile);
            fprintf(
                stdout,
                "RDMA runtime profile: supported=%s implementation=%s one_sided=%s mn_cpu_bypass=%s\n",
                profile.supported ? "true" : "false",
                profile.implementation,
                profile.one_sided ? "true" : "false",
                profile.mn_cpu_bypass ? "true" : "false"
            );
            kvs_rdma_print_local_nics(stdout);
            kvs_rdma_host_binding_report(rdma_host, &report);
            fprintf(
                stdout,
                "MN RDMA bind-check: host=%s resolved=%s %s\n",
                rdma_host,
                report.resolved_ip[0] != '\0' ? report.resolved_ip : "-",
                report.matched_rdma_netdev ? "matched_rdma_netdev=true" :
                (report.note[0] != '\0' ? report.note : "matched_rdma_netdev=false")
            );
        }
        signal(SIGINT, kvs_on_signal);
        signal(SIGTERM, kvs_on_signal);
        while (!kvs_stop_requested) {
            pause();
        }
        kvs_mn_node_destroy(node);
        kvs_free_config(&config);
        return 0;
    }

    {
        kvs_cn_node *node = kvs_cn_node_create(&config.cn, err, sizeof(err));
        if (node == NULL) {
            fprintf(stderr, "%s\n", err);
            kvs_free_config(&config);
            return 1;
        }
        if (command == NULL) {
            rc = kvs_run_repl(node);
        } else if (strcmp(command, "write") == 0 && argi + 1 < argc) {
            rc = kvs_cn_write(node, argv[argi], argv[argi + 1], err, sizeof(err));
            if (rc == 0) {
                puts("OK");
            }
        } else if (strcmp(command, "update") == 0 && argi + 1 < argc) {
            rc = kvs_cn_update(node, argv[argi], argv[argi + 1], err, sizeof(err));
            if (rc == 0) {
                puts("OK");
            }
        } else if (strcmp(command, "read") == 0 && argi < argc) {
            char *value = NULL;
            bool found = false;
            rc = kvs_cn_read(node, argv[argi], &value, &found, err, sizeof(err));
            if (rc == 0) {
                puts(found ? value : "NOT_FOUND");
            }
            free(value);
        } else if (strcmp(command, "delete") == 0 && argi < argc) {
            rc = kvs_cn_delete(node, argv[argi], err, sizeof(err));
            if (rc == 0) {
                puts("OK");
            }
        } else if (strcmp(command, "state") == 0) {
            json_t *state = NULL;
            rc = kvs_cn_debug_cluster_state(node, &state, err, sizeof(err));
            if (rc == 0) {
                json_dumpf(state, stdout, JSON_INDENT(2) | JSON_SORT_KEYS);
                fputc('\n', stdout);
            }
            json_decref(state);
        } else if (strcmp(command, "verify-rdma") == 0) {
            const char *probe_key = "__rdma_probe__";
            if (argi + 1 < argc && strcmp(argv[argi], "--probe-key") == 0) {
                probe_key = argv[argi + 1];
            }
            rc = kvs_cn_verify_rdma(node, probe_key, stdout, err, sizeof(err));
        } else if (strcmp(command, "repl") == 0) {
            rc = kvs_run_repl(node);
        } else {
            kvs_print_usage(stderr);
            rc = 1;
        }
        if (rc != 0 && err[0] != '\0') {
            fprintf(stderr, "%s\n", err);
        }
        kvs_cn_node_destroy(node);
    }

    kvs_free_config(&config);
    return rc == 0 ? 0 : 1;
}
