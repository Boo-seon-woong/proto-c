#include "kvs/tdx_runtime.h"

#include "kvs/common.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>

bool kvs_is_running_in_tdx_guest(void) {
    static const char *tdx_paths[] = {
        "/dev/tdx_guest",
        "/sys/class/misc/tdx_guest",
        "/sys/devices/virtual/misc/tdx_guest",
        "/sys/firmware/tdx",
    };
    FILE *fp;
    char buffer[4096];
    size_t i;

    for (i = 0; i < sizeof(tdx_paths) / sizeof(tdx_paths[0]); ++i) {
        if (access(tdx_paths[i], F_OK) == 0) {
            return true;
        }
    }

    fp = fopen("/proc/cpuinfo", "r");
    if (fp == NULL) {
        return false;
    }
    while (fgets(buffer, sizeof(buffer), fp) != NULL) {
        if (strstr(buffer, "tdx_guest") != NULL) {
            fclose(fp);
            return true;
        }
    }
    fclose(fp);
    return false;
}

int kvs_enforce_tdx_requirement(bool require_tdx, char *err, size_t err_len) {
    if (!require_tdx) {
        return 0;
    }
    if (kvs_is_running_in_tdx_guest()) {
        return 0;
    }
    kvs_errorf(
        err,
        err_len,
        "require_tdx=true but TDX guest environment was not detected. "
        "Run this process inside a TDX VM guest or set require_tdx=false."
    );
    return -1;
}
