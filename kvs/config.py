from __future__ import annotations

import json
import os
from typing import Any, Dict, Tuple

from kvs.cn_node import CNConfig
from kvs.mn_node import MNNodeConfig
from kvs.rpc import Endpoint


def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fp:
        data = json.load(fp)
    if not isinstance(data, dict):
        raise ValueError("config root must be an object")
    return data


def load_role_and_config(path: str) -> Tuple[str, CNConfig | MNNodeConfig]:
    raw = load_config(path)
    role = str(raw.get("role", "")).lower()
    root_require_tdx = bool(raw.get("require_tdx", False))
    config_dir = os.path.dirname(os.path.abspath(path))
    if role == "mn":
        mn_raw = raw.get("mn")
        if not isinstance(mn_raw, dict):
            raise ValueError("missing 'mn' object in config")
        return role, parse_mn_config(mn_raw, config_dir, root_require_tdx)
    if role == "cn":
        cn_raw = raw.get("cn")
        if not isinstance(cn_raw, dict):
            raise ValueError("missing 'cn' object in config")
        return role, parse_cn_config(cn_raw, root_require_tdx)
    raise ValueError("role must be either 'mn' or 'cn'")


def parse_mn_config(raw: Dict[str, Any], config_dir: str, root_require_tdx: bool) -> MNNodeConfig:
    state_dir_raw = str(raw["state_dir"])
    if os.path.isabs(state_dir_raw):
        state_dir = state_dir_raw
    else:
        state_dir = os.path.abspath(os.path.join(config_dir, state_dir_raw))

    rdma_listen_host = raw.get("rdma_listen_host")
    rdma_listen_port = raw.get("rdma_listen_port")

    return MNNodeConfig(
        node_id=str(raw["node_id"]),
        listen_host=str(raw["listen_host"]),
        listen_port=int(raw["listen_port"]),
        cache_capacity=int(raw["cache_capacity"]),
        state_dir=state_dir,
        require_tdx=bool(raw.get("require_tdx", root_require_tdx)),
        enable_rdma_server=bool(raw.get("enable_rdma_server", False)),
        rdma_listen_host=str(rdma_listen_host) if rdma_listen_host is not None else None,
        rdma_listen_port=int(rdma_listen_port) if rdma_listen_port is not None else None,
        require_rdma_server=bool(raw.get("require_rdma_server", False)),
    )


def parse_cn_config(raw: Dict[str, Any], root_require_tdx: bool) -> CNConfig:
    endpoints_raw = raw.get("mn_endpoints", [])
    if not isinstance(endpoints_raw, list):
        raise ValueError("cn.mn_endpoints must be an array")
    endpoints = [
        Endpoint(
            node_id=str(item["node_id"]),
            host=str(item["host"]),
            port=int(item["port"]),
            rdma_port=int(item["rdma_port"]) if item.get("rdma_port") is not None else None,
        )
        for item in endpoints_raw
    ]

    return CNConfig(
        client_id=str(raw["client_id"]),
        encryption_key_hex=str(raw["encryption_key_hex"]),
        replication_factor=int(raw.get("replication_factor", 1)),
        mn_endpoints=endpoints,
        populate_cache_on_read_miss=bool(raw.get("populate_cache_on_read_miss", True)),
        max_retries=int(raw.get("max_retries", 8)),
        require_tdx=bool(raw.get("require_tdx", root_require_tdx)),
        cache_path_transport=str(raw.get("cache_path_transport", "auto")),
        trace_operations=bool(raw.get("trace_operations", True)),
    )
