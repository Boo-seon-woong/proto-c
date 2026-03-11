from __future__ import annotations

import argparse
import json
import shlex
from typing import Any, Dict
from typing import Sequence

from kvs.cn_node import CNNode
from kvs.config import load_role_and_config
from kvs.mn_node import MNNode
from kvs.rdma_rpc import list_rdma_nics, rdma_host_binding_report, rdma_transport_profile
from kvs.tdx_runtime import enforce_tdx_requirement


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="kvs",
        description="TDX-compatible client-centric KVS prototype",
    )
    parser.add_argument(
        "--config",
        default="build/config.json",
        help="path to node config file (default: build/config.json)",
    )
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("serve", help="run MN server loop (MN role only)")

    write_parser = subparsers.add_parser("write", help="write key/value (CN role only)")
    write_parser.add_argument("key")
    write_parser.add_argument("value")

    update_parser = subparsers.add_parser("update", help="update key/value (CN role only)")
    update_parser.add_argument("key")
    update_parser.add_argument("value")

    read_parser = subparsers.add_parser("read", help="read key (CN role only)")
    read_parser.add_argument("key")

    delete_parser = subparsers.add_parser("delete", help="delete key (CN role only)")
    delete_parser.add_argument("key")

    subparsers.add_parser("state", help="print MN state from all endpoints (CN role only)")
    verify_parser = subparsers.add_parser("verify-rdma", help="verify RDMA runtime path (CN role only)")
    verify_parser.add_argument("--probe-key", default="__rdma_probe__", help="key used for non-mutating RDMA probe")
    subparsers.add_parser("repl", help="interactive CN shell")
    return parser


def run_repl(client: CNNode) -> None:
    print("CN REPL commands: write <k> <v>, update <k> <v>, read <k>, delete <k>, state, verify-rdma, quit")
    while True:
        try:
            line = input("cn> ").strip()
        except EOFError:
            print()
            return
        if not line:
            continue
        if line in {"quit", "exit"}:
            return

        parts = shlex.split(line)
        cmd = parts[0]
        try:
            if cmd == "write" and len(parts) >= 3:
                key = parts[1]
                value = " ".join(parts[2:])
                client.write(key, value)
                print("OK")
                continue
            if cmd == "update" and len(parts) >= 3:
                key = parts[1]
                value = " ".join(parts[2:])
                client.update(key, value)
                print("OK")
                continue
            if cmd == "read" and len(parts) == 2:
                value = client.read(parts[1])
                print("NOT_FOUND" if value is None else value)
                continue
            if cmd == "delete" and len(parts) == 2:
                client.delete(parts[1])
                print("OK")
                continue
            if cmd == "state" and len(parts) == 1:
                print(json.dumps(client.debug_cluster_state(), indent=2, sort_keys=True))
                continue
            if cmd == "verify-rdma" and len(parts) == 1:
                run_verify_rdma(client, "__rdma_probe__")
                continue
        except Exception as exc:
            print(f"ERROR: {exc}")
            continue

        print("Invalid command")


def _format_rdma_match(report: Dict[str, Any]) -> str:
    if report.get("matched_rdma_netdev", False):
        matches = report.get("matches", [])
        if not matches:
            return "matched_rdma_netdev=true"
        parts = [f"{item.get('device')}:{item.get('netdev')}" for item in matches]
        return f"matched_rdma_netdev=true matches={','.join(parts)}"
    note = report.get("note")
    if note:
        return f"matched_rdma_netdev=false note={note}"
    err = report.get("error")
    if err:
        return f"matched_rdma_netdev=false error={err}"
    return "matched_rdma_netdev=false"


def _print_runtime_rdma_profile() -> None:
    profile = rdma_transport_profile()
    print(
        "RDMA runtime profile: "
        f"supported={profile.get('supported')} "
        f"implementation={profile.get('implementation')} "
        f"one_sided={profile.get('one_sided')} "
        f"mn_cpu_bypass={profile.get('mn_cpu_bypass')}"
    )
    if profile.get("error"):
        print(f"RDMA runtime detail: {profile.get('error')}")
    if not profile.get("one_sided", False):
        print("RDMA strict note: current cache-path is two-sided rsocket RPC, so MN CPU participates.")


def _print_local_rdma_nics() -> None:
    devices = list_rdma_nics()
    if not devices:
        print("RDMA NIC inventory: none detected")
        return
    for dev in devices:
        print(
            "RDMA NIC: "
            f"device={dev.get('device')} "
            f"active={dev.get('active')} "
            f"netdevs={','.join(dev.get('netdevs', [])) or '-'}"
        )
        for port in dev.get("ports", []):
            print(
                "RDMA NIC port: "
                f"device={dev.get('device')} "
                f"port={port.get('port')} "
                f"state={port.get('state')} "
                f"link_layer={port.get('link_layer')}"
            )


def run_verify_rdma(client: CNNode, probe_key: str) -> int:
    _print_runtime_rdma_profile()
    _print_local_rdma_nics()

    all_cache_probes_rdma = True
    for endpoint in client.config.mn_endpoints:
        report = rdma_host_binding_report(endpoint.host)
        print(
            f"RDMA host binding: endpoint={endpoint.node_id} host={endpoint.host} "
            f"resolved={report.get('resolved_ip')} {_format_rdma_match(report)}"
        )
        try:
            outcome = client._cache_rpc_outcome(endpoint, "rdma_read_prime", {"key": probe_key})
            transport = outcome.transport
            if outcome.fallback_error:
                transport = f"{transport}(fallback-from-rdma)"
            print(
                f"RDMA probe: endpoint={endpoint.node_id} action=rdma_read_prime "
                f"transport={transport}"
            )
            if outcome.transport != "rdma":
                all_cache_probes_rdma = False
        except Exception as exc:
            all_cache_probes_rdma = False
            print(f"RDMA probe: endpoint={endpoint.node_id} action=rdma_read_prime error={exc}")

    print(
        "RDMA probe summary: "
        f"cache_probes_all_rdma={all_cache_probes_rdma} "
        "one_sided_cpu_bypass=false"
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    role, config = load_role_and_config(args.config)
    enforce_tdx_requirement(bool(getattr(config, "require_tdx", False)))

    if role == "mn":
        if args.command not in {None, "serve"}:
            raise SystemExit("MN role only supports: serve")
        node = MNNode(config)  # type: ignore[arg-type]
        print(f"MN node {node.config.node_id} control(TCP) listening on {node.config.listen_host}:{node.config.listen_port}")
        if node.config.enable_rdma_server:
            rdma_host = node.config.rdma_listen_host or node.config.listen_host
            rdma_port = node.config.rdma_listen_port or (node.config.listen_port + 100)
            print(f"MN node {node.config.node_id} cache-path(RDMA) listening on {rdma_host}:{rdma_port}")
            _print_runtime_rdma_profile()
            _print_local_rdma_nics()
            report = rdma_host_binding_report(rdma_host)
            print(
                f"MN RDMA bind-check: host={rdma_host} resolved={report.get('resolved_ip')} "
                f"{_format_rdma_match(report)}"
            )
        node.serve_forever()
        return 0

    client = CNNode(config)  # type: ignore[arg-type]
    command = args.command or "repl"
    if command == "write":
        client.write(args.key, args.value)
        print("OK")
        return 0
    if command == "update":
        client.update(args.key, args.value)
        print("OK")
        return 0
    if command == "read":
        value = client.read(args.key)
        print("NOT_FOUND" if value is None else value)
        return 0
    if command == "delete":
        client.delete(args.key)
        print("OK")
        return 0
    if command == "state":
        print(json.dumps(client.debug_cluster_state(), indent=2, sort_keys=True))
        return 0
    if command == "verify-rdma":
        return run_verify_rdma(client, args.probe_key)
    if command == "repl":
        run_repl(client)
        return 0

    raise SystemExit(f"unsupported command for CN role: {command}")


if __name__ == "__main__":
    raise SystemExit(main())
