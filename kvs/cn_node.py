from __future__ import annotations

import hashlib
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from kvs.crypto import AEADCipher, key_from_hex
from kvs.models import CacheSlot, CipherRecord, PrimeEntry
from kvs.rdma_rpc import RDMAEndpoint, RDMAError, rdma_call
from kvs.rpc import Endpoint, rpc_call

TOMBSTONE_MARKER = b"__TDX_KVS_TOMBSTONE__"


@dataclass
class CNConfig:
    client_id: str
    encryption_key_hex: str
    replication_factor: int
    mn_endpoints: List[Endpoint]
    populate_cache_on_read_miss: bool = True
    max_retries: int = 8
    require_tdx: bool = False
    cache_path_transport: str = "auto"
    trace_operations: bool = True


@dataclass(frozen=True)
class CacheRPCOutcome:
    response: Dict[str, Any]
    transport: str
    fallback_error: Optional[str] = None


class CNNode:
    def __init__(self, config: CNConfig):
        if not config.mn_endpoints:
            raise ValueError("cn config must include at least one MN endpoint")
        if config.replication_factor <= 0:
            raise ValueError("replication_factor must be > 0")

        self.config = config
        self._cipher = AEADCipher(key_from_hex(config.encryption_key_hex))
        self._trace_enabled = bool(config.trace_operations)

    def _trace(self, message: str) -> None:
        if not self._trace_enabled:
            return
        print(f"[CN-TRACE][{self.config.client_id}] {message}", file=sys.stderr, flush=True)

    def _cache_rpc_call(self, endpoint: Endpoint, action: str, params: Dict[str, Any]) -> Dict[str, Any]:
        return self._cache_rpc_outcome(endpoint, action, params).response

    def _cache_rpc_outcome(self, endpoint: Endpoint, action: str, params: Dict[str, Any]) -> CacheRPCOutcome:
        mode = self.config.cache_path_transport.lower()
        if mode not in {"auto", "tcp", "rdma"}:
            raise ValueError("cache_path_transport must be one of: auto, tcp, rdma")

        should_try_rdma = mode == "rdma" or (mode == "auto" and endpoint.rdma_port is not None)
        if should_try_rdma:
            if endpoint.rdma_port is None:
                raise RuntimeError(f"{endpoint.node_id} has no rdma_port configured")
            rdma_endpoint = RDMAEndpoint(
                node_id=endpoint.node_id,
                host=endpoint.host,
                port=endpoint.rdma_port,
            )
            try:
                return CacheRPCOutcome(
                    response=rdma_call(rdma_endpoint, action, params),
                    transport="rdma",
                )
            except RDMAError as exc:
                if mode == "rdma":
                    raise
                return CacheRPCOutcome(
                    response=rpc_call(endpoint, action, params),
                    transport="tcp",
                    fallback_error=str(exc),
                )

        return CacheRPCOutcome(
            response=rpc_call(endpoint, action, params),
            transport="tcp",
        )

    def _transport_label(self, outcome: CacheRPCOutcome) -> str:
        if outcome.fallback_error:
            return f"tcp(fallback-from-rdma:{outcome.fallback_error})"
        return outcome.transport

    def _stable_hash(self, key: str) -> int:
        digest = hashlib.sha256(key.encode("utf-8")).digest()
        return int.from_bytes(digest[:8], "big")

    def _select_replicas(self, key: str) -> List[Endpoint]:
        endpoints = self.config.mn_endpoints
        replica_count = min(self.config.replication_factor, len(endpoints))
        start = self._stable_hash(key) % len(endpoints)
        return [endpoints[(start + i) % len(endpoints)] for i in range(replica_count)]

    def write(self, key: str, value: str) -> None:
        self._upsert(key, value, operation="write")

    def update(self, key: str, value: str) -> None:
        self._upsert(key, value, operation="update")

    def _upsert(self, key: str, value: str, operation: str) -> None:
        plaintext = value.encode("utf-8")
        record = self._cipher.encrypt(plaintext, aad=key.encode("utf-8"))
        record.tombstone = False
        replicas = self._select_replicas(key)
        replica_ids = ",".join(endpoint.node_id for endpoint in replicas)
        self._trace(f"op={operation} key={key} start replicas={replica_ids}")
        self._replicate_record(key, record, operation=operation, replicas=replicas)
        self._trace(f"op={operation} key={key} done")

    def delete(self, key: str) -> None:
        record = self._cipher.encrypt(TOMBSTONE_MARKER, aad=key.encode("utf-8"))
        record.tombstone = True
        replicas = self._select_replicas(key)
        replica_ids = ",".join(endpoint.node_id for endpoint in replicas)
        self._trace(f"op=delete key={key} start replicas={replica_ids}")
        self._replicate_record(key, record, operation="delete", replicas=replicas)
        self._trace(f"op=delete key={key} done")

    def _replicate_record(
        self,
        key: str,
        record: CipherRecord,
        operation: str,
        replicas: Optional[List[Endpoint]] = None,
    ) -> None:
        targets = replicas if replicas is not None else self._select_replicas(key)
        errors: List[str] = []
        for endpoint in targets:
            try:
                self._commit_to_replica(endpoint, key, record, operation=operation)
            except Exception as exc:
                errors.append(f"{endpoint.node_id}: {exc}")
        if errors:
            joined = "; ".join(errors)
            raise RuntimeError(f"replication failed on one or more replicas: {joined}")

    def _commit_to_replica(self, endpoint: Endpoint, key: str, record: CipherRecord, operation: str) -> None:
        for attempt in range(1, self.config.max_retries + 1):
            step_transports: List[str] = []
            prime_outcome = self._cache_rpc_outcome(endpoint, "rdma_read_prime", {"key": key})
            step_transports.append(prime_outcome.transport)
            prime_result = prime_outcome.response["result"]
            expected_addr: Optional[int] = None
            expected_epoch: Optional[int] = None
            private_addr: Optional[int] = None
            cache_hit = bool(prime_result.get("found"))

            if cache_hit:
                current = PrimeEntry.from_dict(prime_result["entry"])
                expected_addr = current.addr
                expected_epoch = current.epoch
                private_addr = current.private_addr
                self._trace(
                    f"op={operation} key={key} replica={endpoint.node_id} attempt={attempt} "
                    f"cache_lookup=hit transport={self._transport_label(prime_outcome)}"
                )
            else:
                private_result = rpc_call(endpoint, "cpu_fetch_private", {"key": key})["result"]
                if private_result.get("found"):
                    private_addr = int(private_result["private_addr"])
                self._trace(
                    f"op={operation} key={key} replica={endpoint.node_id} attempt={attempt} "
                    f"cache_lookup=miss transport={self._transport_label(prime_outcome)} "
                    f"private_lookup={'hit' if private_result.get('found') else 'miss'} transport=tcp"
                )

            alloc_outcome = self._cache_rpc_outcome(endpoint, "rdma_alloc_slot", {})
            step_transports.append(alloc_outcome.transport)
            alloc_result = alloc_outcome.response["result"]
            slot_id = int(alloc_result["slot_id"])
            slot_epoch = int(alloc_result["epoch"])

            write_outcome = self._cache_rpc_outcome(
                endpoint,
                "rdma_write_slot",
                {
                    "slot_id": slot_id,
                    "epoch": slot_epoch,
                    "record": record.to_dict(),
                },
            )
            step_transports.append(write_outcome.transport)
            write_result = write_outcome.response["result"]
            if not write_result.get("write_ok", False):
                self._trace(
                    f"op={operation} key={key} replica={endpoint.node_id} attempt={attempt} "
                    f"slot_write=retry-needed reason={write_result.get('reason', 'unknown')} "
                    f"transport={self._transport_label(write_outcome)}"
                )
                continue

            cas_outcome = self._cache_rpc_outcome(
                endpoint,
                "rdma_cas_prime",
                {
                    "key": key,
                    "expected_addr": expected_addr,
                    "expected_epoch": expected_epoch,
                    "new_addr": slot_id,
                    "new_epoch": slot_epoch,
                    "private_addr": private_addr,
                },
            )
            step_transports.append(cas_outcome.transport)
            cas_result = cas_outcome.response["result"]
            if cas_result.get("cas_ok", False):
                rdma_only = all(step_transport == "rdma" for step_transport in step_transports) and cache_hit
                if cache_hit:
                    path_summary = "cache-hit-rdma-only" if rdma_only else "cache-hit-mixed-transport"
                else:
                    path_summary = "cache-miss-with-private-cpu-fetch"
                self._trace(
                    f"op={operation} key={key} replica={endpoint.node_id} attempt={attempt} "
                    f"commit=ok summary={path_summary} "
                    f"alloc_transport={self._transport_label(alloc_outcome)} "
                    f"write_transport={self._transport_label(write_outcome)} "
                    f"cas_transport={self._transport_label(cas_outcome)}"
                )
                return
            self._trace(
                f"op={operation} key={key} replica={endpoint.node_id} attempt={attempt} "
                f"cas=retry-needed transport={self._transport_label(cas_outcome)}"
            )
        raise RuntimeError("max retry exceeded for CAS commit")

    def read(self, key: str) -> Optional[str]:
        primary = self._select_replicas(key)[0]
        self._trace(f"op=read key={key} primary={primary.node_id} start")
        for attempt in range(1, self.config.max_retries + 1):
            prime1_outcome = self._cache_rpc_outcome(primary, "rdma_read_prime", {"key": key})
            prime1_result = prime1_outcome.response["result"]

            if not prime1_result.get("found"):
                private_result = rpc_call(primary, "cpu_fetch_private", {"key": key})["result"]
                if not private_result.get("found"):
                    self._trace(
                        f"op=read key={key} primary={primary.node_id} attempt={attempt} "
                        f"cache_lookup=miss transport={self._transport_label(prime1_outcome)} "
                        f"private_lookup=miss transport=tcp result=not_found"
                    )
                    return None
                record = CipherRecord.from_dict(private_result["record"])
                private_addr = int(private_result["private_addr"])
                self._trace(
                    f"op=read key={key} primary={primary.node_id} attempt={attempt} "
                    f"cache_lookup=miss transport={self._transport_label(prime1_outcome)} "
                    f"private_lookup=hit transport=tcp"
                )
                if self.config.populate_cache_on_read_miss:
                    self._promote_private_to_cache(primary, key, record, private_addr)
                if record.tombstone:
                    self._trace(
                        f"op=read key={key} primary={primary.node_id} attempt={attempt} "
                        "result=cache-miss-tombstone"
                    )
                    return None
                plaintext = self._cipher.decrypt(record, aad=key.encode("utf-8"))
                if plaintext == TOMBSTONE_MARKER:
                    self._trace(
                        f"op=read key={key} primary={primary.node_id} attempt={attempt} "
                        "result=cache-miss-tombstone"
                    )
                    return None
                self._trace(
                    f"op=read key={key} primary={primary.node_id} attempt={attempt} "
                    "result=cache-miss-served-from-private transport=tcp"
                )
                return plaintext.decode("utf-8")

            prime1 = PrimeEntry.from_dict(prime1_result["entry"])
            slot_outcome = self._cache_rpc_outcome(primary, "rdma_read_slot", {"slot_id": prime1.addr})
            prime2_outcome = self._cache_rpc_outcome(primary, "rdma_read_prime", {"key": key})
            slot_result = slot_outcome.response["result"]
            prime2_result = prime2_outcome.response["result"]

            if not prime2_result.get("found"):
                self._trace(
                    f"op=read key={key} primary={primary.node_id} attempt={attempt} "
                    f"cache_lookup=hit transport={self._transport_label(prime1_outcome)} "
                    "snapshot=prime-disappeared retry=yes"
                )
                continue
            prime2 = PrimeEntry.from_dict(prime2_result["entry"])
            if prime1.addr != prime2.addr or prime1.epoch != prime2.epoch:
                self._trace(
                    f"op=read key={key} primary={primary.node_id} attempt={attempt} "
                    f"cache_lookup=hit transport={self._transport_label(prime1_outcome)} "
                    "snapshot=prime-changed retry=yes"
                )
                continue

            if not slot_result.get("found"):
                self._trace(
                    f"op=read key={key} primary={primary.node_id} attempt={attempt} "
                    f"cache_lookup=hit transport={self._transport_label(prime1_outcome)} "
                    "slot_lookup=miss retry=yes"
                )
                continue
            slot = CacheSlot.from_dict(slot_result["slot"])
            if slot.epoch != prime1.epoch:
                self._trace(
                    f"op=read key={key} primary={primary.node_id} attempt={attempt} "
                    f"cache_lookup=hit transport={self._transport_label(prime1_outcome)} "
                    "slot_epoch_mismatch retry=yes"
                )
                continue

            cache_transports = [prime1_outcome.transport, slot_outcome.transport, prime2_outcome.transport]
            rdma_only = all(step_transport == "rdma" for step_transport in cache_transports)
            if slot.record.tombstone:
                self._trace(
                    f"op=read key={key} primary={primary.node_id} attempt={attempt} "
                    "cache_lookup=hit result=tombstone "
                    f"prime_transport={self._transport_label(prime1_outcome)} "
                    f"slot_transport={self._transport_label(slot_outcome)} "
                    f"prime_recheck_transport={self._transport_label(prime2_outcome)} "
                    f"summary={'cache-hit-rdma-only' if rdma_only else 'cache-hit-mixed-transport'}"
                )
                return None
            plaintext = self._cipher.decrypt(slot.record, aad=key.encode("utf-8"))
            if plaintext == TOMBSTONE_MARKER:
                self._trace(
                    f"op=read key={key} primary={primary.node_id} attempt={attempt} "
                    "cache_lookup=hit result=tombstone "
                    f"prime_transport={self._transport_label(prime1_outcome)} "
                    f"slot_transport={self._transport_label(slot_outcome)} "
                    f"prime_recheck_transport={self._transport_label(prime2_outcome)} "
                    f"summary={'cache-hit-rdma-only' if rdma_only else 'cache-hit-mixed-transport'}"
                )
                return None
            self._trace(
                f"op=read key={key} primary={primary.node_id} attempt={attempt} "
                "cache_lookup=hit result=served-from-cache "
                f"prime_transport={self._transport_label(prime1_outcome)} "
                f"slot_transport={self._transport_label(slot_outcome)} "
                f"prime_recheck_transport={self._transport_label(prime2_outcome)} "
                f"summary={'cache-hit-rdma-only' if rdma_only else 'cache-hit-mixed-transport'}"
            )
            return plaintext.decode("utf-8")

        raise RuntimeError("snapshot read failed after max retries")

    def _promote_private_to_cache(
        self,
        endpoint: Endpoint,
        key: str,
        record: CipherRecord,
        private_addr: Optional[int],
    ) -> None:
        for attempt in range(1, self.config.max_retries + 1):
            alloc_outcome = self._cache_rpc_outcome(endpoint, "rdma_alloc_slot", {})
            alloc_result = alloc_outcome.response["result"]
            slot_id = int(alloc_result["slot_id"])
            slot_epoch = int(alloc_result["epoch"])

            write_outcome = self._cache_rpc_outcome(
                endpoint,
                "rdma_write_slot",
                {
                    "slot_id": slot_id,
                    "epoch": slot_epoch,
                    "record": record.to_dict(),
                },
            )
            write_result = write_outcome.response["result"]
            if not write_result.get("write_ok", False):
                self._trace(
                    f"op=read key={key} promote-cache replica={endpoint.node_id} attempt={attempt} "
                    f"slot_write=retry-needed reason={write_result.get('reason', 'unknown')} "
                    f"transport={self._transport_label(write_outcome)}"
                )
                continue

            cas_outcome = self._cache_rpc_outcome(
                endpoint,
                "rdma_cas_prime",
                {
                    "key": key,
                    "expected_addr": None,
                    "expected_epoch": None,
                    "new_addr": slot_id,
                    "new_epoch": slot_epoch,
                    "private_addr": private_addr,
                },
            )
            cas_result = cas_outcome.response["result"]
            if cas_result.get("cas_ok", False):
                self._trace(
                    f"op=read key={key} promote-cache replica={endpoint.node_id} attempt={attempt} "
                    f"result=ok alloc_transport={self._transport_label(alloc_outcome)} "
                    f"write_transport={self._transport_label(write_outcome)} "
                    f"cas_transport={self._transport_label(cas_outcome)}"
                )
                return
            current = cas_result.get("current")
            if current is not None:
                self._trace(
                    f"op=read key={key} promote-cache replica={endpoint.node_id} attempt={attempt} "
                    f"result=skipped-existing-prime cas_transport={self._transport_label(cas_outcome)}"
                )
                return

    def debug_cluster_state(self) -> Dict[str, Any]:
        states: Dict[str, Any] = {}
        for endpoint in self.config.mn_endpoints:
            states[endpoint.node_id] = rpc_call(endpoint, "debug_state")["result"]
        return states
