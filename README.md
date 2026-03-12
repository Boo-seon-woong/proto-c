# TDX-Compatible Client-Centric KVS Prototype

이 저장소의 주 구현은 이제 Python 프로토타입이 아니라 C 기반 런타임입니다. JSON config 형식과 CN/MN 기능 계약은 유지하면서, 내부 구현은 보다 전형적인 RDMA 시스템 소프트웨어 구조로 재편했습니다.

핵심 변경점:

- `src/` + `include/` 기반의 명시적 C 모듈 구조
- CN/MN 상태를 동적 객체 대신 구조체, 해시 버킷, LRU, 슬롯/prime 포인터로 관리
- TCP control path와 rsocket 기반 RDMA cache path를 transport 계층으로 분리
- OpenSSL 기반 AEAD 암호화와 내장 JSON config/RPC 처리
- C 통합 테스트(`make test`) 제공

참고:

- 기존 `kvs/` Python 코드는 설계 참고용으로 남아 있습니다.
- 현재 RDMA 경로는 `librdmacm` `rsocket` 기반 two-sided RPC입니다. one-sided verbs data path처럼 MN CPU를 완전히 우회하지는 않습니다.

## Layout

- `src/cn/`: CN path, operation modules, snapshot consensus
- `src/mn/`: MN path
- `src/`: shared runtime modules
- `include/`: public headers
- `build/`: config examples
- `tests/`: C integration tests
- `kvs/`: legacy Python reference prototype

## Build

```bash
make
```

생성물:

- `bin/kvs`
- `bin/kvs-test`

필요 라이브러리:

- `openssl`
- `librdmacm`

참고:

- JSON 처리는 저장소 내부의 경량 `jansson` 호환 레이어로 빌드됩니다.

## Functional Scope

- Authoritative commit pointer: `CachePrimeTable[key] = (addr, epoch)`
- FUSEE-style write consensus: `backup CAS -> quorum -> primary commit pointer`
- Majority quorum rule: `Q = floor(R / 2) + 1`
- Fast-path write/update via cache slot allocation + prime CAS
- Cache miss 시 private fetch 후 cache promote
- Read snapshot rule: `prime1 -> slot -> prime2`
- Tombstone delete
- LRU eviction + private backing flush
- Replica fan-out (`hash(key) % N`) + backup rollback on aborted quorum attempts
- Private backing file 기반 lazy recovery

## Config

기존 JSON 스키마를 유지합니다.

- `role: "mn"`: MN daemon
- `role: "cn"`: CN CLI
- `require_tdx: true`: TDX guest 강제
- `mn.enable_rdma_server: true`: RDMA cache-path listener 활성화
- `cn.cache_path_transport: "tcp" | "auto" | "rdma"`
- `cn.print_operation_latency: true`: `write/update/read/delete` latency를 `stderr`로 출력

예시:

- `build/config.mn.example.json`
- `build/config.mn2.example.json`
- `build/config.cn.example.json`

## Run

majority quorum을 의미 있게 쓰려면 MN 3개 이상이 필요합니다. 현재 예제 파일은 `mn-1`, `mn-2`만 제공하므로, `build/config.mn2.example.json`을 복제해서 `mn-3`용 포트/ID/state_dir만 바꿔 추가하면 됩니다.

MN 실행:

```bash
bin/kvs --config build/config.mn.example.json serve
bin/kvs --config build/config.mn2.example.json serve
```

CN 명령 실행:

```bash
bin/kvs --config build/config.cn.example.json --latency write user:1 hello
bin/kvs --config build/config.cn.example.json write user:1 hello
bin/kvs --config build/config.cn.example.json update user:1 hello-v2
bin/kvs --config build/config.cn.example.json read user:1
bin/kvs --config build/config.cn.example.json delete user:1
bin/kvs --config build/config.cn.example.json state
bin/kvs --config build/config.cn.example.json verify-rdma
```

REPL:

```bash
bin/kvs --config build/config.cn.example.json repl
bin/kvs --config build/config.cn.example.json --latency repl
```

## Test

```bash
make test
```

현재 테스트 범위:

- write/read/update/delete
- eviction + private backing recovery
- quorum write with one backup down
- primary-unavailable write failure

## Architecture Notes

- `src/mn/node.c`: cache slots, prime table, eviction, private backing persistence
- `src/cn/node.c`: CN lifecycle, replica selection, cluster debug, RDMA verification
- `src/cn/cache_rpc.c`: cache-path transport dispatch
- `src/cn/commit.c`: FUSEE-style backup quorum, rollback, primary-last commit orchestration
- `src/cn/insert.c`: expected-null insert/promote prepare/CAS/rollback path
- `src/cn/write.c`: write command path
- `src/cn/update.c`: expected-present update prepare/CAS/rollback path
- `src/cn/read.c`: read command path
- `src/cn/snapshot.c`: snapshot consensus double-check path
- `src/cn/delete.c`: delete/tombstone path
- `src/rpc.c`: TCP control plane RPC
- `src/rdma_rpc.c`: rsocket RDMA cache-path RPC
- `src/crypto.c`: AES-GCM, fallback authenticated stream
