# TDX-Compatible Client-Centric KVS Prototype

`skill.md` 설계를 기준으로 CN/MN 역할 분리, cache-prime 기반 CAS 합의, ciphertext-only shared/private 메모리 모델을 구현한 프로토타입입니다.
현재는 cache path를 RDMA(`librdmacm` rsocket)로, private miss 경로를 TCP RPC로 분리할 수 있습니다.
현재 RDMA 구현은 `rsocket` 기반 two-sided RPC이므로, one-sided RDMA처럼 MN CPU를 완전히 우회하지는 않습니다.

## 구현 범위

- Authoritative commit pointer: `CachePrimeTable[key] = (addr, epoch)`
- Fast path write/update (cache hit): CN이 RDMA cache path에서 slot 할당/암호화/CAS 수행
- Cache miss 경로: CN이 MN CPU private fetch 호출 후 cache slot에 재삽입
- Read snapshot rule: `prime1 -> slot -> prime2` 더블체크 불일치 시 retry
- Eviction: PrimeEntry 기준 victim 선택, ciphertext를 private backing에 flush 후 prime 제거
- Delete: tombstone write + CAS, tombstone read는 `NOT_FOUND`
- Replication: `hash(key) % N` 기반 replica 선택 후 각 replica PrimeEntry CAS
- Recovery: private backing 파일 기반 cold key 유지, cache/prime는 lazy rebuild

## 디렉터리

- `kvs/`: 구현 코드
- `build/`: 역할 선택용 설정 파일 및 예시
- `tests/`: 통합 테스트

## 역할 선택 방식

노드 역할은 config 파일의 `role` 값으로 결정됩니다.

- `role: "mn"` -> Memory Node 서버 실행
- `role: "cn"` -> Client Node CLI 실행
- `require_tdx: true` -> TDX guest 환경이 아니면 시작 즉시 실패
- `mn.enable_rdma_server: true` -> MN에서 RDMA cache-path 서버 활성화
- `cn.cache_path_transport: "rdma"` -> CN에서 cache 연산을 RDMA로 강제
- `cn.trace_operations: true` -> read/write/update/delete 시 cache hit/miss + RDMA/TCP 경로를 터미널에 출력

기본 config 경로는 `build/config.json` 입니다.

## Config 예시

- `build/config.mn.example.json`
- `build/config.mn2.example.json`
- `build/config.cn.example.json`

필요하면 예시 파일을 복사해 실제 실행 config로 사용하면 됩니다.

## 실행 예시

`build/config.mn.example.json`과 `build/config.mn2.example.json`은 `require_tdx: true`로 설정되어 있습니다.
TDX VM 게스트 외부에서 테스트할 때는 해당 값을 `false`로 바꿔야 서버가 시작됩니다.

### 1) MN 2개 실행

```bash
python3 -m kvs --config build/config.mn.example.json serve
python3 -m kvs --config build/config.mn2.example.json serve
```

네트워크/방화벽:
- TCP control plane: `7001`, `7002`
- RDMA cache path: `7101`, `7102`
- CN에서 위 포트로 모두 접근 가능해야 합니다.

### 2) CN로 write/update/read/delete

```bash
python3 -m kvs --config build/config.cn.example.json write user:1 hello
python3 -m kvs --config build/config.cn.example.json update user:1 hello-v2
python3 -m kvs --config build/config.cn.example.json read user:1
python3 -m kvs --config build/config.cn.example.json delete user:1
python3 -m kvs --config build/config.cn.example.json read user:1
```

### 3) 상태 확인

```bash
python3 -m kvs --config build/config.cn.example.json state
```

### 4) REPL

```bash
python3 -m kvs --config build/config.cn.example.json repl
```

### 5) RDMA 경로 점검

```bash
python3 -m kvs --config build/config.cn.example.json verify-rdma
```

출력 항목:
- RDMA runtime profile (`supported`, `implementation`, `one_sided`, `mn_cpu_bypass`)
- 로컬 RDMA NIC/포트 상태
- endpoint별 `rdma_read_prime` probe가 RDMA인지, fallback(TCP)인지

## 테스트

```bash
python3 -m unittest discover -s tests -v
```

## 암호화 구현

- `cryptography` 패키지가 있으면 AES-GCM 사용
- 없으면 표준 라이브러리 기반 authenticated stream fallback(`hmac-stream-v1`) 사용

Shared memory와 private backing 모두 ciphertext만 저장하며, CN만 복호화 키를 가집니다.

## RDMA + TCP 분리 설정 요약

- MN
  - `listen_host/listen_port`: TCP control plane (`cpu_fetch_private` 등)
  - `rdma_listen_host/rdma_listen_port`: RDMA cache path (`rdma_*` 액션)
- CN endpoint
  - `port`: TCP control plane 포트
  - `rdma_port`: RDMA cache path 포트
- CN 정책
  - `cache_path_transport: "rdma"`: RDMA 실패 시 즉시 에러
  - `cache_path_transport: "auto"`: RDMA 실패 시 TCP fallback
  - `trace_operations: true`: CRUD 요청마다 hit/miss + transport trace 출력

주의:
- RDMA cache path는 일반적으로 loopback(`127.0.0.1`)에서 동작하지 않습니다.
- `rdma_listen_host`/`mn_endpoints.host`는 RDMA NIC가 붙은 실제 인터페이스 IP를 사용해야 합니다.
- `verify-rdma`가 RDMA probe 성공을 보여도, 현재 구현은 one-sided RDMA CPU-bypass가 아니라 two-sided RPC 모델입니다.
