# Proto Directory Usage Guideline

이 문서는 `/home/seonung/2026/proto` 디렉터리의 KVS 프로토타입을 실제로 실행하고 실험할 때 필요한 최소 절차를 정리한 가이드입니다.

## 1) 목적과 구성

이 프로토타입은 `skill.md` 설계를 코드로 구현한 환경입니다.

- CN(Client Node): 클라이언트 로직, 암호화/복호화, CAS 기반 업데이트 수행
- MN(Memory Node): Prime Table, Cache Slot, Private Backing 관리
- Shared/Private 저장 데이터: 모두 ciphertext
- Authoritative pointer: `CachePrimeTable[key] = (addr, epoch)`

주요 경로:
- Fast path: cache hit 시 CN이 RDMA cache path로 prime/slot/CAS 처리
- Miss path: prime 미존재 시 TCP control path로 MN CPU private fetch 후 cache 재삽입
- Snapshot read: `prime1 -> slot -> prime2` 더블체크
- Eviction: prime 기반 victim 선택, private flush 후 prime 제거

주의: 현재 RDMA 경로는 `librdmacm`의 `rsocket` 기반 two-sided RPC입니다. 즉, one-sided RDMA처럼 MN CPU를 완전히 우회하지는 않습니다.

## 2) 디렉터리 맵

- `kvs/`: 실행 코드
- `build/`: 실행 config 샘플
- `tests/`: 통합/단위 테스트
- `README.md`: 요약 실행법
- `experiment.md`: 상세 실험 가이드(현재 문서)

## 3) 사전 확인

프로젝트 루트:

```bash
cd /home/seonung/2026/proto
```

Python 버전 확인:

```bash
python3 --version
```

기본 동작 점검:

```bash
python3 -m kvs --help
```

## 4) Config 원칙

역할 결정은 config 파일의 `role` 값으로 수행됩니다.

- `role: "mn"`: MN 서버 모드
- `role: "cn"`: CN CLI/REPL 모드

TDX 강제 실행:

- `require_tdx: true` 인 경우, TDX guest 환경이 아니면 시작 즉시 실패
- 현재 `build/config.mn.example.json`, `build/config.mn2.example.json`, `build/config.json`은 `require_tdx: true`
- 로컬 개발/디버깅 시에는 `require_tdx: false` 로 바꿔 실행

전송 경로 분리:

- MN `listen_*`: TCP control path
- MN `rdma_listen_*`: RDMA cache path
- CN endpoint `port`: TCP control path
- CN endpoint `rdma_port`: RDMA cache path
- CN `cache_path_transport`
  - `rdma`: cache path는 RDMA만 허용
  - `auto`: RDMA 실패 시 TCP fallback
- CN `trace_operations`
  - `true`: read/write/update/delete마다 cache hit/miss + transport(RDMA/TCP/fallback) trace 출력
  - `false`: trace 비활성화
- RDMA 설정 시 `rdma_listen_host`/`mn_endpoints.host`에 loopback(`127.0.0.1`) 대신 RDMA NIC의 실제 IP를 사용

## 5) 실행 시나리오 A (TDX 게스트 내부)

### 5.1 MN 실행

터미널 1:

```bash
python3 -m kvs --config build/config.mn.example.json serve
```

터미널 2:

```bash
python3 -m kvs --config build/config.mn2.example.json serve
```

방화벽에서 아래 포트를 열어야 합니다.
- TCP control path: 7001, 7002
- RDMA cache path: 7101, 7102

### 5.2 CN 명령 실행

터미널 3:

```bash
python3 -m kvs --config build/config.cn.example.json write user:1 hello
python3 -m kvs --config build/config.cn.example.json update user:1 hello-v2
python3 -m kvs --config build/config.cn.example.json read user:1
python3 -m kvs --config build/config.cn.example.json delete user:1
python3 -m kvs --config build/config.cn.example.json read user:1
```

예상 결과:
- 첫 read: `hello`
- delete 이후 read: `NOT_FOUND`

## 6) 실행 시나리오 B (로컬 개발/디버깅)

TDX 환경이 아니면 MN config에서 `require_tdx`를 `false`로 수정해야 합니다.

예시:

```json
{
  "role": "mn",
  "require_tdx": false,
  "mn": {
    "node_id": "mn-1",
    "listen_host": "127.0.0.1",
    "listen_port": 7001,
    "cache_capacity": 1024,
    "state_dir": "state/mn-1"
  }
}
```

동일하게 MN2도 `false`로 변경 후 실행합니다.

## 7) 상태 관찰 포인트

클러스터 상태 조회:

```bash
python3 -m kvs --config build/config.cn.example.json state
```

확인할 값:
- `prime_entries`: 현재 hot key 개수
- `cache_slots`: 현재 캐시 사용 슬롯 수
- `private_entries`: eviction 등으로 private backing에 존재하는 key 개수
- `prime_keys`: 현재 prime table에 살아있는 key 목록

RDMA 경로 점검:

```bash
python3 -m kvs --config build/config.cn.example.json verify-rdma
```

출력에서 확인할 핵심:
- endpoint별 `rdma_read_prime` probe transport가 `rdma`인지
- host IP가 active RDMA NIC(netdev)에 매핑되는지
- `one_sided=false`, `mn_cpu_bypass=false` (현재 구현 한계 명시)

## 8) 추천 실험 순서

1. 기본 쓰기/읽기/삭제
2. 반복 update로 CAS 경로 확인
3. cache_capacity를 작은 값으로 낮춰 eviction 유도
4. eviction 후 과거 key read로 private miss-recovery 경로 확인
5. `state` 결과로 prime/private/cacheslot 변화를 기록

## 9) 자동 검증

전체 테스트:

```bash
python3 -m unittest discover -s tests -v
```

검증 범위:
- write/read/update/delete
- eviction + private fetch recovery
- replication
- `require_tdx` 강제/해제 동작

## 10) 자주 발생하는 문제

### 문제: `require_tdx=true but TDX guest environment was not detected`

원인:
- TDX 게스트 외부에서 MN 실행 시도

해결:
- 실제 TDX VM guest에서 실행하거나
- 로컬 실험 목적이면 config의 `require_tdx`를 `false`로 변경

### 문제: CN read가 `NOT_FOUND`

점검:
- MN 2개가 실제로 실행 중인지
- CN의 `mn_endpoints` 주소/포트가 올바른지
- write가 먼저 성공했는지

## 11) 실험 기록 템플릿 (권장)

실험마다 아래 항목을 남기면 재현성이 올라갑니다.

- 날짜/시간
- 사용 config 파일 경로
- TDX 여부 (`require_tdx`, 실제 게스트 여부)
- 실행 명령
- 핵심 결과(`state`, read/write 결과, 오류 메시지)
