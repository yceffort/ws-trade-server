# M2: 체결 발생기를 별도 프로세스로 떼고 공유 메모리로 넘기기

## 목표

지금은 웹소켓 서버 프로세스 안에서 가짜 체결을 만든다. 이것을 두 프로세스로 나눈다.

- **feed**: 체결을 만들어 공유 메모리 링 버퍼에 쓴다. 메시지 형식은 protobuf(`proto/feed.proto`의 `Trade`).
- **gateway**: 링 버퍼를 읽어 JSON 본문으로 바꾸고 hub에 발행한다. 클라이언트와 주고받는 프레임은 M1과 같다.

증권사의 시세 수신 프로세스와 웹소켓 서버가 공유 메모리로 데이터를 주고받는다는 흔한 구조를 가장 작게 세워 보는 것이다. 블로그 글에 인용되므로 짧고 읽기 쉬워야 하고, `unsafe`는 왜 안전한지 글로 설명할 수 있어야 한다.

## 먼저 읽을 것

- `AGENTS.md`: 저장소 규칙과 검증 명령
- `.agents/skills/rust-skills/SKILL.md`와, 그중 `unsafe-`, `conc-`, `async-`, `err-` 범주에서 이 작업에 해당하는 규칙 파일. 특히 `unsafe-safety-comment`, `unsafe-minimize-scope`, `conc-atomic-ordering`, `async-spawn-blocking`
- `proto/feed.proto`: 내부 메시지. 수정하지 않는다
- `contract/wts-stomp.json`: 클라이언트 프레임의 기준. M1과 같은 역할이다
- 현재 `src/`와 `tests/` 전체

## 링 버퍼 (`src/ring.rs`)

쓰는 쪽이 하나이고 읽는 쪽이 여럿인 브로드캐스트 링이다. 쓰는 쪽은 읽는 쪽을 기다리지 않는다. 느린 읽는 쪽은 덮어쓰여서 데이터를 잃고, 그 사실을 스스로 알아챈다. 읽는 쪽마다 자기 위치를 따로 들고 있으므로 gateway를 여러 개 띄워도 서로 영향이 없다.

### 파일 배치

파일 하나를 두 프로세스가 `mmap`으로 공유한다. 모든 정수는 리틀 엔디언이다.

| 오프셋 | 크기 | 내용 |
| --- | --- | --- |
| 0 | 8 | magic. 상수 하나를 정해 쓴다 |
| 8 | 8 | capacity. 데이터 영역의 바이트 수, 2의 거듭제곱 |
| 16 | 8 | `tail_intent` (`AtomicU64`). 쓰는 쪽이 "여기까지 덮어쓸 것"이라고 먼저 알리는 위치 |
| 24 | 8 | `tail` (`AtomicU64`). 여기까지는 다 썼다는 위치 |
| 32 | 32 | 예약(0) |
| 64 | capacity | 데이터 영역 |

`tail`과 `tail_intent`는 지금까지 쓴 총 바이트 수이고 줄어들지 않는다. 데이터 영역 안의 실제 자리는 `위치 % capacity`다.

### 레코드

`[len: u32][payload: len 바이트][8바이트 경계까지 패딩]`. 레코드는 데이터 영역의 끝을 넘어 이어지지 않는다. 끝까지 남은 공간이 레코드보다 작으면 그 자리에 `len = u32::MAX`인 건너뛰기 표시를 쓰고 영역의 처음부터 쓴다. payload는 최대 4096바이트이고 넘으면 오류다.

### 데이터 영역에 접근하는 방법

데이터 영역 전체를 `&[AtomicU8]` 한 장으로 보고, 바이트마다 `Relaxed`로 읽고 쓴다. 읽는 쪽이 복사하는 순간에 쓰는 쪽이 같은 자리를 덮어쓸 수 있는데, 원자 타입이 아닌 메모리를 그렇게 동시에 읽고 쓰면 Rust에서는 데이터 경쟁이라 정의되지 않은 동작이다. 원자 바이트로 접근하면 정의된 동작이 되고, 아래의 fence가 덮어쓰기를 알아채는 근거가 된다.

### 쓰는 쪽

레코드 하나(건너뛰기 표시 포함)마다 다음 순서를 지킨다.

1. `tail_intent`에 `max(지금 저장된 tail_intent, tail + 크기)`를 `Relaxed`로 저장하고, 바로 `fence(Release)`를 둔다. Release 저장만으로는 뒤따르는 바이트 쓰기가 이 저장보다 먼저 보이는 것을 막지 못한다. `max`를 쓰는 이유는 쓰는 쪽이 1과 3 사이에서 죽었다가 다시 떴을 때 `tail_intent`가 줄어드는 것을 막기 위해서다.
2. 바이트를 쓴다(`Relaxed`).
3. `tail`에 `tail + 크기`를 저장한다(`Release`).

### 읽는 쪽

자기 위치 `pos`를 들고 있다. 새로 연 읽는 쪽은 그 시점의 `tail`에서 시작한다(이전 데이터는 읽지 않는다). 읽기 전용 매핑에서 보장되는 원자 연산은 `Relaxed` load뿐이므로, 순서 보장이 필요한 곳은 `Relaxed` load와 `fence(Acquire)`를 짝지어 쓴다.

1. `tail`을 `Relaxed`로 읽고 `fence(Acquire)`를 둔다. `pos == tail`이면 새 데이터가 없다.
2. `pos` 자리의 `len`을 복사한다. 건너뛰기 표시가 아닌데 4096보다 크거나, `자리 + 4 + len`이 데이터 영역의 끝을 넘으면 덮어쓰인 값이므로 payload를 읽지 않고 4의 유실 처리로 간다.
3. payload를 **자기 버퍼로 복사**한다.
4. `fence(Acquire)`를 두고 `tail_intent`를 `Relaxed`로 읽는다. `tail_intent - pos > capacity`이면 복사하는 동안 쓰는 쪽이 그 자리를 덮어썼을 수 있으므로 복사본을 버리고, `pos`를 현재 `tail`로 옮기고, 유실을 알린다. 아니면 복사본은 온전하고 `pos`를 레코드 크기만큼 옮긴다.

읽기 함수의 결과는 "레코드 하나", "새 데이터 없음", "유실됨" 세 가지를 구분할 수 있어야 한다. 쓰는 쪽이 재시작해서 `tail < pos`가 되는 경우는 없다(아래 "파일 만들기" 참고).

### 파일 만들기와 다시 열기

- capacity는 8192 이상이어야 한다(가장 큰 레코드가 4104바이트다).
- 쓰는 쪽은 파일이 없으면 만들고 헤더를 채우되 magic을 맨 마지막에 쓴다. 읽는 쪽이 반쯤 채워진 헤더를 믿지 않게 하기 위해서다. 파일이 있으면 magic과 capacity를 검증하고 **기존 `tail`에서 이어 쓴다.** 이미 있는 파일을 truncate 하지 않는다. 다른 프로세스가 매핑한 파일을 줄이면 그 프로세스가 SIGBUS로 죽기 때문이다. capacity가 다르면 오류로 끝낸다.
- 쓰는 쪽은 파일에 배타적 잠금(`File::try_lock`)을 잡고 사는 동안 유지한다. 이미 잠겨 있으면 오류로 끝낸다. 쓰는 쪽이 둘이면 링이 깨진다.
- 읽는 쪽은 파일을 읽기 전용으로 매핑하고 magic을 검증한다. 파일이 없거나 magic이 아직 없으면 오류를 돌려준다(재시도는 호출하는 쪽이 한다).

### unsafe 규칙

- `unsafe`는 `src/ring.rs`에만 둔다. `Cargo.toml`에 `[lints.rust] unsafe_code = "deny"`를 넣고 `ring.rs`에서만 허용한다.
- 모든 `unsafe` 블록 위에 `// SAFETY:` 주석으로 왜 안전한지 적는다(한국어).
- 데이터 영역에는 `&[u8]`나 `&mut [u8]` 참조를 만들지 않고 `ptr::copy_nonoverlapping`도 쓰지 않는다. 위에 적은 대로 `&[AtomicU8]`로만 접근하고, 검증이 끝난 복사본만 밖으로 내보낸다. `unsafe`가 필요한 곳은 매핑한 포인터로 이 원자 뷰와 헤더의 `&AtomicU64` 둘을 만드는 자리뿐이어야 한다.
- `Send`나 `Sync`를 직접 구현해야 하면 근거를 주석으로 적는다.
- 매핑은 `memmap2` 크레이트를 쓴다.

### Miri로 검사할 수 있게

Miri는 파일 `mmap`을 지원하지 않는다. 그래서 읽기와 쓰기 로직은 "헤더의 원자 값 둘과 `&[AtomicU8]` 데이터 뷰"에만 의존하게 하고, 그 뷰를 만드는 방법을 둘 둔다.

- 파일 매핑: 실제로 쓰는 방법
- 힙 메모리: `ring::in_memory(capacity)`가 같은 영역을 공유하는 쓰는 쪽 하나와 읽는 쪽 하나를 돌려준다. 테스트와 Miri 검사용이며 문서 주석에 그렇게 적는다. 파일 매핑과 같은 포인터 변환 코드를 거쳐야 한다(정렬, 길이 계산이 같은 경로로 검사되도록).

`tests/ring_miri.rs`에 힙 영역으로 도는 테스트를 둔다: 순서대로 읽기, 여러 바퀴 돌기, 유실 감지, 그리고 스레드 둘의 부하 테스트(무늬 검사는 `tests/ring.rs`의 5번과 같고, Miri가 느리므로 레코드 2000건). 이 파일은 일반 `cargo test`에서도 통과해야 한다.

nightly 툴체인과 Miri는 설치돼 있다. 아래 명령 하나만 실행해도 된다. 그 밖의 `cargo +<toolchain>` 명령은 여전히 금지다.

```sh
cargo +nightly miri test --test ring_miri
```

## protobuf

- `prost`로 `proto/feed.proto`를 빌드한다. 이 맥에는 `protoc`가 없고 설치하지 않는다. `build.rs`에서 `protox`(순수 Rust 컴파일러)로 디스크립터를 만들어 `prost-build`에 넘기는 방식을 우선 시도한다. 안 되면 멈춰서 보고한다.
- 링 레코드의 payload는 `Trade`를 인코딩한 바이트다.

## feed 프로세스 (`src/bin/feed.rs`, `src/feed.rs`)

- 기존 발생기를 `Trade`(proto)를 만들도록 바꾼다. 가격과 금액은 `Decimal`(정수와 자릿수)로 담는다. `f64`를 쓰지 않는다.
- `seq`는 1부터 종목 구분 없이 1씩 늘린다.
- 종목마다 평균 초당 N건을 불규칙한 간격으로 만들어 링에 쓴다. 쓰는 쪽은 프로세스 안에서 하나여야 하므로 태스크 여럿이 직접 쓰지 않게 한다.
- 실행: `cargo run --bin feed -- --ring <경로> --symbols kr:A005930,kr:A000660,us:US19990122001 --rate 5 --capacity 1048576`. 인자는 전부 선택이고 이 값이 기본값이다. `--ring`의 기본값은 `std::env::temp_dir()` 아래 `ws-trade-feed.ring`.

## gateway 프로세스 (`src/bin/gateway.rs`)

- 기존 `src/main.rs`를 옮긴다. 인자: `--addr`(기본 `127.0.0.1:8080`), `--ring`(기본값은 feed와 같음), `--poll-us`(기본 200).
- **브리지**: 전용 OS 스레드 하나가 링을 읽는다. 새 데이터가 없으면 `poll-us` 마이크로초 잔다. 레코드를 받으면 `Trade`로 디코딩하고, JSON 본문과 topic을 만들어 hub에 발행한다. 비동기 런타임 밖의 스레드이므로 블로킹 전송을 쓴다.
- 링 파일이 아직 없으면 0.5초마다 다시 열어 본다. feed와 gateway를 어느 순서로 띄워도 된다.
- 유실은 `Trade.seq`로 센다. 직전에 받은 `seq`보다 2 이상 크면 그 차이만큼 유실로 `eprintln!` 한다. 링이 유실을 알렸을 때도 한 줄 남긴다. `seq`가 직전보다 작아지면 유실이 아니라 feed가 다시 뜬 것으로 보고 한 줄 남긴다.
- hub의 명령 채널이 가득 차면 브리지가 막히고, 그동안 링에서 뒤처져 유실이 생긴다. 이 동작은 의도한 것이다. 따로 버리는 로직을 넣지 않는다.

## proto에서 JSON으로 (`src/trade.rs`)

클라이언트로 나가는 본문의 키 순서와 타입은 계약의 `server.bodies` 그대로여야 한다(M1과 같음).

| JSON 키 | 출처 |
| --- | --- |
| `code`, `currency`, `volume`, `cumulativeVolume` | 같은 이름의 필드 |
| `dt` | `traded_at_ms`를 UTC 초 단위 `2026-01-02T03:04:05Z` 형식으로 |
| `exchange` | `EXCHANGE_KRX`는 `"krx"`, `EXCHANGE_NXT`는 `"nxt"`. 국내만 |
| `session` | `SESSION_REGULAR`는 `"REGULAR"`, `SESSION_DAY`는 `"DAY"` |
| `base`, `close` | `base_price`, `price` |
| `tradeType` | `SIDE_BUY`는 `"BUY"`, `SIDE_SELL`은 `"SELL"` |
| `changeType` | `CHANGE_TYPE_UP`은 `"UP"`, `CHANGE_TYPE_DOWN`은 `"DOWN"`, `CHANGE_TYPE_UPPER_LIMIT`은 `"UPPER_LIMIT"` |
| `tradingStrength`, `cumulativeAmount` | 같은 이름의 필드 |
| `baseKrw`, `closeKrw`, `cumulativeAmountKrw` | `krw.base_price`, `krw.price`, `krw.cumulative_amount`. 미국만 |

- `Decimal`은 JSON 숫자로 내보낸다. `scale`이 0이면 정수로, 아니면 소수점 아래를 정확히 `scale` 자리로 쓴다(`units` 15096, `scale` 2는 `150.96`). **`f64`를 거치지 않고 자릿수를 그대로 쓴다.**
- 계약상 정수인 필드(국내 `base`, `close`, 양쪽의 `cumulativeAmount`)는 feed가 `scale` 0으로, 소수인 필드는 `scale` 1 이상으로 만든다.
- 필수 필드가 비었거나 enum이 UNSPECIFIED여서 본문을 만들 수 없는 메시지는 버리고 한 줄 남긴다.
- topic은 `/topic/v1/{kr|us}/stock/trade/{code}`.

## 파일 구성

패키지 하나에 라이브러리와 실행 파일 둘.

- `build.rs`: proto 빌드
- `src/lib.rs`: 모듈 선언, `Config`, `start`. `Config`에서 `symbols`와 `rate`를 빼고 링 경로(선택)와 poll 간격을 넣는다. 링 경로가 없으면 브리지를 띄우지 않는다(기존 테스트는 hub에 직접 발행한다)
- `src/ring.rs`: 링 버퍼
- `src/pb.rs`: 생성된 proto 코드를 포함
- `src/feed.rs`: 발생기
- `src/trade.rs`: proto에서 JSON 본문 만들기
- `src/bridge.rs`: 링에서 hub로 넘기는 스레드
- `src/bin/feed.rs`, `src/bin/gateway.rs`: 인자 파싱과 실행
- `src/stomp.rs`, `src/hub.rs`, `src/connection.rs`: 꼭 필요한 만큼만 바꾼다
- `tests/ring.rs`, `tests/feed_to_client.rs`: 새 테스트. 기존 `tests/contract.rs`는 유지

파일 하나가 250줄을 넘어가면 멈추고 보고한다.

## 테스트 (필수)

`tests/ring.rs` (임시 파일을 쓰고 끝나면 지운다. 쓰는 쪽과 읽는 쪽이 같은 파일을 각자 매핑한다)

1. 쓴 순서대로, 쓴 내용 그대로 읽힌다.
2. 작은 capacity에서 읽는 쪽이 따라가며 여러 바퀴를 돌아도 순서와 내용이 온전하다(건너뛰기 표시 경로 포함).
3. 읽는 쪽이 읽지 않는 동안 capacity보다 많이 쓰면 읽는 쪽이 유실을 알리고, 그 뒤의 레코드는 다시 온전하게 읽힌다.
4. 읽는 쪽 둘이 서로 다른 속도로 읽어도 각자 온전하게 받는다.
5. 스레드 둘의 부하 테스트: 쓰는 스레드가 순번과 그 순번에서 계산한 무늬로 채운 레코드를 20만 건 쓰고, 읽는 스레드는 받은 레코드마다 무늬가 순번과 맞는지, 순번이 늘기만 하는지 확인한다. 작은 capacity로 덮어쓰기를 일부러 일으킨다. 유실은 허용하되 유실 알림 없이 순번이 건너뛰면 실패, 무늬가 깨진 레코드가 한 건이라도 나오면 실패다.
6. 이미 있는 링 파일을 쓰는 쪽이 다시 열면 `tail`이 이어지고, 열려 있던 읽는 쪽이 계속 읽는다.
7. 쓰는 쪽을 둘 열면 둘째가 오류를 낸다. capacity가 다른 파일을 열면 오류를 낸다. 4096바이트를 넘는 payload는 오류다.

`tests/feed_to_client.rs`

8. `feed` 실행 파일(`env!("CARGO_BIN_EXE_feed")`)을 임시 링 경로로 띄우고, 테스트 안에서 gateway를 그 링으로 시작한 뒤, 웹소켓 클라이언트로 구독해서 국내와 미국 MESSAGE를 받는다. 본문이 계약의 `bodies`와 맞는지 기존 검사 함수로 확인한다. 끝나면 feed 프로세스를 반드시 종료한다.
9. `Decimal`에서 JSON 숫자로: `scale` 0, 2, 18인 값과 음수, 0.05처럼 정수부가 0인 값이 자릿수 그대로 나온다(단위 테스트여도 된다).

10. 쓰는 쪽이 `tail_intent`만 올리고 죽은 상태(파일의 `tail_intent`가 `tail`보다 앞선 상태)를 만들어 놓고 다시 열어 쓰면 `tail_intent`가 줄어들지 않는다.

기존 테스트 16개는 계속 통과해야 한다(발생기와 `Config`가 바뀌는 만큼 테스트 보조 코드는 맞춰 고친다). 테스트는 `sleep`으로 때우지 말고 타임아웃을 건 대기로 쓴다. 전체가 30초 안에 끝나야 한다.

## 검증 명령

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

그리고 두 실행 파일을 직접 띄워 gateway에 연결이 되고 MESSAGE가 오는지 한 번 확인한 뒤 두 프로세스를 모두 종료한다.

## 범위 밖

futex나 eventfd 같은 깨우기 장치, 읽는 쪽 위치를 링에 기록하는 일, 여러 쓰는 쪽, 링 크기 변경, 레코드 체크섬, 압축, 벤치마크. 위에 적은 Miri 명령 말고는 `cargo +nightly` 같은 다른 툴체인 지정 명령을 쓰지 않는다(없는 툴체인을 자동으로 내려받는다).

## 규칙

- git으로 상태를 바꾸지 않는다.
- 이 문서, `proto/`, `contract/`, `capture/`, `evidence/`, `.agents/`, `.claude/`, `AGENTS.md`, `CLAUDE.md`는 수정하지 않는다. 이 문서나 proto가 틀렸거나 모순된다고 판단하면 고치지 말고 멈춰서 보고한다.
- 이 문서에 없는 기능을 넣지 않는다.
- `README.md`의 "서버 실행" 절을 두 실행 파일 기준으로 고치고, "공유 메모리 링 버퍼" 절을 추가해 파일 배치 표와 쓰는 순서, 읽는 순서를 이 문서보다 짧게 적는다. "구현하지 않은 것" 목록도 맞게 고친다. 그 밖의 README 내용은 건드리지 않는다.
- 주석과 문서의 표기 규칙은 `AGENTS.md`를 따른다.
