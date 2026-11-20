# ws-trade-server

증권 체결 웹소켓 서버가 어떻게 생겼을지 공개 자료만으로 추측하고, Rust로 직접 세워 보는 저장소다. 블로그 글의 근거 자료와 실험 코드를 둔다.

## 근거로 쓰는 것

- 공개 명세: 토스증권 Open API 실시간(웹소켓) AsyncAPI 문서
- 누구나 볼 수 있는 프레임: 로그인하지 않은 브라우저의 개발자 도구에 찍히는 웹소켓 프레임
- 공개 발표와 교과서 수준의 상식

특정 회사의 내부 구조를 아는 것처럼 쓰지 않는다. 받아 온 원본은 `evidence/raw/`에 두고 커밋하지 않는다. 출처와 버전, 해시는 `evidence/README.md`에 적는다.

## 프레임 캡처

웹 화면의 소켓은 페이지가 아니라 SharedWorker 안에서 열려서 페이지의 Network 탭에는 프레임이 보이지 않는다. `capture/cdp-capture.mjs`는 CDP로 워커 타깃에 직접 붙어 핸드셰이크와 프레임을 받아 적는다.

```sh
node capture/cdp-capture.mjs https://www.tossinvest.com/stocks/A005930/order --seconds 25 --out evidence/raw/frames.jsonl
```

## 서버 실행

캡처한 프레임과 모양이 같은 STOMP over WebSocket 체결 서버다. 프레임 형식은 토스증권 웹 화면의 실시간 소켓을 참고해 그대로 따랐다. 서버 안쪽 구조는 일반적인 설계로 직접 정한 것이며 토스증권의 실제 구현과는 무관하다. 프로세스는 둘이다. `feed`가 가짜 체결을 protobuf(`proto/feed.proto`의 `Trade`)로 만들어 공유 메모리 링 버퍼에 쓰고, `gateway`가 링을 읽어 JSON 본문으로 바꿔 클라이언트에 보낸다. 어느 쪽을 먼저 띄워도 된다. 인자는 모두 선택이고 아래 값이 기본값이며, `--ring`의 기본값은 임시 디렉터리 아래 `ws-trade-feed.ring`이다.

```sh
cargo run --bin feed -- --ring <경로> --symbols kr:A005930,kr:A000660,us:US19990122001 --rate 5 --capacity 1048576
cargo run --bin gateway -- --addr 127.0.0.1:8080 --ring <경로> --poll-us 200
```

캡처에서 확인해 구현한 것:

- 경로 `/ws`와 서브프로토콜 `v12.stomp` 협상
- `CONNECTED`, `RECEIPT`, `MESSAGE`의 헤더 이름과 순서, NUL 종료
- `RECEIPT`가 그 구독의 첫 `MESSAGE`보다 먼저 나감
- `message-id`는 `{connection-id}-{seq}`, 연결마다 1부터 빠짐없이 증가
- 국내와 미국 본문의 키 순서와 숫자 타입(정수, 소수)
- 같은 destination을 두 id로 구독하면 각각 한 번씩 전달
- 데이터가 흐르는 중에도 서버 heartbeat 전송, 클라이언트 heartbeat는 무시
- `UNSUBSCRIBE`(`receipt`, `id`)는 구독을 제거하고, `receipt`가 있으면 `message:unsubscribe success`인 `RECEIPT`로 답함. 그 `RECEIPT` 뒤에는 그 구독의 `MESSAGE`가 오지 않음

실측이 없어 STOMP 기본 동작으로 넣은 것:

- [공통 입력 경계](docs/source-input.md)는 로컬 링과 향후 broker 입력을 같은 bounded hub 큐에 전달한다. 별도 envelope의 안정적인 source/event 식별자와 십진 문자열 위치는 실험 규약이다. Kafka 연결, durable checkpoint, replay와 HA 배포는 아직 구현하지 않았다.

- 구독 중이 아닌 id의 `UNSUBSCRIBE`도 `receipt`가 있으면 같은 `RECEIPT`로 답함
- `UNSUBSCRIBE`에 `receipt`가 없으면 응답 없음
- `DISCONNECT`, 파싱할 수 없는 프레임, `CONNECT` 전의 다른 명령, 모르는 명령은 연결을 닫음
- 프레임은 NUL로 끝나야 하고, NUL 뒤에 줄바꿈이 더 붙은 프레임도 받음(캡처한 클라이언트 프레임은 모두 NUL로 끝남)
- 같은 id로 다시 구독하면 기존 구독을 덮어씀
- 송신 큐가 가득 찬 느린 클라이언트는 연결을 끊음(임시 정책)
- 웹소켓 업그레이드와 첫 `CONNECT`를 10초 안에 끝내지 못하면 연결을 끊음
- 클라이언트 메시지는 64 KiB까지만 받음
- `--max-connections`, `--max-subscriptions`, `--max-commands-per-sec`는 0이면 무제한이다. 활성화하면 각각 업그레이드 대기 중인 소켓을 포함한 연결 수, 연결별 구독 id 수, 연결별 1초 고정 구간의 구독 및 해지 명령 수를 제한한다. `--allowed-topics`는 쉼표로 구분한 정확한 topic 허용 목록이다. 초과 연결은 최대 64개의 별도 거절 작업에서 HTTP 503으로 응답하고 거절 작업까지 차면 즉시 소켓을 닫는다. 구독 거절은 실험용 `ERROR`의 `message` 헤더에 사유를 넣고 연결 종료로 알린다. [공개 Open API](https://openapi.tossinvest.com/openapi-docs/latest/asyncapi.json)의 한도 기능에서 착안했으며 수치와 오류 프레임은 WTS 실측값이 아니다.
- 구독 거절은 송신 큐와 별도로 연결에 통지한다. `ERROR` 전송이 막히면 거절 시점부터 1초 뒤 소켓을 닫고 구독과 연결 슬롯을 반환한다. 이 유예 시간은 WTS 실측값이 아닌 실험 정책이다.
- 허브 명령 전달이 밀려도 소켓 읽기는 계속해 heartbeat를 반영한다. 연결별 대기 명령은 처리 중인 1개 외에 32개까지 보관하고, 초과하면 연결을 닫아 메모리를 제한한다. 이 대기열 크기는 실측값이 아닌 실험 정책이다.
- `--idle-ms`를 0보다 크게 주면 마지막 클라이언트 메시지 이후 그 시간 동안 수신이 없을 때 연결과 구독을 정리한다. 서버 송신은 타이머를 갱신하지 않는다. 기본값 0은 비활성이다. 공개 Open API의 유휴 종료에서 착안한 실험이며 WTS에서 관측한 값이나 STOMP heartbeat 협상 결과가 아니다.
- `--http-addr 127.0.0.1:8081 --snapshot-topics topic,...`를 주면 지정한 topic만 최신 본문과 서버 수신 시각을 보관하고 `GET /snapshots`로 조회한다. 기본값은 비활성이다. 전체 체결 재전송이나 원자적인 스트림 전환은 보장하지 않는다. `node examples/reconnect.mjs ws://127.0.0.1:8080/ws http://127.0.0.1:8081/snapshots topic...`는 지수 지연과 jitter로 재접속하고 재구독 후 조회한다. 조회 중 스트림이 갱신된 topic은 스냅샷으로 덮어쓰지 않는다. 이 API는 loopback에서만 열리며, Open API의 초기 REST 조회에서 착안한 실험 설계다.
- `start_managed`의 종료 핸들과 gateway의 SIGINT/SIGTERM은 새 연결을 막고 실험용 `ERROR message:server-shutdown`을 보낸다. 3초 안에 송신을 끝내지 못한 연결은 취소한다. 조회 소켓, hub, 링 대기 및 발행 스레드도 정리한다. `start`는 기존 테스트용으로 프로세스 수명의 동작을 유지한다. 종료 통지의 기능은 공개 Open API를 참고했고 STOMP 오류 모양은 이 저장소에서 정했다.
- `--auth-file <경로>`는 실험용 JSON 토큰 배열 `[{"token":"demo-only","expires_at":<UNIX 초>}]`을 읽고 CONNECT의 `authorization:Bearer <토큰>`을 검사한다. Unix에서 일반 파일만 읽고 500ms마다 다시 확인하며 실패하면 목록을 비운다. 파일 열기는 nonblocking으로 처리해 FIFO로 교체돼도 종료를 막지 않는다. 만료도 최대 500ms마다 시스템 시각으로 재검사한다. 활성 세션의 만료와 폐기도 반영하고, 송신이 막혀 오류 통지를 못 보낸 소켓은 1초 안에 닫는다. 잘못된 토큰, 만료, 폐기는 각각 `auth-invalid`, `auth-expired`, `auth-revoked` 오류로 구분한다. 실제 OAuth 검증을 대신하지 않는다. 재접속 예제는 `DEMO_TOKEN` 환경 변수를 사용한다. 조회 HTTP는 로컬 관리용이며 토큰 인증 대상이 아니다.
- 토큰 파일 항목에 선택 필드 `principal`을 두면(`{"token":"demo-only","expires_at":<UNIX 초>,"principal":"demo-account"}`) 그 토큰으로 연결한 주체만 `/experimental/personal/{principal}/orders` 같은 개인 topic을 구독할 수 있다. `principal`이 없는 기존 파일도 그대로 읽으며, 없거나 빈 문자열이면 주체가 없는 것으로 본다. 주체는 CONNECT 시점에 고정하며 파일의 `principal`이 변경되거나 제거되면 기존 세션을 `auth-revoked`로 종료하고 재인증을 요구한다. 접두사 `/experimental/personal/`로 시작하는 destination은 모두 개인 topic이고 그 뒤 첫 구간이 소유자다. 소유자가 없는 `/experimental/personal` 자체도 거절한다. 그래서 주체에 `/`가 들어 있으면 첫 구간만 소유자로 읽혀 자기 topic도 구독할 수 없다. 소유자 구간이 비었거나, 주체가 다르거나, 인증을 켜지 않은 연결의 구독은 색인에 올리기 전에 그 구독만 거절하고 연결과 다른 구독은 유지한다. 거절된 SUBSCRIBE는 아무 상태도 바꾸지 않으므로 이미 쓰는 id로 보냈어도 그 id의 기존 구독은 그대로 남는다(같은 id의 덮어쓰기는 받아들인 구독에만 일어난다). `receipt`가 있으면 `RECEIPT`에 `message:forbidden-topic`과 `response-code:403`을 담아 알리고, 없으면 아무 프레임도 보내지 않는다. 사유는 미인증과 남의 topic을 구분하지 않으며 거절 프레임과 로그에 토큰이나 주체를 넣지 않는다. 명령 속도 제한은 이 확인보다 먼저 적용하고, 거절된 구독은 `--max-subscriptions`에 세지 않는다. `--allowed-topics`는 정확히 일치하는 공개 topic 목록이라 주체별 topic을 나열할 수 없으므로 개인 topic에는 적용하지 않고 소유 여부로만 허용한다. `--snapshot-topics`에 개인 topic을 넣어도 보관하지 않아 `GET /snapshots`로 본문이 나가지 않는다. 토큰이 폐기되거나 만료되면 기존 세션 종료 경로로 개인 topic 전달도 함께 끊긴다. 개인 이벤트를 만드는 발행기는 없다. [공개 Open API](https://openapi.tossinvest.com/openapi-docs/latest/asyncapi.json)의 주문 이벤트 채널(본인 계좌만 구독, 부적격 항목만 거부)에서 착안했으며 topic 이름과 거절 형식은 WTS에서 관측한 값이 아닌 이 저장소의 실험 규약이다.
- 선택적인 [TLS 프록시](edge/README.md)는 로컬 인증서로 WSS를 제공한다. `/ws`, `v12.stomp`, 메시지 경계와 ping/pong을 gateway까지 전달하고 송신 버퍼가 1 MiB를 넘으면 연결을 끊는다. 기본 gateway는 계속 WS이며 WTS 서버의 내부 TLS 배치를 재현한다고 주장하지 않는다.
- TLS 프록시는 명시적인 `deflate` 옵션에서만 permessage-deflate를 협상한다. 양방향 context takeover를 끄고 압축 작업 동시 수를 4로 제한한다. gateway와 프록시 사이는 계속 평문이며 WTS의 압축 파라미터를 재현한 것은 아니다. 같은 합성 데이터의 전송 바이트, CPU, 메모리와 지연 비교는 `node edge/compare.mjs`로 실행한다.
- `cargo run --example corrections`는 별도 `/experimental/trade-events` topic에서 생성, 정정, 취소를 발행하고 WebSocket 소비자가 누적 수량과 금액을 갱신하는 예제다. `event_id`는 중복 적용을 막고 정정 및 취소의 `target_id`는 기존 체결을 가리킨다. 잘못된 수량, 없는 대상, 이미 취소한 대상과 같은 이벤트 id의 다른 본문은 오류다. 이력 용량을 넘으면 자동 퇴출하지 않고 오류를 반환한다. 금액은 정수 최소 단위다. [Alpaca의 정정 및 취소 기능](https://docs.alpaca.markets/us/docs/real-time-stock-pricing-data)에서 착안했으며 형식과 합성 값은 WTS와 무관하다.
- `--http-addr`를 켜면 로컬 `GET /metrics`에서 STOMP 연결 수, 구독 수, 송신 큐 점유와 용량, hub 명령 대기, 발행 및 큐 적재 수, 느린 연결 차단 수, 링 덮어쓰기 횟수, seq 누락 건수와 재시작 횟수, 디코딩 및 본문 생성 실패를 읽는다. 링 덮어쓰기 횟수는 유실 체결 건수가 아니며 seq 누락과 합산하지 않는다. 큐 적재 수는 클라이언트 수신 보장이 아니다. 지표에는 종목이나 토큰 label이 없고 팬아웃 집계는 hub 안에서 수행한다. 브리지 카운터만 atomic으로 읽으므로 모든 필드가 같은 순간의 값임을 보장하지 않는다. 특정 증권사의 내부 메트릭을 재현한 것은 아니다.
- gateway는 [구조화 로그](docs/logging.md)를 stderr로 출력한다. `WTS_LOG_FORMAT=json`과 `RUST_LOG`로 형식과 수준을 고르고 연결 종료 사유는 DEBUG에서 확인한다. 체결별 로그 대신 10초 요약을 남기며, 출력 큐가 차서 버린 로그는 `logs_dropped_total`로 센다. 로깅 정책은 이 저장소의 운영 실험이다.
- [Docker와 Kubernetes 배포](deploy/README.md)는 합성 feed용 실험 설정이다. 두 topic에 최근 10초 이내 snapshot이 있어야 준비 상태로 판단하며 WTS의 배포 구조나 실거래 휴장 정책을 재현한 것이 아니다.
- [고가용성 및 재접속 복구 설계](docs/ha-design.md)는 미구현 제안이다. 복제 로그와 여러 gateway, cursor 복구 및 단계별 drain을 별도 HA 구성으로 계획하며 실제 증권사 동작을 관측한 내용이 아니다.
- 연결마다 잡는 읽기 버퍼는 4 KiB(웹소켓 라이브러리 기본값은 128 KiB)
- 송신 큐에 이미 쌓인 프레임은 4개까지 모아 소켓에 한 번에 씀. 프레임의 내용과 순서는 그대로임(측정은 `bench/README.md`의 "쓰기 묶음")

구현하지 않은 것:

- 헤더 이스케이프
- 설정 파일
- 링의 깨우기 장치(futex, eventfd), 읽는 쪽 위치를 링에 기록하는 일, 여러 쓰는 쪽, 링 크기 변경, 레코드 체크섬, 압축

## 부하 실험

`bench/run.sh <이름> --connections 200 --seconds 5`로 gateway를 띄우고 부하를 걸어 `bench/results/<이름>.json`을 만든다. 인자와 결과 필드는 `bench/README.md`에 있다. Linux에서 재려면 같은 명령 앞에 `bench/linux.sh`를 붙인다.

## 공유 메모리 링 버퍼

`src/ring.rs`. 쓰는 쪽 하나, 읽는 쪽 여럿인 브로드캐스트 링이다. 쓰는 쪽은 읽는 쪽을 기다리지 않고, 덮어쓰인 읽는 쪽은 스스로 유실을 알아챈다. 두 프로세스가 파일 하나를 `mmap`으로 공유하며, 정수는 리틀 엔디언이다.

| 오프셋 | 크기 | 내용 |
| --- | --- | --- |
| 0 | 8 | magic `WSRING01`. 파일을 만들 때 맨 마지막에 쓴다 |
| 8 | 8 | capacity. 데이터 영역의 바이트 수, 8192 이상인 2의 거듭제곱 |
| 16 | 8 | `tail_intent`. 쓰는 쪽이 여기까지 덮어쓰겠다고 먼저 알리는 위치 |
| 24 | 8 | `tail`. 여기까지는 다 썼다는 위치 |
| 32 | 32 | 예약(0) |
| 64 | capacity | 데이터 영역. 레코드는 `[len: u32][payload][8바이트 경계까지 패딩]`, 끝에 자리가 모자라면 `len = u32::MAX`인 건너뛰기 표시를 쓰고 처음부터 쓴다 |

위치는 지금까지 쓴 총 바이트 수이고 줄어들지 않는다. 데이터 영역은 `AtomicU8`로만 읽고 쓴다.

쓰는 순서(레코드마다):

1. `tail_intent`에 `max(저장된 값, tail + 크기)`를 Relaxed로 저장하고 `fence(Release)`
2. 바이트를 쓴다
3. `tail`에 `tail + 크기`를 Release로 저장

읽는 순서(읽기 전용 매핑이라 Relaxed load와 `fence(Acquire)`를 짝지어 쓴다):

1. `tail`을 읽고 `fence(Acquire)`. 내 위치와 같으면 새 데이터가 없다
2. `len`을 복사한다. 4096보다 크거나 영역 끝을 넘으면 유실로 처리한다
3. payload를 내 버퍼로 복사한다
4. `fence(Acquire)` 뒤에 `tail_intent`를 읽는다. 내 위치보다 capacity 넘게 앞서 있으면 복사하는 동안 덮어쓰였을 수 있으므로 버리고, 위치를 지금의 `tail`로 옮기고 유실을 알린다

쓰는 쪽은 파일에 배타적 잠금을 잡고, 이미 있는 파일은 줄이지 않고 기존 `tail`에서 이어 쓴다. 파일 mmap을 지원하지 않는 Miri로는 힙 메모리 링(`ring::in_memory`)을 검사한다.

```sh
cargo +nightly miri test --test ring_miri
```

## 캡처와 대조하기

서버를 띄운 뒤 계약의 클라이언트 프레임을 그대로 재생하고, 진짜 서버의 캡처에 쓴 것과 같은 추출기를 돌려 두 결과를 비교한다. 압축 협상, 건수, 자릿수, 타이밍처럼 가짜 데이터라서 달라지는 항목만 다르게 나와야 한다.

```sh
node capture/replay-client.mjs ws://127.0.0.1:8080/ws --seconds 25 --out evidence/raw/replay.jsonl
node capture/extract-contract.mjs evidence/raw/replay.jsonl > evidence/raw/replay-contract.json
diff contract/wts-stomp.json evidence/raw/replay-contract.json
```

## Docker와 Kubernetes 배포

서비스용 Rust 이미지와 TLS 프록시 이미지, Compose 설정과 종단 검증은 [배포 안내](deploy/README.md)에 있다. 합성 feed를 사용하는 단일 인스턴스 실행 환경이며 실제 증권사 배포를 재현한 것은 아니다.

[Kubernetes 안내](deploy/k8s/README.md)는 공유 링을 같은 Pod의 emptyDir에 두는 Deployment, Secret, probe와 kind 검증을 제공한다. 단일 replica와 Recreate를 사용하므로 교체 중 중단이 있으며 수평 확장이나 고가용성을 제공하지 않는다.

고가용성과 계획된 배포 중 서비스 가용성의 후속 설계는 [HA 구조](docs/ha-design.md)와 [구현 및 검증 계획](docs/ha-rollout.md)에 있다. 현재 배포 설정을 변경하거나 HA 구현이 완료되었다는 뜻은 아니다.
