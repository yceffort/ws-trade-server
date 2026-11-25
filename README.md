# ws-trade-server

증권 체결 웹소켓 서버가 어떻게 생겼을지 공개 자료만으로 추측하고, Rust로 직접 세워 보는 저장소다. 블로그 글의 근거 자료와 실험 코드를 둔다.

## 근거로 쓰는 것

- 공개 명세: 토스증권 Open API 실시간(웹소켓) AsyncAPI 문서
- 누구나 볼 수 있는 프레임: 로그인하지 않은 브라우저의 개발자 도구에 찍히는 웹소켓 프레임
- 공개 발표와 교과서 수준의 상식

특정 회사의 내부 구조를 아는 것처럼 쓰지 않는다. 받아 온 원본은 `evidence/raw/`에 두고 커밋하지 않는다. 출처와 버전, 해시는 `evidence/README.md`에 적는다.

## 두 실행 경로

실행 경로가 둘이다. 기본 STOMP 경로는 캡처한 프레임을 재현하는 단일 서버다. HA 경로는 같은 합성 체결을 복제 로그에 쓰고 여러 gateway가 나눠 받으며, 끊긴 클라이언트가 이어 받게 하는 별도 실험이다.

| 항목 | 기본 STOMP | HA |
| --- | --- | --- |
| 경로와 프로토콜 | `/ws`, `v12.stomp`. 캡처 계약을 따른다 | `/experimental/ha/ws`, `wts.ha.v1`. 이 저장소가 정한 JSON 프로토콜([HA 프로토콜](docs/ha-protocol.md)) |
| 입력 | `feed`가 쓰는 공유 메모리 링 | Kafka broker 3개의 복제 로그. active와 standby feed가 transaction으로 쓴다([복제 로그](deploy/ha/README.md)) |
| 인증 | 선택. 로컬 JSON 토큰 파일 | 로컬 JSON 토큰 파일 또는 공유 JWT와 HTTPS 정책 공급자([공유 인증](docs/ha-auth.md)) |
| 재접속 | 다시 구독한다. 끊긴 동안의 체결은 복구하지 않는다. `--http-addr`와 `--snapshot-topics`를 켜면 지정한 topic의 최신 본문을 `GET /snapshots`로 조회할 수 있다 | 서명 cursor로 보관 범위 안의 committed 체결을 다시 받는다. 클라이언트가 event ID로 중복을 거른다 |
| 데이터 보장 | 느린 연결은 끊고 링이 덮어쓰이면 체결을 잃는다. 잃은 양은 seq 누락으로 센다 | 상태와 cursor를 함께 영속화하는 클라이언트는 보관 범위 안에서 committed 기록과 같은 순서로 적용한다. 시험은 oracle로 누락과 중복 적용이 0인지 본다 |
| 실행과 배포 | `cargo run --bin feed`, `--bin gateway`. [단일 Pod](deploy/k8s/README.md), Recreate | `--features ha`. gateway와 edge 3개, broker 3개, feed 2개의 [별도 k8s 구성](deploy/k8s/ha/README.md)과 직렬 rollout |
| 검증 환경 | 단위, 통합, 캡처 대조, Compose, kind | 단일 호스트 Compose 장애 시험, kind 수용 시험, AWS EKS 실측 |

## HA 구현과 검증 상태

2026-11-23 `main` 기준이다. 구현한 것, 기능 시험을 통과한 것, 운영에 가까운 환경에서 잰 것을 나눠 적는다. 측정 수치와 환경은 [용량 기록](docs/ha-capacity.md)에 있다.

- 구현: 복제 로그와 producer fencing, checkpoint 복구, [gateway 복원](docs/ha-gateway.md), 서명 cursor 재접속, 공유 JWT와 정책 갱신, [준비와 drain 상태 머신](docs/ha-lifecycle.md), 별도 k8s 구성과 직렬 rollout, [관측 스택과 알림](deploy/k8s/ha/observability/README.md), [WSS 합성 probe](probe/README.md).
- 기능 시험(단일 호스트 Compose와 kind, CI의 `HA` workflow): producer 인계와 fencing, broker 1대 중단과 과반수 단절, gateway를 바꿔 가며 committed 대조, broker 하나를 예고 없이 끊는 동안 기존 연결 유지([#94](https://github.com/yceffort/ws-trade-server/issues/94)), 토큰 만료 뒤 재인증([#99](https://github.com/yceffort/ws-trade-server/issues/99)), 보관 경계 밖 cursor의 명시적 만료([#98](https://github.com/yceffort/ws-trade-server/issues/98)).
- 운영 환경 실측(AWS EKS, 2026-11-22): 1,000 clients의 계획 배포와 feed failover가 누락 0, 중복 0으로 통과했다. 같은 측정에서 AZ 하나를 끊자 그 AZ와 무관한 gateway의 클라이언트까지 끊겼고(#94), gateway 2개가 남은 동안 edge가 단일 스레드 한계에 있었다([#95](https://github.com/yceffort/ws-trade-server/issues/95)). 대응 뒤 2026-11-25에 다시 재자 AZ를 차단해도 다른 AZ의 연결 656개 중 650개가 유지됐고, gateway 하나가 730개를 받는 동안 edge worker 2개는 각각 0.8 core 아래였다. 대신 그 gateway와 broker가 함께 있던 노드의 CPU가 90% 안팎이었다.

아직 실험 범위 밖이거나 검증하지 않은 것:

- 입력은 합성 feed뿐이다. 외부 source는 `ExternalSource` 규약과 시험만 있고 실제 연결은 없다.
- 운영 인증 공급자는 없다. 정책 공급자와 토큰 발급은 개발용이다.
- 서로 다른 release를 섞어 돌릴 때의 프로토콜과 snapshot 호환은 시험하지 않았다.
- broker 디스크 손실 같은 스토리지 복구와 region 단위 DR은 범위 밖이다.
- 목표 처리량에서 24시간 보관은 장시간 시험을 하지 않았다(#98).

## 프레임 캡처

웹 화면의 소켓은 페이지가 아니라 SharedWorker 안에서 열려서 페이지의 Network 탭에는 프레임이 보이지 않는다. `capture/cdp-capture.js`는 CDP로 워커 타깃에 직접 붙어 핸드셰이크와 프레임을 받아 적는다.

```sh
node capture/cdp-capture.js https://www.tossinvest.com/stocks/A005930/order --seconds 25 --out evidence/raw/frames.jsonl
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

- [공통 입력 경계](docs/source-input.md)는 로컬 링과 broker 입력을 같은 bounded hub 큐에 전달한다. 별도 envelope의 안정적인 source/event 식별자와 십진 문자열 위치는 실험 규약이다. broker 입력을 쓰는 HA 경로는 위 "두 실행 경로"와 "HA 구현과 검증 상태"에 있다.

- 구독 중이 아닌 id의 `UNSUBSCRIBE`도 `receipt`가 있으면 같은 `RECEIPT`로 답함
- `UNSUBSCRIBE`에 `receipt`가 없으면 응답 없음
- `DISCONNECT`, 파싱할 수 없는 프레임, `CONNECT` 전의 다른 명령, 모르는 명령은 연결을 닫음
- 프레임은 NUL로 끝나야 하고, NUL 뒤에 줄바꿈이 더 붙은 프레임도 받음(캡처한 클라이언트 프레임은 모두 NUL로 끝남)
- 같은 id로 다시 구독하면 기존 구독을 덮어씀
- 송신 큐가 가득 찬 느린 클라이언트는 연결을 끊음. `--latest-first-topics`로 고른 topic의 MESSAGE는 아래 최신 우선 정책을 따름
- `--latest-first-topics topic,...`(`Config::latest_first_topics`)로 고른 topic은 최신 우선 정책을 쓴다. 형식과 검증은 `--allowed-topics`와 같고, 기본값은 비어 있으며 비어 있으면 동작과 프레임이 바뀌지 않는다. 개인 topic(`/experimental/personal/...`)은 넣어도 적용하지 않고 기본 정책을 따른다. 합치면 주문 생성처럼 뒤의 이벤트로 대신할 수 없는 이벤트가 사라지기 때문이다. 최신 우선 구독은 대기 칸 하나를 둔다. 발행이 오면 칸이 차 있을 때(그 표지가 아직 송신 큐에 있을 때)는 칸의 값만 바꾸고 앞의 체결은 건너뛴다. 칸이 비어 있고 송신 큐에 `--latest-first-depth`(`Config::latest_first_depth`, K, 기본값 64)건 미만이 쌓여 있으면 다른 topic처럼 MESSAGE로 넣고, K건 이상이면 칸을 채우고 표지를 넣는다. 그래서 최신 우선 topic만 구독한 멈춘 연결은 큐가 K건에 이른 뒤로 발행을 받은 구독마다 표지가 한 칸씩만 늘어나고, 큐에 이미 있는 프레임(RECEIPT 포함)과 이 표지를 합친 수가 송신 큐 크기를 넘지 않으면 끊지 않는다. 넘으면 끊는다. 풀리면 송신 큐에 남은 오래된 체결을 최대 K건 받은 뒤 구독마다 가장 최근 체결을 받는다. 칸이 구독마다 있으므로 같은 topic을 중복 구독한 구독끼리도 건너뛰는 체결이 다를 수 있다. 그래서 병합이 일어나면 계약의 `duplicateSubscriptions`에서 관측한 `deliveredToEach: true`가 성립하지 않을 수 있다(K=1로 캡처 재생을 대조하면 false였다). K는 병합을 시작하는 큐 깊이라서 가장 최근 체결보다 먼저 받는 프레임에는 다른 구독의 표지와 RECEIPT도 있을 수 있다. 큐가 K건 아래로 줄었어도 칸이 차 있으면 값만 바꾼다. 그래야 표지가 더 새 체결을 내보낸 뒤 더 오래된 MESSAGE가 나가는 일이 없다. K를 둔 것은 칸으로만 접으면(K=0) 쓰는 쪽이 조금만 밀려도 체결이 합쳐지기 때문이다. `bench/run.sh`로 연결 1,000개를 붙인 측정에서 K=0이면 읽기를 늦추지 않은 클라이언트도 종목당 초당 5건에서 0.5~3.8%, 80건에서 7.1~19.0%를 잃었다. K=64와 256은 5건, 40건, 80건에서 세 번씩, 9번 모두 유실과 건너뜀이 0이었다. K=8은 9번 가운데 80건의 한 번에서 57건을 건너뛰어 탐침이 118,100건 중 1건을 받지 못했다. 기본값 64는 실제 gateway에서 9번 모두 0이었던 가장 작은 측정값이다. 처리 시간이 발행 간격의 0.9배인 연결을 흉내 낸 예제에서도 K=8은 종목당 건수별 중앙값으로 2.5~3.2%를 잃었고 64는 0.0%(소수 첫째 자리까지 출력)였다. 64면 최신 우선 topic만 구독한 멈춘 연결이 붙드는 대기분은 송신 큐 용량 대신 64건과 구독당 1건이다(`bench/README.md`의 "느린 수신자 정책 세 방식 비교"). 쓰는 쪽이 계속 발행보다 느려 큐가 K건을 넘으면 그때부터는 체결이 합쳐진다. K는 송신 큐 용량보다 작아야 하며, 최신 우선 topic이 있는데 그렇지 않으면 시작하지 않는다. `message-id`의 순번은 실제로 쓰는 MESSAGE에만 매기므로 빠지지 않고, 그래서 순번으로는 건너뛴 체결을 알 수 없다. 건너뛴 건수는 `latest_skipped_total`로 끊은 횟수(`slow_disconnections_total`)와 따로 세고 `GET /metrics`와 10초 요약 로그에 남긴다. 표지와 RECEIPT, ERROR, 일반 MESSAGE로 송신 큐가 차면 지금처럼 끊는다. 송신이 일정 시간 막힌 연결은 아래 `--write-timeout-ms`로 끊을 수 있다. 최신 우선은 재접속 뒤 cursor로 복구할 대상이 아닌 topic에 쓰는 선택 정책이고, [고가용성 설계](docs/ha-design.md)에서 복구 대상인 topic은 느린 클라이언트를 끊고 cursor로 다시 붙는 쪽을 따른다. 공개 Open API의 체결과 호가 채널 서술(유실 시 최신 우선, 유실 감지용 순번 없음)에서 착안했으며 정책의 형태와 수치는 WTS에서 관측한 값이 아닌 이 저장소의 실험 설계다.
- 웹소켓 업그레이드와 첫 `CONNECT`를 10초 안에 끝내지 못하면 연결을 끊음
- 클라이언트 메시지는 64 KiB까지만 받음
- `--max-connections`, `--max-subscriptions`, `--max-commands-per-sec`는 0이면 무제한이다. 활성화하면 각각 업그레이드 대기 중인 소켓을 포함한 연결 수, 연결별 구독 id 수, 연결별 1초 고정 구간의 구독 및 해지 명령 수를 제한한다. `--allowed-topics`는 쉼표로 구분한 정확한 topic 허용 목록이다. 초과 연결은 최대 64개의 별도 거절 작업에서 HTTP 503으로 응답하고 거절 작업까지 차면 즉시 소켓을 닫는다. 구독 거절은 실험용 `ERROR`의 `message` 헤더에 사유를 넣고 연결 종료로 알린다. [공개 Open API](https://openapi.tossinvest.com/openapi-docs/latest/asyncapi.json)의 한도 기능에서 착안했으며 수치와 오류 프레임은 WTS 실측값이 아니다.
- 구독 거절은 송신 큐와 별도로 연결에 통지한다. `ERROR` 전송이 막히면 거절 시점부터 1초 뒤 소켓을 닫고 구독과 연결 슬롯을 반환한다. 이 유예 시간은 WTS 실측값이 아닌 실험 정책이다.
- 허브 명령 전달이 밀려도 소켓 읽기는 계속해 heartbeat를 반영한다. 연결별 대기 명령은 처리 중인 1개 외에 32개까지 보관하고, 초과하면 연결을 닫아 메모리를 제한한다. 이 대기열 크기는 실측값이 아닌 실험 정책이다.
- `--idle-ms`를 0보다 크게 주면 마지막 클라이언트 메시지 이후 그 시간 동안 수신이 없을 때 연결과 구독을 정리한다. 서버 송신은 타이머를 갱신하지 않는다. 기본값 0은 비활성이다. 공개 Open API의 유휴 종료에서 착안한 실험이며 WTS에서 관측한 값이나 STOMP heartbeat 협상 결과가 아니다.
- `--write-timeout-ms`(`Config::write_timeout`)를 0보다 크게 주면 소켓 쓰기 한 번(최대 4프레임의 묶음 쓰기 또는 heartbeat)이 그 시간 안에 끝나지 않을 때 ERROR 없이 연결과 구독을 정리하고 종료 사유 `write_timeout`으로 센다. 송신 큐가 차야 끊는 기본 정책과 달리, 개인 topic처럼 드물게 발행되는 연결도 소켓이 막히면 끊긴다. 한 소켓에 공개 topic과 개인 topic이 함께 실리므로 기한은 topic이 아니라 연결 단위다. 쓸 것이 없는 동안은 재지 않아 유휴 연결은 끊지 않지만, 느리게라도 읽는 클라이언트도 쓰기 한 번이 기한을 넘기면 끊긴다. 기한이 지나면 남은 쓰기를 멈추고 연결을 닫는다. 그 묶음의 일부는 이미 소켓에 넘어갔을 수 있으므로 어디까지 전달됐는지는 보장하지 않는다. 느린 연결 차단(`slow_disconnections_total`)과는 따로 센다. 기본값 0은 비활성이다. [공개 Open API](https://openapi.tossinvest.com/openapi-docs/latest/asyncapi.json)의 주문 이벤트 채널 서술(미소비분을 건너뛰지 않고 유지하다가 수신이 2초 이상 막히면 별도 close code 없이 연결 종료)에서 착안했다. 그 2초를 기본값으로 두지 않으며, 기한 값과 연결 단위 적용은 WTS 실측이 아닌 이 저장소의 실험 정책이다.
- `--http-addr 127.0.0.1:8081 --snapshot-topics topic,...`를 주면 지정한 topic만 최신 본문과 서버 수신 시각을 보관하고 `GET /snapshots`로 조회한다. 기본값은 비활성이다. 전체 체결 재전송이나 원자적인 스트림 전환은 보장하지 않는다. `node examples/reconnect.js ws://127.0.0.1:8080/ws http://127.0.0.1:8081/snapshots topic...`는 지수 지연과 jitter로 재접속하고 재구독 후 조회한다. 조회 중 스트림이 갱신된 topic은 스냅샷으로 덮어쓰지 않는다. 이 API는 loopback에서만 열리며, Open API의 초기 REST 조회에서 착안한 실험 설계다.
- `start_managed`의 종료 핸들과 gateway의 SIGINT/SIGTERM은 새 연결을 막고 실험용 `ERROR message:server-shutdown`을 보낸다. 3초 안에 송신을 끝내지 못한 연결은 취소한다. 조회 소켓, hub, 링 대기 및 발행 스레드도 정리한다. `start`는 기존 테스트용으로 프로세스 수명의 동작을 유지한다. 종료 통지의 기능은 공개 Open API를 참고했고 STOMP 오류 모양은 이 저장소에서 정했다.
- `--auth-file <경로>`는 실험용 JSON 토큰 배열 `[{"token":"demo-only","expires_at":<UNIX 초>}]`을 읽고 CONNECT의 `authorization:Bearer <토큰>`을 검사한다. Unix에서 일반 파일만 읽고 500ms마다 다시 확인하며 실패하면 목록을 비운다. 파일 열기는 nonblocking으로 처리해 FIFO로 교체돼도 종료를 막지 않는다. 만료도 최대 500ms마다 시스템 시각으로 재검사한다. 활성 세션의 만료와 폐기도 반영하고, 송신이 막혀 오류 통지를 못 보낸 소켓은 1초 안에 닫는다. 잘못된 토큰, 만료, 폐기는 각각 `auth-invalid`, `auth-expired`, `auth-revoked` 오류로 구분한다. 토큰 비교는 일치 위치와 무관한 시간에 끝나도록 `subtle`의 상수 시간 비교로 목록 전체를 훑는다(토큰 길이는 숨기지 않는다). 실제 OAuth 검증을 대신하지 않는다. 재접속 예제는 `DEMO_TOKEN` 환경 변수를 사용한다. 조회 HTTP는 로컬 관리용이며 토큰 인증 대상이 아니다.
- 토큰 파일 항목에 선택 필드 `principal`을 두면(`{"token":"demo-only","expires_at":<UNIX 초>,"principal":"demo-account"}`) 그 토큰으로 연결한 주체만 `/experimental/personal/{principal}/orders` 같은 개인 topic을 구독할 수 있다. `principal`이 없는 기존 파일도 그대로 읽으며, 없거나 빈 문자열이면 주체가 없는 것으로 본다. 주체는 CONNECT 시점에 고정하며 파일의 `principal`이 변경되거나 제거되면 기존 세션을 `auth-revoked`로 종료하고 재인증을 요구한다. 접두사 `/experimental/personal/`로 시작하는 destination은 모두 개인 topic이고 그 뒤 첫 구간이 소유자다. 소유자가 없는 `/experimental/personal` 자체도 거절한다. 그래서 주체에 `/`가 들어 있으면 첫 구간만 소유자로 읽혀 자기 topic도 구독할 수 없다. 소유자 구간이 비었거나, 주체가 다르거나, 인증을 켜지 않은 연결의 구독은 색인에 올리기 전에 그 구독만 거절하고 연결과 다른 구독은 유지한다. 거절된 SUBSCRIBE는 아무 상태도 바꾸지 않으므로 이미 쓰는 id로 보냈어도 그 id의 기존 구독은 그대로 남는다(같은 id의 덮어쓰기는 받아들인 구독에만 일어난다). `receipt`가 있으면 `RECEIPT`에 `message:forbidden-topic`과 `response-code:403`을 담아 알리고, 없으면 아무 프레임도 보내지 않는다. 사유는 미인증과 남의 topic을 구분하지 않으며 거절 프레임과 로그에 토큰이나 주체를 넣지 않는다. 명령 속도 제한은 이 확인보다 먼저 적용하고, 거절된 구독은 `--max-subscriptions`에 세지 않는다. `--allowed-topics`는 정확히 일치하는 공개 topic 목록이라 주체별 topic을 나열할 수 없으므로 개인 topic에는 적용하지 않고 소유 여부로만 허용한다. `--snapshot-topics`에 개인 topic을 넣어도 보관하지 않아 `GET /snapshots`로 본문이 나가지 않는다. 토큰이 폐기되거나 만료되면 기존 세션 종료 경로로 개인 topic 전달도 함께 끊긴다. 개인 이벤트를 만드는 발행기는 없다. [공개 Open API](https://openapi.tossinvest.com/openapi-docs/latest/asyncapi.json)의 주문 이벤트 채널(본인 계좌만 구독, 부적격 항목만 거부)에서 착안했으며 topic 이름과 거절 형식은 WTS에서 관측한 값이 아닌 이 저장소의 실험 규약이다.
- 선택적인 [TLS 프록시](edge/README.md)는 로컬 인증서로 WSS를 제공한다. `/ws`, `v12.stomp`, 메시지 경계와 ping/pong을 gateway까지 전달하고 송신 버퍼가 1 MiB를 넘으면 연결을 끊는다. 기본 gateway는 계속 WS이며 WTS 서버의 내부 TLS 배치를 재현한다고 주장하지 않는다.
- TLS 프록시는 명시적인 `deflate` 옵션에서만 permessage-deflate를 협상한다. 양방향 context takeover를 끄고 압축 작업 동시 수를 4로 제한한다. gateway와 프록시 사이는 계속 평문이며 WTS의 압축 파라미터를 재현한 것은 아니다. 같은 합성 데이터의 전송 바이트, CPU, 메모리와 지연 비교는 `node edge/compare.js`로 실행한다.
- `cargo run --example corrections`는 별도 `/experimental/trade-events` topic에서 생성, 정정, 취소를 발행하고 WebSocket 소비자가 누적 수량과 금액을 갱신하는 예제다. `event_id`는 중복 적용을 막고 정정 및 취소의 `target_id`는 기존 체결을 가리킨다. 잘못된 수량, 없는 대상, 이미 취소한 대상과 같은 이벤트 id의 다른 본문은 오류다. 이력 용량을 넘으면 자동 퇴출하지 않고 오류를 반환한다. 금액은 정수 최소 단위다. [Alpaca의 정정 및 취소 기능](https://docs.alpaca.markets/us/docs/real-time-stock-pricing-data)에서 착안했으며 형식과 합성 값은 WTS와 무관하다.
- `--http-addr`를 켜면 로컬 `GET /metrics`에서 STOMP 연결 수, 구독 수, 송신 큐 점유와 용량, hub 명령 대기, 발행 및 큐 적재 수, 느린 연결 차단 수, 최신 우선 정책에서 건너뛴 체결 수, 링 덮어쓰기 횟수, seq 누락 건수와 재시작 횟수, 디코딩 및 본문 생성 실패를 읽는다. 링 덮어쓰기 횟수는 유실 체결 건수가 아니며 seq 누락과 합산하지 않는다. 큐 적재 수는 클라이언트 수신 보장이 아니다. 지표에는 종목이나 토큰 label이 없고 팬아웃 집계는 hub 안에서 수행한다. 브리지 카운터만 atomic으로 읽으므로 모든 필드가 같은 순간의 값임을 보장하지 않는다. 특정 증권사의 내부 메트릭을 재현한 것은 아니다.
- 같은 조회 포트의 `GET /prometheus`는 `/metrics`의 필드를 `wts_` 접두사를 붙인 Prometheus text 형식으로 내고, `_total`로 끝나는 것만 counter다. `/metrics` JSON의 필드와 응답은 바뀌지 않는다. 연결 종료 사유(`wts_connections_closed_total`), 세션 전 거절 사유(`wts_connections_rejected_total`), 구독 거절 사유(`wts_subscription_rejections_total`)는 고정 목록과 `other`만 `reason` label로 쓰고, 목록 밖 사유는 `other`로 센다. 사용자, 토큰, connection-id, topic은 label로 만들지 않으므로 시계열 수는 연결 수와 무관하게 일정하다. 송신 큐가 차서 hub가 지운 연결의 종료 사유는 `slow_consumer`이고 `slow_disconnections_total`과 같은 사건이다. `latest_skipped_total`, 링 덮어쓰기, seq 누락, 느린 연결 차단은 따로 내며 하나의 유실 수로 합치지 않는다. `wts_start_time_seconds`가 바뀌면 counter가 0부터 다시 센 것이다. hub가 답하지 않으면 0을 내지 않고 503을 돌려 수집 실패(미수집)로 보이게 한다. 사유 목록과 이름은 WTS에서 관측한 값이 아닌 이 저장소의 실험 규약이다. `wts_queue_wait_seconds`는 hub가 발행을 처리하기 시작한 때부터 쓰는 태스크가 그 MESSAGE(최신 우선 표지면 칸에 든 본문)를 큐에서 꺼낼 때까지, `wts_socket_write_seconds`는 묶음 하나의 프레임을 만들어 웹소켓에 넘기고 flush가 끝날 때까지의 시간(초)이다. flush 완료는 커널 송신 버퍼에 넘겼다는 뜻이고, 둘 다 클라이언트가 받거나 적용한 시각이 아니다. 실패하거나 기한에 걸린 쓰기는 세지 않는다. 시계는 발행마다 한 번, 쓰기 묶음에서는 항목을 큐에서 꺼낼 때마다 한 번과 끝에 한 번 읽고, 최신 우선 표지는 칸을 비운 직후에 한 번 더 읽는다. 경계는 100us부터 5초까지 15개로 고정이고 label이 없다. 계측 전후 비용은 `bench/README.md`의 "지연 계측 비용"에 있다.
- gateway는 [구조화 로그](docs/logging.md)를 stderr로 출력한다. `WTS_LOG_FORMAT=json`과 `RUST_LOG`로 형식과 수준을 고르고 연결 종료 사유는 DEBUG에서 확인한다. 체결별 로그 대신 10초 요약을 남기며, 출력 큐가 차서 버린 로그는 `logs_dropped_total`로 센다. 로깅 정책은 이 저장소의 운영 실험이다.
- [Docker와 Kubernetes 배포](deploy/README.md)는 합성 feed용 실험 설정이다. 두 topic에 최근 10초 이내 snapshot이 있어야 준비 상태로 판단하며 WTS의 배포 구조나 실거래 휴장 정책을 재현한 것이 아니다.
- [WSS 합성 probe](probe/README.md)는 고정 대상의 인증, 구독, 상태 적용과 재접속을 주기적으로 검증한다. HA는 committed oracle를 대조하고 STOMP는 단절 중 체결 무유실을 주장하지 않는다. 실행 기한과 지표는 이 저장소의 실험 정책이다.
- [관측 대시보드](deploy/k8s/ha/observability/README.md)는 서비스, 전달, HA와 컨테이너 자원을 namespace/Pod별로 보여준다. 부하 worker event-loop 지연과 계획 배포, rollback, 장애 주입 로그를 함께 표시하며 이 구성과 임계값은 이 저장소의 실험 설계다.
- 연결마다 잡는 읽기 버퍼는 4 KiB(웹소켓 라이브러리 기본값은 128 KiB)
- 송신 큐에 이미 쌓인 프레임은 4개까지 모아 소켓에 한 번에 씀. 프레임의 내용과 순서는 그대로임(측정은 `bench/README.md`의 "쓰기 묶음")

구현하지 않은 것:

- 헤더 이스케이프
- 설정 파일
- 링의 깨우기 장치(futex, eventfd), 읽는 쪽 위치를 링에 기록하는 일, 여러 쓰는 쪽, 링 크기 변경, 레코드 체크섬, 압축

## 부하 실험

실제 WebSocket 클라이언트의 인증, 구독, 수신, 해지와 재접속은 [종단 테스트 안내](docs/websocket-e2e.md)의 명령으로 확인한다. 테스트가 합성 feed와 gateway, TLS 프록시를 띄우며 기존 실험 서버에 클라이언트만 연결하는 실행 예제도 제공한다.

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
node capture/replay-client.js ws://127.0.0.1:8080/ws --seconds 25 --out evidence/raw/replay.jsonl
node capture/extract-contract.js evidence/raw/replay.jsonl > evidence/raw/replay-contract.json
diff contract/wts-stomp.json evidence/raw/replay-contract.json
```

## Docker와 Kubernetes 배포

서비스용 Rust 이미지와 TLS 프록시 이미지, Compose 설정과 종단 검증은 [배포 안내](deploy/README.md)에 있다. 합성 feed를 사용하는 단일 인스턴스 실행 환경이며 실제 증권사 배포를 재현한 것은 아니다.

[Kubernetes 안내](deploy/k8s/README.md)는 공유 링을 같은 Pod의 emptyDir에 두는 Deployment, Secret, probe와 kind 검증을 제공한다. 단일 replica와 Recreate를 사용하므로 교체 중 중단이 있으며 수평 확장이나 고가용성을 제공하지 않는다.

HA 구성은 이 단일 인스턴스 설정과 별개다. 배포는 [HA 배포](deploy/k8s/ha/README.md), 구조와 보장 범위는 [HA 구조](docs/ha-design.md), 배포 순서와 검증 목표는 [구현 및 검증 계획](docs/ha-rollout.md), 수용 시험은 [HA 수용 시험](bench/ha/README.md), 운영 확인은 [운영 절차](docs/ha-runbook.md)에 있다. 설계와 측정은 공개 자료와 이 저장소의 실험에서 나왔으며 실제 증권사 동작을 관측한 것이 아니다.
