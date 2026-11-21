# 복구 가능한 WebSocket 프로토콜

[#49](https://github.com/yceffort/ws-trade-server/issues/49)의 실험 구현이다. 기존 `/ws`와 캡처 계약은 유지한다. `ha` feature와 [복제 로그](../deploy/ha/README.md)를 준비하고 모든 gateway에 같은 cursor 키, source 설정과 인증 주체를 배포한다. 공유 JWT와 온라인 정책 갱신은 #51의 후속 작업이다.

```sh
cargo build --features ha --bin gateway
target/debug/gateway --ha-kafka deploy/local/ha/gateway.json --ha-source deploy/local/ha/source.json --ha-cursor-keys deploy/local/ha/cursor-keys.json --auth-file deploy/local/ha/auth.json --addr 127.0.0.1:8080 --http-addr 127.0.0.1:8081
npm ci --prefix edge --ignore-scripts
node examples/ha-client.mjs --urls ws://127.0.0.1:8080/experimental/ha/ws --token-file deploy/local/ha/client-token --state /tmp/wts-client-state.json --mode state --topics /topic/v1/kr/stock/trade/A005930,/topic/v1/us/stock/trade/US19990122001
```

토픽은 `source.json`에 설정된 실제 목록으로 바꾼다. 개발 설정 생성기가 만드는 키와 토큰은 gitignore된 파일에만 두며 기존 파일을 자동 갱신하지 않는다. 만료되면 개발 환경을 새로 준비하거나 명시적으로 교체한다. 운영에서는 시크릿 공급자가 모든 gateway에 같은 키를 제공해야 한다. CLI에 토큰을 직접 넣지 않는다. 외부 주소는 `wss:`로 연결한다.

## 연결과 시작 위치

경로는 `/experimental/ha/ws`, subprotocol은 `wts.ha.v1`이다. query string은 허용하지 않는다. 먼저 `{"type":"connect","token":"Bearer <token>"}`을 보내고 `connected`를 받은 뒤 `resume`을 보낸다. 각 단계의 입력 마감은 10초다. 토픽은 정렬된 중복 없는 고정 목록이며 변경하려면 새 연결을 연다.

```json
{"type":"resume","request_id":"first","topics":["/topic/v1/kr/stock/trade/A005930"],"mode":"events","start_at":{"log_id":"demo-log","partition":0,"offset":"123"}}
```

`events`의 최초 `start_at`은 포함 시작이다. 범위는 broker의 보관 시작 위치부터 Last Stable Offset L까지이며 L 자체는 tail 시작으로 허용한다. 이후에는 `start_at` 대신 마지막 영속화된 `cursor`를 보낸다. cursor가 가리키는 위치는 제외 시작이다. 두 필드를 함께 보내거나 events에서 둘 다 생략하면 거절한다. 숫자 offset, 다른 log/partition, 허용 범위 밖 위치도 거절한다. 커서가 gateway보다 앞서면 제한된 시간 동안 catch-up을 기다리며 replay와 live 모두 요청 위치 미만을 제외한다.

`state`의 최초 요청은 cursor와 start_at을 모두 생략한다. 고정된 최신 상태와 no-data 토픽을 snapshot으로 받고 이어지는 이벤트를 적용한다. state도 cursor가 있으면 저장된 상태 다음 이벤트부터 재개한다. events 요청을 snapshot으로 자동 대체하지 않는다.

## 프레임과 영속화

`event`에는 원래 체결 body, `event_id`, topic과 `position`을 담는다. 64비트 위치와 원본 순번은 문자열이다. `source-status`는 공급자의 연결과 시장 상태이며 거래가 없는 휴장도 구분한다. `caught-up`은 해당 replay 경계까지 전송했음을 뜻하며 클라이언트의 적용 ACK가 아니다.

snapshot은 최대 512KiB이며 `snapshot-begin`, 32KiB 원본 바이트 단위의 base64 `snapshot-chunk`, SHA-256을 포함한 `snapshot-end`로 보낸다. 각 프레임은 48KiB 이하이다. 거래 body는 JSON 확장을 포함해 46KiB 이하여야 하며 초과하면 broker commit 전에 거절한다. hub에서 snapshot 경계 C를 고정하고 같은 명령으로 live buffer를 등록하므로 중간 이벤트를 놓치지 않는다. snapshot을 쓰지 않는 재개는 로그를 C까지 읽은 다음 등록된 live buffer를 소비한다. 클라이언트는 조각 수, 전체 크기, checksum과 end를 검증하고 다음 checkpoint를 받은 뒤 상태와 cursor를 함께 영속화한다.

`checkpoint`는 첫 이벤트 직후와 이후 100건, 256KiB 또는 250ms마다 발행한다. replay 한도에 도달하면 마지막 처리 위치의 checkpoint를 전송할 제어 프레임 예산을 남긴다. cursor에는 log, partition, 위치, 토픽 scope, 인증 주체와 24시간 만료를 묶고 HMAC-SHA256으로 서명한다. 키 ID에 해당하는 검증 키를 기존 커서의 유효 기간 동안 보관해야 하며 현재 키가 새 커서의 전체 수명 동안 유효하지 않으면 발급을 거절한다.

예제 클라이언트는 상태와 cursor를 같은 임시 파일에 쓰고 fsync, rename, 디렉터리 fsync가 끝난 뒤 checkpoint 적용을 인정한다. 파일 하나는 클라이언트 프로세스 하나만 소유해야 한다. 이벤트 ID의 원본 순번으로 중복 적용을 막으며 정확성 비교용 적용 개수와 ID 해시 체인을 함께 저장한다. 저장 전 죽으면 마지막 저장 위치부터 재전송받는다. 체결 이력 전체의 영구 보관을 제공하는 예제는 아니다.

## 제한과 실패

기본 HA 연결 한도는 10,000이다. replay worker 32개, 대기 요청 128개, 연결별 live buffer 1MiB, replay 요청 10,000개 레코드 또는 10MiB 또는 10초로 제한한다. 마지막 1초와 16KiB는 제어 프레임에 남긴다. 대기 중에는 live buffer를 만들지 않는다. worker가 취소되어도 blocking consumer가 끝날 때까지 permit을 반환하지 않는다. 고정 scope 밖 거래는 live 큐에 넣지 않고 마지막 위치와 구독 데이터 순번만 coalesce한다. 앞선 구독 데이터를 모두 전송해야 해당 위치의 checkpoint를 보낼 수 있다. 느린 연결만 종료하며 다른 연결을 기다리게 하지 않는다.

서버와 클라이언트는 1초 heartbeat, 3초 수신 deadline을 쓴다. source가 stale이면 오류를 알리고 닫는다. 예제는 첫 재접속에 최대 500ms jitter를 적용하고 이후 실패에 최대 30초 지수 지연을 쓴다. replay-limit 때문에 동일 위치에서 진행하지 못한 재시도는 3회에서 멈춘다. 첫 checkpoint 이전의 일시적인 연결 실패는 재시도하며 재개 위치를 바꾸지 않는다. 진행 여부는 재서명된 cursor 문자열이 아니라 영속화한 offset 증가로 판단한다. `invalid-cursor`, `cursor-expired`, `start-out-of-range`, scope 및 인증 오류는 자동으로 다른 시작점으로 이동하지 않는다.

`cargo test --features ha`, `npm test --prefix edge`, `node --test examples/ha-state.test.mjs`가 계약 검증과 영속화 경계를 검사한다. `deploy/ha/smoke.sh`는 실제 Kafka의 committed 입력을 기준으로 10,040건의 체결 ID 순서, 해시와 최종 상태를 비교하며 replay 제한, gateway 변경, checkpoint 직전 프로세스 SIGKILL, snapshot 전송 중 SIGKILL과 범위 오류를 시험한다. 단일 호스트 기능 시험이며 실제 LB 및 여러 장애 도메인의 복구 SLO 검증은 #52와 #53에서 수행한다.

클라이언트 적용 callback이 실패하면 해당 연결의 적용 실패를 고정하고 이미 대기 중인 후속 checkpoint도 저장하지 않는다. snapshot 전송 중 live 큐가 가득 차도 제한 시간 안에 snapshot-end와 그 경계의 checkpoint까지 전송한 뒤 replay-limit로 종료한다. 이 경우 caught-up은 보내지 않는다. Kafka reader는 생성부터 모든 오류, 취소와 종료까지 같은 blocking 작업이 소유하며 reader 종료 뒤 worker permit을 반환한다. 연결별 freshness 감시는 hub가 게시한 공유 상태를 읽으므로 연결 수에 비례하는 actor 조회를 만들지 않는다.
