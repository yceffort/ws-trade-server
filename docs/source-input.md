# 공통 입력 경계

[#46](https://github.com/yceffort/ws-trade-server/issues/46)의 구현이다. 기존 gateway는 계속 로컬 mmap 링을 기본 입력으로 사용한다. `RingAdapter`와 `BrokerAdapter`는 `Hub::input()`이 제공하는 같은 1,024개 명령 큐로 발행한다. 별도 중간 큐나 무제한 태스크를 추가하지 않는다. BrokerAdapter는 검증한 레코드를 hub로 전달하는 라이브러리 어댑터이며 Kafka에 연결하는 consumer가 아니다.

```rust
use std::sync::Arc;
use ws_trade_server::source::{BrokerAdapter, Envelope, Position, ReceivedAtMs, SourceId, SourceSeq};

// hub와 trade는 호출자가 소유한 기존 Hub 및 protobuf Trade다.
let source = SourceId::new("demo-provider".into(), "trades".into())?;
let input = BrokerAdapter::new(hub.input(), "demo-log".into(), vec![source.clone()])?;
let event = Arc::new(Envelope::new(
    "demo-log".into(), source, "epoch-1".into(), SourceSeq(42), trade, ReceivedAtMs(1_000),
)?);
let position = Position::new("demo-log".into(), 0, 100)?;
let applied = input.publish(&event, position).await?;
assert_eq!(applied.envelope.source_seq(), 42);
assert_eq!(applied.position.offset(), 100);
```

이 코드는 호출 형태를 설명하는 조각이다. source sequence는 원본 제공자가 지정한 안정적인 순번이며 `Trade.seq`, broker offset, 연결별 STOMP message-id와 다르다. 호출자는 재시도 때 같은 event를 유지해야 한다. 생성자의 원본 순번과 수신 시각은 SourceSeq와 ReceivedAtMs로 구분한다. producer CLI와 Kafka reader는 [#47 복제 로그](../deploy/ha/README.md)에 구현했다. [gateway 입력 선택과 consumer 수명 관리](ha-gateway.md)는 #48에서 연결했다.

## 식별자와 직렬화

source_id는 provider namespace와 stream ID를 묶는다. log_id, namespace, stream, epoch는 각각 1~96자의 ASCII 영문자, 숫자, 점, 밑줄, 하이픈만 허용한다. event_id는 `provider/stream/epoch/source_seq`이며 구성 요소에 `/`가 없으므로 경계가 모호하지 않다. 다른 제공자의 같은 원본 순번도 서로 다른 이벤트가 된다. 최초 설정은 제공자당 stream 하나이며 BrokerAdapter 생성 시 중복 provider namespace를 거절한다. 설정 객체의 식별자는 생성 후 변경할 수 없다. 영구 namespace와 checkpoint의 대조는 #47의 Publisher에서 수행한다.

envelope JSON은 schema_version 1, log_id, source_id, source_epoch, source_seq, event_id, received_at_ms, trade를 가진다. source_seq와 received_at_ms는 십진 문자열이다. trade는 기존 `proto/feed.proto`의 Trade를 인코딩한 byte 배열이므로 원본 숫자를 JSON 부동소수점으로 바꾸지 않는다. 입력 bytes는 알 수 없는 protobuf 필드까지 그대로 보존하며, 디코딩한 값은 검증과 기존 본문 렌더링에 사용한다. 기존 protobuf 스키마와 `/ws` 본문은 바꾸지 않는다. 원본 체결 발생 시각은 Trade.traded_at_ms에 있다.

Position JSON은 log_id, partition, offset을 가진다. offset도 십진 문자열이며 signed 64비트 broker offset의 비음수 범위만 허용한다. log_id가 envelope나 어댑터 설정과 다르면 발행하지 않는다. 서명 cursor와 클라이언트 replay 규약은 #49의 범위이며 Position 자체는 접근 권한이나 서명 cursor가 아니다.

JSON 숫자로 보낸 sequence/offset, 부호, 앞자리 0, overflow, 지원하지 않는 schema, 위조된 event_id, 등록하지 않은 source는 거절한다. envelope 입력은 128KiB, protobuf Trade는 16KiB로 제한한다. 소수 scale은 렌더링 전에 18 이하인지 확인해 비정상 scale 때문에 큰 문자열을 할당하지 않는다. 종목 코드와 통화는 기존 Trade 검증처럼 비어 있는지만 검사하며 namespace의 문자 제한을 적용하지 않는다. 잘못된 입력의 오류에는 원본 payload나 식별자 값을 넣지 않는다. 이 한도와 인코딩은 관측한 증권사 형식이 아닌 내부 실험 규약이다.

## 대기, 취소와 완료

비동기 발행은 먼저 hub 큐의 자리를 예약한다. 대기 중 future를 취소하면 발행하지 않으며 호출자가 가진 원본 event는 유지된다. 큐에 들어간 뒤 취소하거나 완료 응답을 잃으면 이미 처리됐을 수 있다. 그때 새 ID를 만들어 재시도하지 않는다. `Closed`는 큐 진입 실패, `CompletionLost`는 진입 이후 완료 여부를 확인하지 못한 결과다.

비동기 발행 성공은 hub가 cache 및 fan-out 처리를 수행한 뒤 반환한다. 느린 연결을 끊거나 구독자가 없는 경우에도 hub 처리는 완료될 수 있다. 클라이언트가 메시지를 적용했다거나 broker offset을 durable commit했다는 의미는 아니다. `AppliedRecord`로 같은 envelope와 broker 위치를 호출자에게 돌려주며 broker commit은 이 어댑터가 수행하지 않는다. snapshot과 applied offset을 원자적으로 복원하는 작업은 #48에 남아 있다.

링 브리지는 런타임 밖의 전용 스레드에서 `enqueue_blocking`을 사용한다. 이는 큐 진입까지만 기다린다. hub 종료로 채널이 닫히면 대기 중 발행도 오류로 풀리며 기존 서버 종료 경로가 bridge 스레드를 join한다. 비동기 runtime 안에서는 blocking 메서드를 호출하지 않는다. 여러 발행 future를 제한 없이 spawn하면 호출자 쪽의 메모리를 제한할 수 없으므로 consumer는 발행을 await하거나 별도 유한 worker 수를 사용해야 한다.

## 남은 HA 작업

입력 어댑터만으로 복구를 보장하지 않는다. 복제 로그, transaction/fencing, sequence 공백 및 중복 검증, durable checkpoint는 [#47](../deploy/ha/README.md)에 구현했다. [snapshot 복원과 전체 fan-out](ha-gateway.md)은 #48에서 연결했다. 재접속 cursor는 #49에서 구현한다. 현재 구현만으로 무손실 또는 exactly-once 전송을 주장하지 않는다. 전체 구조와 순서는 [HA 설계](ha-design.md)와 [배포 계획](ha-rollout.md)에 있다.
