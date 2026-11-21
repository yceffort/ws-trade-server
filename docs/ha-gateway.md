# gateway의 복제 로그 입력

[#48](https://github.com/yceffort/ws-trade-server/issues/48)의 구현이다. `ha` feature로 빌드한 gateway는 mmap 대신 Kafka를 입력으로 선택할 수 있다. 각 gateway가 partition 0을 명시적으로 assign해 전체 로그를 읽고, consumer group의 partition 분배나 자동 offset commit을 사용하지 않는다.

```sh
cargo build --features ha --bin gateway
target/debug/gateway --ha-kafka deploy/local/ha/gateway.json --ha-source deploy/local/ha/source.json --addr 127.0.0.1:8080 --http-addr 127.0.0.1:8081
```

먼저 [복제 로그 개발 환경](../deploy/ha/README.md)의 broker와 active `ha-feed`를 실행한다. Kafka와 source 설정은 두 옵션을 함께 지정하며 `--ring`과 동시에 사용할 수 없다. 기존 `/ws`의 STOMP 형식과 로컬 인증 파일 모드를 그대로 사용할 수 있다. 이 연결은 새 HA 재접속 프로토콜이나 공유 서명 인증을 제공하지 않는다. 각각 #49와 #51의 후속 작업이다.

gateway는 완전한 committed snapshot의 경계 B를 검증하고 B 다음부터 읽는다. 데이터가 없는 topic은 `ReplicaView.snapshot.topics`에서 `null`로 유지한다. 최신 체결의 원본 수신 시각을 보존하며 복원 시각을 체결 시각으로 덮어쓰지 않는다. 로그 레코드의 적용, 최신 상태 cache, 기존 구독자 fan-out과 적용 위치는 기존 1,024개 명령 hub의 같은 직렬 처리 단계에서 갱신한다. 적용 응답은 이 단계가 끝난 뒤 반환하며 클라이언트 수신이나 상태 적용의 ACK가 아니다.

복원 후 최초 조회 경계 또는 `read_committed` EOF까지 따라잡아야 준비된다. Kafka의 물리적 offset에는 제어 레코드와 abort 공백이 있으므로 체결의 source sequence를 별도로 검증한다. consumer가 건너뛴 제어 레코드 위치는 hub에 별도 경계 명령으로 반영한다. 같은 offset과 같은 내용은 최근 1,024개 검증 창 안에서 다시 발행하지 않는다. 다른 내용, 원본 순번 공백, 지원하지 않는 schema/identity, retention 밖 위치는 실패로 남고 latest로 자동 이동하지 않는다. 실패 후 재시작은 새 완전 snapshot에서 시작한다.

snapshot과 EVENTS partition의 commit marker 가시성이 다르면 기존 복원 위치를 유지한 채 broker timeout 안에서 LSO를 기다린다. 준비 판단은 `ReplicaView::ready(policy_fresh)`에 둔다. bootstrap과 catch-up 완료, 최근 성공한 broker 관측과 log end 조회, source 연결 상태, heartbeat의 실제 시각 및 단조 시간 경과를 확인한다. 최신 `read_committed` log end를 250ms마다 다시 조회하며 마지막 조회 위치에 대한 허용 offset lag는 100이다. 최초 catch-up은 조회 경계까지 완료해야 하며 이후 제어 레코드와 fetch 사이의 일시적인 위치 차이는 이 범위에서 허용한다. 새 log end는 다음 레코드 적용 전에 hub에 반영한다. metadata의 일시 오류는 성공 시각을 갱신하지 않고 재시도하며 성공한 관측이 3초 이상 없으면 준비되지 않는다. 한도보다 적용 위치가 뒤처지면 최초 catch-up 이후에도 준비 상태가 내려간다. EOF의 제어 레코드 위치는 조회 후 재assign한 읽기에서 확인해 미확인 범위를 건너뛰지 않는다. 초기 신선도 한도는 3초이며 1초를 넘는 미래 source 시각은 거절한다. 거래가 없어도 source heartbeat가 계속 오면 휴장 상태는 정상이다. 정책 신선도는 별도 입력이며 현재 로컬 실행은 로컬 인증 모드의 정책으로 판단한다. 온라인 정책 갱신은 #51, 관리 endpoint 및 신규 연결 admission과 기존 연결의 stale 통지는 #50에서 연결한다. 이 단계의 gateway는 bootstrap이 끝나야 accept loop를 시작한다.

Kafka poll과 metadata 호출은 blocking worker 하나에서 실행하고 hub 명령은 용량 제한 채널을 통해 적용 완료까지 기다린다. 느린 gateway가 다른 gateway의 consumer를 막지 않으며 느린 WebSocket 구독자는 기존 hub의 연결별 큐 상한에 따라 종료된다. 정상 종료는 poll 중단, hub 종료와 worker join을 수행한다.

`cargo test --features ha`는 no-data, 중복 적용, 원자적 cache/offset 갱신, source 공백 및 휴장/오래된 상태/미래 시각의 준비 판단을 검증한다. `deploy/ha/smoke.sh`는 실제 Kafka에서 독립 gateway 두 개의 WebSocket으로 같은 40개 체결을 전부 받고, 늦게 시작한 세 번째 gateway의 상태가 일치하는지 확인한다. source heartbeat를 멈추면 준비 상태가 내려가고 재개하면 복구하는지도 검사한다. 단일 호스트 프로세스 시험이며 실제 Pod/LB 및 장애 도메인 시험은 #52와 #53에서 수행한다.

같은 partition의 읽기 위치와 EOF 관측 경계를 갱신할 때는 반복 assign 대신 timeout이 있는 seek를 사용한다. CI에서 native assignment 해제 경합의 assertion을 관측했으며, 네 reader의 반복 경계 갱신 시험으로 확인한다. [rdkafka Consumer::seek](https://docs.rs/rdkafka/0.39.0/rdkafka/consumer/trait.Consumer.html#tymethod.seek)의 기존 할당 유지 규약을 따른다.
