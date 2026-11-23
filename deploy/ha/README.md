# 복제 로그 개발 환경

[#47](https://github.com/yceffort/ws-trade-server/issues/47)의 실험 구현이다. 기존 `feed`와 `gateway`는 계속 mmap을 기본으로 사용한다. `ha` feature는 Kafka 4.1.1과 rdkafka 0.39.0 기반의 별도 `ha-feed`를 빌드한다. 이 디렉터리의 단일 호스트 Docker 구성은 장애 기능 시험용이며 운영 HA 배치나 클라이언트 무손실을 보장하지 않는다.

## 실행

Docker, Node.js 24, OpenSSL, Rust와 C 빌드 도구가 필요하다. 모든 명령은 저장소 루트에서 실행한다. host port 29091~29093을 사용한다.

```sh
node deploy/ha/prepare.js
export COMPOSE_PROJECT_NAME=wts-ha-dev
docker compose -f deploy/ha/compose.yaml up -d --wait --wait-timeout 180
deploy/ha/bootstrap.sh
cargo build --locked --features ha --bins
target/debug/ha-feed init --kafka deploy/local/ha/feed.json --source deploy/local/ha/source.json
node deploy/ha/dev-lease.js
```

다른 터미널에서 후보 두 개를 실행한다. `dev-lease.js`는 resourceVersion 비교 갱신을 제공하는 로컬 시험 서버다. 실제 Kubernetes API나 HA 인증 공급자를 대신하는 운영 서비스가 아니다.

```sh
target/debug/ha-feed run --kafka deploy/local/ha/feed.json --source deploy/local/ha/source.json --lease deploy/local/ha/lease-demo-a.json
target/debug/ha-feed run --kafka deploy/local/ha/feed.json --source deploy/local/ha/source.json --lease deploy/local/ha/lease-demo-b.json
```

`init`는 빈 logical shard를 명시적으로 생성하며 기존 상태를 덮어쓰지 않는다. `run`은 checkpoint가 없거나 source identity, epoch, topic 설정이 달라지면 실패한다. 시장이 닫힌 상태는 `--market-open false`로 실행한다. 거래 없이도 매초 source 상태를 기록한다. `--batches N`은 제한된 횟수만 발행한다. `ha-feed audit --kafka deploy/local/ha/feed.json --source deploy/local/ha/source.json`은 보관된 체결 순서와 checkpoint의 전체 최신 상태를 대조한다. 감사는 최대 100,000건과 30초로 제한하며 이미 만료된 원본 구간을 검증했다고 표시하지 않는다.

생성한 인증서는 7일 유효한 개발 전용 CA로 서명한다. 비밀번호와 개인 키는 출력하지 않고 git에서 제외한 `deploy/local/ha/`에 저장한다. 디렉터리는 소유자만 접근하고, broker에 필요한 파일만 read-only로 mount한다. CA 개인 키는 mount하지 않는다. 인증 자료를 갱신하려면 기존 개발 클러스터를 종료하고 운영 데이터와 구분한 뒤 준비 디렉터리를 새로 만든다. 기존 volume과 다른 인증 자료를 무작정 조합하지 않는다.

`feed.json`의 `timeout_ms`는 30초, 다른 역할은 10초다. 이 값은 메시지 전달과 transaction 호출의 기한이고, broker 요청 하나의 socket timeout은 어느 역할이든 10초를 넘기지 않는다. broker 한 대를 예고 없이 잃으면 controller가 fence할 때까지 commit이 멈추고, controller까지 함께 잃으면 10초를 넘겨 feed가 재시작했다([#94](https://github.com/yceffort/ws-trade-server/issues/94)). 이 값이 생기기 전에 준비한 디렉터리는 자동으로 바꾸지 않으므로 `feed.json`의 값을 직접 30000으로 고친다.

## 데이터와 복구 경계

세 broker는 TLS와 SASL/PLAIN을 사용한다. controller 연결은 상호 TLS다. 데이터, snapshot, checkpoint 및 transaction 내부 토픽은 복제 3, 최소 ISR 2이며 unclean leader election은 꺼져 있다. feed만 세 토픽의 쓰기와 고정 transaction ID를 사용한다. gateway는 체결 및 snapshot 읽기만 가능하고 producer checkpoint는 읽지 못한다. client 설정에서 TLS 검증을 끄거나 isolation을 바꿀 수 없다. 각 reader는 partition 0을 직접 assign하며 자동 group 분배와 offset commit을 사용하지 않는다.

하나의 transaction에 최대 100건 체결, source 상태, 완전한 snapshot과 전체 checkpoint를 기록한다. 첫 구현은 topic 두 개의 상태가 작으므로 설계 초안의 10초 주기 대신 매 transaction에 snapshot을 쓴다. 이 선택으로 snapshot 생성 주기를 따로 복구할 필요가 없지만 쓰기와 compaction 비용이 늘어난다. snapshot의 `through`는 source 상태까지 포함한 원본 로그 경계다. Kafka 제어 레코드와 abort 때문에 offset에 공백이 있어도 체결 유실로 판정하지 않는다.

합성 generator의 난수, 논리 시계, symbol 순서, 가격, 누적 수량과 금액을 정수 상태로 복원한다. source heartbeat의 실제 수신 시각은 거래 시각과 별개다. checkpoint에는 영구 provider/stream identity, 설정 hash, 마지막 원본 순번, 최근 1,024개 입력의 ID와 payload hash, 창 경계와 hash schema를 저장한다. hash는 원본 protobuf bytes를 포함하므로 알 수 없는 protobuf 필드가 달라져도 충돌이다. 재수신 시 달라질 수 있는 수신 시각은 제외한다. snapshot과 checkpoint는 각각 512KiB를 넘으면 실패한다.

외부 입력은 `ExternalSource`의 연속 원본 순번과 재전송을 제공해야 한다. `pump_external`은 공백이면 마지막 committed 다음부터 재요청하고, commit 이후에만 ACK한다. 같은 ID와 같은 본문의 재전송은 새 체결을 만들지 않고 ACK만 재확인한다. 다른 본문, 검증 창 밖 입력, 예고 없는 epoch 변경은 ACK 없이 중단한다. 실제 증권사 연결은 구현하지 않았으며 재전송 없는 source의 장애 구간 무손실을 주장하지 않는다.

Lease는 후보 간 인계를 조정하고 별도 스레드에서 갱신한다. 실패나 갱신 기한 초과 시 쓰기를 멈춘다. Kafka의 고정 `wts-{log_id}-trade-0` transaction ID가 stale producer를 fence하는 최종 경계다. commit 응답이 불명확하면 해당 Publisher를 다시 사용하지 않고 새 프로세스에서 fencing 이후 `read_committed` checkpoint를 읽는다. 같은 프로세스가 재초기화를 반복하며 새 leader를 fence하지 않는다. [rdkafka transaction API](https://docs.rs/rdkafka/0.39.0/rdkafka/producer/trait.Producer.html)와 [Kubernetes Lease API](https://kubernetes.io/docs/reference/kubernetes-api/coordination/lease-v1/)를 따른다.

개발 체결 로그는 기본으로 `retention.ms=86400000`과 `retention.bytes=1073741824`를 함께 사용한다. 목표 처리량에서 1GiB는 약 5.2시간이면 차므로 24시간 보관을 보장하지 않는다([용량 기록](../../docs/ha-capacity.md#체결-로그-보관-용량-2026-11-22)). `WTS_HA_RETENTION_MS`와 `WTS_HA_RETENTION_BYTES`로 바꿀 수 있고, `bootstrap.sh`는 이미 있는 토픽에도 같은 값을 적용한다. snapshot/checkpoint는 compact 토픽이며 tombstone을 쓰지 않는다. segment는 16MiB 또는 60초에 roll하고 dirty 비율 0.1, 최대 compaction lag 120초를 설정한다. 기본 최대 10 transaction/초와 checkpoint 512KiB 상한에서 cleaner 비용을 제한하는 개발 설정이며 운영 디스크 용량은 별도로 산정한다. 복원은 tail 8개 위치부터 16배씩 확장해 처음 committed 값을 찾은 뒤 이진 탐색으로 마지막 위치를 좁힌다. 넓어진 구간 전체를 읽지 않으며 토픽에는 단일 shard 키만 허용한다. 조회 시간은 설정한 timeout, 실제 읽는 payload는 16MiB로 제한한다. aborted 레코드가 많아도 고정 offset 개수에서 탐색을 중단하지 않는다. 제한 안에서 복원하지 못하면 빈 shard로 간주하지 않고 실패한다. 빈 결과를 반환하기 전에 최신 물리적 경계를 다시 조회해 compaction 중 교체된 값을 놓치지 않는다. commit marker의 전파 지연을 빈 checkpoint로 오인하지 않도록 물리적 경계를 고정하고 read-committed 가시화를 확인한다. 범위 밖 offset, 잘못된 schema와 identity는 자동으로 latest로 이동하지 않고 오류로 처리한다.

## 검증

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo clippy --features ha --all-targets -- -D warnings
cargo test --features ha
deploy/ha/smoke.sh
```

`smoke.sh`는 고유 이름의 자체 Compose 프로젝트를 만들고 종료할 때 그 프로젝트의 volume만 제거한다. 동일 host port를 쓰는 수동 개발 클러스터는 먼저 내려야 한다. 합성 상태 복원, producer fencing, commit 전 Lease 상실 시 abort, read-only ACL, SIGSTOP 후 standby 인계와 stale writer 재개, API 단절, broker 1대 중단, 체결 partition leader의 네트워크 단절 중 기존 클라이언트 유지와 committed 대조(`broker-isolation.js`), 보관 경계 밖 cursor의 명시적 만료(`retention-boundary.js`), 과반수 네트워크 단절, 전체 broker 재시작을 검사한다. 별도 빈 데이터셋에서 외부 입력 공백, commit 뒤 ACK 유실, 재전송과 1,024건 창 만료를 검사한다. 실제 broker commit 성공 직후 응답을 잃는 경우는 시험 빌드의 Writer 경계에서 오류를 주입해 검증한다. Publisher의 메모리가 전진하지 않은 상태에서 재시작하고, committed checkpoint 복원과 동일 ID 재전송 결과를 실제 로그와 대조한다. 네트워크 패킷을 유실시키는 시험과는 구분한다. CI의 `HA` workflow도 같은 시험을 실행한다. 출력과 artifact에는 수치 결과만 저장하고 인증 자료는 올리지 않는다.

이 시험의 Kafka는 실제 복제와 transaction을 사용하지만 하나의 Docker 호스트에서 동작한다. gateway 재접속은 #49에서 다뤘고, 실제 장애 도메인과 LoadBalancer는 #90, 부하 SLO는 #91의 별도 검증 대상이다.
