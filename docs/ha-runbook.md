# HA 운영 확인과 복구

HA 기능은 `--features ha` 경로다. 기존 단일 프로세스 성능 수치를 HA 용량으로 사용하지 않는다. `bench/ha/README.md`의 committed oracle와 `docs/ha-capacity.md`의 실제 측정 범위를 함께 확인한다.

## 관측

관리 endpoint는 gateway의 loopback에만 연다. `/ha-metrics`는 Prometheus text 형식이고 기존 `/metrics` JSON 형식은 유지한다. `settings.metricsNamespace`를 지정하면 metrics 전용 sidecar가 `/metrics` GET만 9090에서 제공한다. `/drain`과 인증 정보는 노출하지 않는다. CNI가 NetworkPolicy를 집행하는지 실제로 검증하고 해당 namespace의 Prometheus에서만 수집한다. 기본 수집 설정과 알림 예제는 `deploy/k8s/ha/observability/`에 있다.

Ready 수, source와 broker 관측 나이, Kafka 위치 lag, replay worker와 대기 수, replay byte, checkpoint, slow client, drain 경과 시간, 정책 freshness를 수집한다. Kafka 위치 lag는 transaction marker를 포함하므로 체결 건수나 초 단위 지연으로 해석하지 않는다. 프로세스가 살아 있어도 source가 멈추거나 정책이 만료되면 Ready는 false다. 휴장 상태에서도 committed source status가 갱신되는지 확인한다. scrape 누락을 정상으로 취급하지 않는다.

Producer fencing은 프로세스가 종료되는 사건이므로 scrape 직전 사라질 수 있다. `ha_producer_stopped`의 `reason=producer-fenced` 구조 로그를 지속 로그 수집기로 보관하고 Loki count_over_time으로 측정한다. 제공한 Loki rule은 수집기가 `namespace`, `component=feed` label을 붙인다는 전제다. token, credential, cursor, 실제 체결 payload를 log label이나 본문에 넣지 않는다. 계획된 feed 교체 때 한 번 발생한 fencing과 반복 fencing을 구분한다.

## 조치

| 관측 | 조치 |
| --- | --- |
| Ready 3개 미만 또는 metrics 수집 불가 | 새 rollout을 멈추고 현재 slot과 terminating Pod를 확인한다. Ready 2개 미만이면 N-1 가용성 전제가 무너진 상태다. |
| source 또는 broker 관측 3초 초과 | broker quorum, TLS/ACL, feed Lease와 committed status를 확인한다. stale 상태에서 임의로 readiness를 true로 바꾸지 않는다. |
| replay waiters 96 초과 또는 반복 replay-limit | CPU throttling, edge 메모리, client 디스크 지연, checkpoint 전진을 확인한다. 입력과 동시 접속 속도를 줄여 원인을 분리하고 무제한 queue로 변경하지 않는다. |
| drain 40초 초과 | 다음 slot 변경을 멈춘다. native broker 호출, replica task 종료와 sidecar 상태를 확인한다. 60초 grace 이후 강제 종료가 발생했으면 client oracle로 복구를 확인한다. |
| policy freshness 만료 | 공급자 TLS, 자격 증명, version rollback과 subject generation을 확인한다. 오래된 정책의 유효 시간을 임의 연장하지 않는다. |
| 반복 fencing 또는 writer 두 개 | source 입력을 멈추고 Lease 소유권과 transactional.id를 확인한다. 기존 committed checkpoint로만 복구하며 수신 여부가 불명확한 마지막 batch는 같은 ID로 replay한다. |
| broker quorum 부족 | 임의 초기화나 PVC 삭제 없이 quorum과 ISR을 복구한다. 가용성 5초 목표를 적용하지 않고 복구 후 committed oracle를 수행한다. |

## 배포 실패

드라이버의 journal을 보존하고 같은 plan으로 재시작한다. 실패한 slot을 복구할 때 다른 두 slot이 안정적으로 Ready여야 한다. rollback 역시 각 slot의 안정화와 client oracle를 확인한다. ConfigMap journal이 손상됐으면 자동 초기화하지 말고 현재 Deployment template과 이전 설정을 대조한다. 강제 종료 후 남은 lock은 이전 driver 프로세스가 실제로 종료된 것을 확인한 뒤에만 제거한다. namespace의 gateway 전체에 일괄 apply하거나 GitOps로 동시에 덮어쓰지 않는다.

## 운영 승인 전 필요한 증거

실제 외부 LB의 WebSocket upgrade, 인증서, idle timeout, endpoint 제거 지연, drain과 재접속 p99를 측정한다. 서로 다른 물리 장애 도메인과 zone 내부 surge 노드, N-1 및 4 Pod 순간 용량을 확보하고 단일 도메인 손실을 시험한다. CNI allow/deny, 스토리지 복구, 실제 N/N-1 release 혼합과 rollback 결과도 남긴다. 단일 호스트 kind 결과로 이 조건을 완료했다고 판정하지 않는다.

Kafka 클라이언트 오류는 native 원문 대신 고정 facility 분류와 오류 enum 코드만 기록한다. `PartitionEOF`는 현재 committed 경계에 도달했다는 정상 소비 상태이므로 오류 로그에서 제외한다. 인증 설정, 토큰과 payload는 이 로그에 기록하지 않는다.

`prometheus-rules.yaml`의 `wts_ha_expected_namespace`는 discovery와 독립된 감시 대상 목록이다. 기본값은 `wts-ha`이며 실제 배포 namespace로 바꾸거나 namespace마다 같은 record를 다른 label로 추가한다. 감시 대상의 모든 endpoint가 없어져도 다른 namespace의 정상 `up` 때문에 알림이 사라지지 않는다. `namespace`를 바꾸거나 추가하면 `prometheus-rules.test.yaml`의 `input_series`와 `exp_labels`도 함께 수정하고 추가 namespace에서 발생할 누락 알림을 기대 결과에 포함한다. 규칙을 적용하기 전에 `promtool test rules deploy/k8s/ha/observability/prometheus-rules.test.yaml`을 실행한다.

이 테스트는 일곱 Prometheus 알림의 임계값 경계, `for` 유지 시간 전후와 정상 복구를 합성 시계열로 확인한다. Ready 집계의 namespace 분리, 짧은 Ready 감소 뒤 유지 시간 초기화, 일부 수집 실패와 endpoint 전체 소실 및 재발견, source와 broker 각각의 만료, replay counter 리셋과 5분 관측창의 만료도 포함한다. 테스트 평가 간격은 가장 짧은 `for`와 같은 5초다. 실제 배포에서는 Prometheus의 rule 평가 간격과 scrape 간격 때문에 알림 시각이 달라질 수 있다.

CI와 같은 고정 이미지로 로컬에서 확인하려면 저장소 루트에서 아래 명령을 실행한다. 외부 수신처로 통지하지 않으며, 이 검증은 Alertmanager 전달, Loki fencing 규칙, 실제 수집기 장애와 운영 환경 장애 주입을 검증하지 않는다. 해당 통지 경로는 [관측 알림 이슈 #76](https://github.com/yceffort/ws-trade-server/issues/76)의 후속 작업이다.

```sh
docker run --rm --entrypoint /bin/promtool -v "$PWD/deploy/k8s/ha/observability:/rules:ro" -w /rules prom/prometheus@sha256:63805ebb8d2b3920190daf1cb14a60871b16fd38bed42b857a3182bc621f4996 test rules prometheus-rules.test.yaml
```

서버의 admission 상태 감시와 연결별 freshness 감시는 같은 hub 상태 watch를 사용한다. 상태를 읽기 위한 actor 요청의 대기 시간을 source 장애로 판정하지 않으며, 실제 관측 시각과 lag, 공유 인증 및 cursor key 준비 상태는 계속 검사한다.
