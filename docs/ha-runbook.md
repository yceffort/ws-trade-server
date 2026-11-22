# HA 운영 확인과 복구

HA 기능은 `--features ha` 경로다. 기존 단일 프로세스 성능 수치를 HA 용량으로 사용하지 않는다. `bench/ha/README.md`의 committed oracle와 `docs/ha-capacity.md`의 실제 측정 범위를 함께 확인한다.

## 관측

관리 endpoint는 gateway의 loopback에만 연다. `/ha-metrics`는 Prometheus text 형식이고 기존 `/metrics` JSON 형식은 유지한다. `/ha-metrics`에는 일반 gateway 지표(`GET /prometheus`와 같은 `wts_` 지표)도 함께 담으므로 같은 sidecar 하나로 연결 수, 송신 큐, 로그 폐기와 종료 사유를 수집한다. `settings.metricsNamespace`를 지정하면 metrics 전용 sidecar가 `/metrics` GET만 9090에서 제공한다. `/drain`과 인증 정보는 노출하지 않는다. CNI가 NetworkPolicy를 집행하는지 실제로 검증하고 해당 namespace의 Prometheus에서만 수집한다. 기본 수집 설정과 알림 예제는 `deploy/k8s/ha/observability/`에 있고, 같은 디렉터리의 [개발용 관측 스택](../deploy/k8s/ha/observability/README.md)이 Prometheus, Loki, Alloy, Grafana를 함께 띄운다.

Ready 수, source와 broker 관측 나이, Kafka 위치 lag, replay worker와 대기 수, replay byte, checkpoint, slow client, drain 경과 시간, 정책 freshness, 고정 사유별 HA 연결 종료 수(`wts_ha_closed_total`)를 수집한다. `wts_ha_catch_up_seconds`는 resume 처리를 시작한 때부터 `caught-up`을 보낼 때까지(replay worker 대기와 snapshot 또는 replay 전송 포함), `wts_ha_replay_seconds`는 replay를 시작한 때부터 live 구독을 붙인 시점(`follow_replica`가 돌려준 위치)의 끝에 닿을 때까지의 시간이다. 그래서 요청을 받은 뒤 그 시점까지 적용된 레코드도 replay 구간에 들어간다. 경계는 5ms부터 10초(replay 기한)까지 11개다. 두 지표 모두 `caught-up`까지 보낸 resume만 세며, 클라이언트가 적용한 시각이 아니다. Kafka 위치 lag는 transaction marker를 포함하므로 체결 건수나 초 단위 지연으로 해석하지 않는다. 프로세스가 살아 있어도 source가 멈추거나 정책이 만료되면 Ready는 false다. 휴장 상태에서도 committed source status가 갱신되는지 확인한다. scrape 누락을 정상으로 취급하지 않는다.

Producer fencing은 프로세스가 종료되는 사건이므로 scrape 직전 사라질 수 있다. `ha_producer_stopped`의 `reason=producer-fenced` 구조 로그를 지속 로그 수집기로 보관하고 Loki count_over_time으로 측정한다. 제공한 Loki rule은 수집기가 `namespace`, `component=feed` label을 붙인다는 전제다. token, credential, cursor, 실제 체결 payload를 log label이나 본문에 넣지 않는다. 계획된 feed 교체 때 한 번 발생한 fencing과 반복 fencing을 구분한다.

체결 로그의 실제 보관 범위는 `ha-oracle retention <gateway.json> <source.json> 86400`으로 조회한다. 보관 시작 위치와 남아 있는 가장 오래된 committed 레코드의 시각을 출력하며, 로그가 지워졌는데 24시간보다 짧으면 종료 코드 2를 낸다. 주기적으로 실행해 2를 목표 미달로 본다.

## 조치

| 관측 | 조치 |
| --- | --- |
| Ready 3개 미만 또는 metrics 수집 불가 | 새 rollout을 멈추고 현재 slot과 terminating Pod를 확인한다. Ready 2개 미만이면 N-1 가용성 전제가 무너진 상태다. |
| source 또는 broker 관측 3초 초과 | broker quorum, TLS/ACL, feed Lease와 committed status를 확인한다. stale 상태에서 임의로 readiness를 true로 바꾸지 않는다. |
| replay waiters 96 초과 또는 반복 replay-limit | CPU throttling, edge 메모리, client 디스크 지연, checkpoint 전진을 확인한다. 입력과 동시 접속 속도를 줄여 원인을 분리하고 무제한 queue로 변경하지 않는다. |
| drain 40초 초과 | 다음 slot 변경을 멈춘다. native broker 호출, replica task 종료와 sidecar 상태를 확인한다. 60초 grace 이후 강제 종료가 발생했으면 client oracle로 복구를 확인한다. |
| policy freshness 만료 | 공급자 TLS, 자격 증명, version rollback과 subject generation을 확인한다. 오래된 정책의 유효 시간을 임의 연장하지 않는다. |
| 반복 fencing 또는 writer 두 개 | source 입력을 멈추고 Lease 소유권과 transactional.id를 확인한다. 기존 committed checkpoint로만 복구하며 수신 여부가 불명확한 마지막 batch는 같은 ID로 replay한다. |
| 보관 범위 목표 미달(`ha-oracle retention` 종료 코드 2) | 체결 증가량과 `retention.bytes`를 [용량 기록](ha-capacity.md#체결-로그-보관-용량-2026-11-22)과 비교한다. 늘릴 때 Compose는 `WTS_HA_RETENTION_BYTES`를 주고 `deploy/ha/bootstrap.sh`를 다시 실행한다. 이 스크립트는 기존 토픽에도 같은 값을 적용한다. k8s는 settings의 `retentionBytes`를 바꾸고, 운용 중인 클러스터에는 `deploy/k8s/ha/README.md`의 `kafka-configs --alter` 절차로 적용한다. bootstrap Job은 초기화를 포함하므로 다시 실행하지 않는다. 이미 지워진 구간은 돌아오지 않으며 그 앞 cursor의 클라이언트는 `cursor-expired`를 받는다. |
| broker quorum 부족 | 임의 초기화나 PVC 삭제 없이 quorum과 ISR을 복구한다. 가용성 5초 목표를 적용하지 않고 복구 후 committed oracle를 수행한다. |

## 배포 실패

드라이버의 journal을 보존하고 같은 plan으로 재시작한다. 실패한 slot을 복구할 때 다른 두 slot이 안정적으로 Ready여야 한다. rollback 역시 각 slot의 안정화와 client oracle를 확인한다. ConfigMap journal이 손상됐으면 자동 초기화하지 말고 현재 Deployment template과 이전 설정을 대조한다. 강제 종료 후 남은 lock은 이전 driver 프로세스가 실제로 종료된 것을 확인한 뒤에만 제거한다. namespace의 gateway 전체에 일괄 apply하거나 GitOps로 동시에 덮어쓰지 않는다.

## 운영 승인 전 필요한 증거

실제 외부 LB의 WebSocket upgrade, 인증서, idle timeout, endpoint 제거 지연, drain과 재접속 p99를 측정한다. 서로 다른 물리 장애 도메인과 zone 내부 surge 노드, N-1 및 4 Pod 순간 용량을 확보하고 단일 도메인 손실을 시험한다. CNI allow/deny, 스토리지 복구, 실제 N/N-1 release 혼합과 rollback 결과도 남긴다. 단일 호스트 kind 결과로 이 조건을 완료했다고 판정하지 않는다. 2026-11-22의 AWS 실측은 내부용 NLB의 WebSocket upgrade와 인증서, target 등록과 해제 지연, drain과 계획 재접속 p99, CNI allow와 deny, AZ 하나의 서브넷 차단과 인스턴스 종료, N-1을 쟀다([용량 시험 기록](ha-capacity.md)). 인터넷에 노출된 LB 경로, idle timeout에 실제로 닿는 연결, EBS를 포함한 AZ 전체 장애, 스토리지 복구, 서로 다른 release의 N/N-1 혼합은 재지 않았다.

Kafka 클라이언트 오류는 native 원문 대신 고정 facility 분류와 오류 enum 코드만 기록한다. `PartitionEOF`는 현재 committed 경계에 도달했다는 정상 소비 상태이므로 오류 로그에서 제외한다. 인증 설정, 토큰과 payload는 이 로그에 기록하지 않는다.

`prometheus-rules.yaml`의 `wts_ha_expected_namespace`는 discovery와 독립된 감시 대상 목록이다. 기본값은 `wts-ha`이며 실제 배포 namespace로 바꾸거나 namespace마다 같은 record를 다른 label로 추가한다. 감시 대상의 모든 endpoint가 없어져도 다른 namespace의 정상 `up` 때문에 알림이 사라지지 않는다. `namespace`를 바꾸거나 추가하면 `prometheus-rules.test.yaml`의 `input_series`와 `exp_labels`도 함께 수정하고 추가 namespace에서 발생할 누락 알림을 기대 결과에 포함한다. 규칙을 적용하기 전에 `promtool test rules deploy/k8s/ha/observability/prometheus-rules.test.yaml`을 실행한다.

이 테스트는 일곱 Prometheus 알림의 임계값 경계, `for` 유지 시간 전후와 정상 복구를 합성 시계열로 확인한다. Ready 집계의 namespace 분리, 짧은 Ready 감소 뒤 유지 시간 초기화, 일부 수집 실패와 endpoint 전체 소실 및 재발견, source와 broker 각각의 만료, replay counter 리셋과 5분 관측창의 만료도 포함한다. 테스트 평가 간격은 가장 짧은 `for`와 같은 5초다. 실제 배포에서는 Prometheus의 rule 평가 간격과 scrape 간격 때문에 알림 시각이 달라질 수 있다.

CI와 같은 고정 이미지로 로컬에서 확인하려면 저장소 루트에서 아래 명령을 실행한다. 외부 수신처로 통지하지 않으며, 이 검증은 Alertmanager 전달, Loki fencing 규칙, 실제 수집기 장애와 운영 환경 장애 주입을 검증하지 않는다. Alertmanager까지의 통지와 복구 확인은 아래 [알림 발생과 복구 확인](#alert-response)에 있다([#76](https://github.com/yceffort/ws-trade-server/issues/76)).

```sh
docker run --rm --entrypoint /bin/promtool -v "$PWD/deploy/k8s/ha/observability:/rules:ro" -w /rules prom/prometheus@sha256:63805ebb8d2b3920190daf1cb14a60871b16fd38bed42b857a3182bc621f4996 test rules prometheus-rules.test.yaml
```

서버의 admission 상태 감시와 연결별 freshness 감시는 같은 hub 상태 watch를 사용한다. 상태를 읽기 위한 actor 요청의 대기 시간을 source 장애로 판정하지 않으며, 실제 관측 시각과 lag, 공유 인증 및 cursor key 준비 상태는 계속 검사한다.

<a id="alert-response"></a>

## 알림 발생과 복구 확인

[관측 스택](../deploy/k8s/ha/observability/README.md)은 Prometheus와 Loki ruler의 알림을 Alertmanager 0.28.1과 로컬 테스트 수신기로 연결한다. 외부 메일이나 메신저 수신처는 설정하지 않는다. 알림의 `runbook_url`과 `dashboard_url`로 이 절차와 WTS 대시보드에 접근한다. Grafana를 port-forward하지 않는 환경은 `dashboard_url`의 호스트를 해당 내부 Grafana 주소로 바꾼다.

| 알림 | 확인과 대응 | 복구 확인 |
| --- | --- | --- |
| WtsHaReadyReplicasLow | 다음 slot 배포를 멈추고 namespace의 Ready, freshness, Pod 자원을 확인 | Ready 3개 회복 |
| WtsHaMetricsMissing, WtsHaMetricsIncomplete | Service endpoint, sidecar와 scrape 오류 확인. 빈 결과를 Ready로 해석하지 않음 | endpoint 수집과 필수 지표 복구 |
| WtsHaSourceStale | source 또는 broker 관측 3초 초과나 NaN 확인. feed Lease와 broker 연결 조사 | 두 관측값이 유한하고 3초 이하 |
| WtsHaReplicaLag | Kafka 위치 차이 100 초과. replay와 CPU/throttling 확인 | 위치 lag 100 이하. 체결 건수로 해석하지 않음 |
| WtsHaReplayPressure | 대기 96 초과 또는 replay budget 초과. checkpoint 진행과 N-1 용량 조사 | 대기 감소와 5분간 신규 budget 초과 없음 |
| WtsHaDrainOverdue | 40초 넘긴 drain. 다음 slot 변경 중단, 연결 종료와 LB 제거 확인 | drain 완료 |
| WtsHaPolicyStale | 정책 공급자 TLS, version, 만료 확인 | freshness 회복 |
| WtsHaUnexpectedFencing | 5분에 producer-fenced 2건 이상. 배포 annotation과 Lease, 중복 writer 조사 | 5분 창에서 반복 fencing이 빠짐 |
| WtsLogsDropped | 앱 로그 큐 폐기. 로그량과 stderr 소비 속도 확인 | 5분간 새 폐기 없음 |
| WtsCollectorDropped, WtsCollectorRetries | Alloy 전송 실패와 Loki 수신 한도, 저장소 확인. 앱 큐 폐기와 분리 | 5분간 새 폐기 또는 재시도 없음 |
| WtsObservabilityMissing | Alloy, Loki, Alertmanager의 수집 실패나 전체 endpoint 소실 확인 | 예상 service 재발견과 up=1 |
| WtsAlertDeliveryFailed | Prometheus 또는 Alertmanager 전달 실패. 수신기와 네트워크 확인 | 5분간 새 전달 실패 없음 |
| WtsWatchdog | 1분마다 수신되는 생존 신호. 멈추면 관측 스택 바깥에서 확인 | 신호 수신 재개 |

계획된 leader 교체의 단발 fencing은 기록만 하고, 5분 안에 반복되면 계획 여부와 무관하게 조사한다. `lease-lost`와 `producer-fenced`를 같은 사건으로 세지 않는다. 배포 중 일시 Ready 감소는 30초 유지 시간을 적용한다. 계획 배포라고 namespace 전체를 silence하지 않는다. Alertmanager는 namespace와 alertname으로 묶고 4시간 안에 같은 firing을 반복하지 않으며 복구 통지를 보낸다. metrics 소실이 활성인 namespace의 Ready 부족 알림은 중복 억제한다. 수집 중인 다른 Pod의 필수 지표 누락은 별도 장애이므로 억제하지 않는다.

수신기는 최대 100개 통지를 메모리에 보관하고 alertname, namespace, 상태만 남긴다. 요청 제한 64KiB 안에 규칙 annotation과 generatorURL도 들어가도록 Alertmanager는 한 통지에 최대 16개 알림만 보낸다. 긴 Pod 이름과 이 저장소의 가장 긴 식을 넣은 16개 묶음의 발생과 복구를 시험한다. 그룹이 16개를 넘으면 나머지는 webhook에서 생략되므로 전체 알림은 Alertmanager에서 확인한다. 규칙이나 라벨을 확장하면 바이트 한도를 다시 검증한다. 재시작하면 기록을 잃으며 감사 저장소가 아니다. Alertmanager도 이 개발 구성에서는 emptyDir라 재시작 뒤 중복 억제 상태와 silence가 사라질 수 있다. Prometheus가 죽으면 스스로 알릴 수 없고 Alertmanager가 죽으면 같은 경로로 장애를 전달할 수 없다. WtsWatchdog 수신 간격을 관측 스택 외부에서 확인해야 이 공백을 감지할 수 있다. 실제 외부 통지는 사용자가 지정한 대상과 범위에서 별도 설정한다.

```sh
node deploy/k8s/ha/observability/alert-smoke.mjs
kubectl --context CONTEXT -n wts-observability port-forward svc/alert-receiver 18080:8080
```

port-forward를 실행한 터미널은 그대로 두고 다른 터미널에서 수신기를 조회한다.

```sh
curl http://127.0.0.1:18080/notifications
```

첫 명령은 별도 Docker network와 합성 metrics/log만 사용한다. source 만료, 수집 endpoint 중단과 복구, 단발 및 반복 fencing을 주입하고 수신기의 firing/resolved와 중복 억제, namespace 분리를 확인한다. Loki의 실제 5분 관측창 복구를 기다리므로 약 6분 걸린다. 기존 클러스터와 외부 수신처에는 장애를 주입하지 않는다. Prometheus 단위 시험은 임계값 전후, 유지 시간, 복구, counter reset, 두 namespace와 NaN/누락을 별도로 검사한다.
