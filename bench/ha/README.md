# HA 수용 시험

이 도구는 `read_committed` Kafka 기록을 기준으로 클라이언트가 상태와 cursor를 함께 fsync한 결과를 비교한다. `event_id` 수뿐 아니라 순서, 중복 적용, payload hash도 확인한다. oracle는 요청 시작 offset과 각 클라이언트의 구독 범위 안에서만 비교하며 Kafka transaction marker에 의한 offset 공백을 허용한다. 실제 운영 LB와 물리 장애 도메인 증거는 별도로 검토해야 하므로 보고서의 `production_slo_proven`은 항상 false다.

## 준비와 실행

`deploy/k8s/ha/README.md`에 따라 전용 `wts-ha*` namespace에 broker 3개, feed 2개, gateway 3개와 공유 인증을 준비한다. broker의 기존 PVC와 log_id를 보존하며 bootstrap Job을 재실행하지 않는다. 부하 시험은 해당 namespace에만 적용한다. 부하 클라이언트는 `wts-dev-client-<version>` Secret의 `token`과 `ca.crt`, gateway의 Kafka 읽기 전용 자격 증명을 사용한다. token 값에는 `Bearer ` 접두사가 있어야 한다. 운영 token을 저장소나 artifact에 넣지 않는다.

```sh
docker build -f deploy/ha/Dockerfile -t ws-trade-ha:acceptance .
docker build -f edge/Dockerfile -t ws-trade-edge:acceptance .
docker build -f bench/ha/Dockerfile --build-arg HA_IMAGE=ws-trade-ha:acceptance --build-arg EDGE_IMAGE=ws-trade-edge:acceptance -t ws-trade-ha-load:acceptance .
# settings의 gatewayImage, edgeImage, loadImage는 접근 가능한 registry의 digest로 고정한다.
node bench/ha/job-run.mjs CONTEXT settings.json options.json ./ha-results
```

아래는 일반 부하 시험용 `options.json` 예시다. 필드를 생략하면 `requireReconnectSlo`는 `true`, `startupIntervalMs`는 `10`이다. Service 주소를 사용하는 시험과 실제 외부 LB URL을 사용하는 시험을 별도로 남긴다. 인증서의 SAN과 URL이 일치해야 한다. 측정용 부하 Pod는 서버와 분리된 노드에 두는 것이 좋으며, CPU throttling과 client event loop 지연도 결과와 함께 확인한다. `nodeSelector`와 `tolerations`를 주면 부하 Pod를 taint를 건 전용 노드에 고정한다. 두 값은 Job의 Pod spec에만 들어가고 부하 설정에는 남지 않는다.

```json
{"clients":1000,"workers":4,"durationSeconds":600,"startupIntervalMs":20,"requireReconnectSlo":false}
```

`run.mjs`는 첫 committed 위치를 고정한 뒤 클라이언트들을 시작하고, 초기 catch-up을 60초 안에 완료해야 측정을 시작한다. 첫 접속 분산은 재접속 폭주 시험의 대체가 아니다. 재접속은 클라이언트의 소켓 종료 감지부터 다음 caught-up 직전까지 측정하며, 이전 checkpoint의 상태 저장과 cursor 적용을 기다린다. 장애 감지 이전의 네트워크 단절 시간은 이 값에 포함되지 않으므로 `recovery_from_fault_p99_ms`를 함께 본다. 초기 TLS handshake, 재시도 backoff, replay와 디스크 적용 비용이 모두 포함된다. 실행 도중 Worker가 실패하면 합격하지 않는다.

부하 Job은 결과를 `/state/report.json`에 저장하고 수집을 위해 120초 기다린다. `job-run.mjs`는 보고서, Pod 재시작 정보와 gateway metrics를 로컬 출력 디렉터리에 저장한다. 보고서 생성 전 OOM 또는 비정상 종료가 나도 Pod 종료 상태를 수집하며 API 조회까지 실패하면 수집 실패 표시를 남긴다. 보고서를 기다리는 동안의 kubectl 실패는 실행 기한까지 다시 시도하고, 기한까지 계속 실패하면 보고서 없이 실패로 끝난다. 이전 Job이 있으면 자동 삭제하지 않는다. 결과를 보관한 다음 그 전용 namespace의 `wts-ha-load` Job을 지우고 다시 실행한다. `emptyDir`의 상태 파일은 해당 시험 안에서만 durable하며 Pod를 지우면 사라진다. 클라이언트 프로세스 재시작과 디스크 복구는 별도 FileStore 시험으로 검증한다.

## 배포와 rollback 게이트

계획 배포는 `requireReconnectSlo:true`로 실행한다. 초기 catch-up 후 `/state/control.sock`가 생긴 것을 확인하고, 다음 verifyCommand를 기존 rollout plan에 지정한다. `KUBECONFIG`와 context를 명시해야 한다.

```json
{"verifyCommand":["node","/absolute/repo/bench/ha/kube-verify.mjs","CONTEXT","wts-ha"]}
```

`deploy/k8s/ha/rollout.mjs`는 각 슬롯의 안정화 후 부하 Job에 nonce, plan hash, slot을 전달한다. Job은 새 committed 경계를 고정하고 모든 클라이언트가 저장한 적용 결과를 비교한다. 누락, 중복 적용, payload 차이 또는 재접속 p99 5초 초과가 있으면 다음 슬롯을 변경하지 않는다. 정상 배포와 rollback 양쪽을 같은 부하 세션에서 실행한다. 새 설정 version에는 기존과 호환되는 cursor key, 인증 정책, source 설정이 있어야 한다. 같은 binary의 version 교체 시험으로 서로 다른 release의 N/N-1 호환성을 주장하지 않는다.

## 장애 시험

`enableFaults:true`는 전용 부하 ServiceAccount에 해당 namespace의 Pod 삭제, `wts-broker` StatefulSet 조회와 patch, `wts-feed` Lease 조회 권한을 추가한다. Pod 삭제 권한은 RBAC에서 label로 제한할 수 없으므로 namespace에 시험 대상만 두고 시험 후 Role과 RoleBinding도 제거한다. 일반 부하 실행은 이 권한과 token을 받지 않는다.

```json
{"clients":1000,"workers":4,"durationSeconds":600,"requireReconnectSlo":false,"enableFaults":true,"actions":[{"name":"gateway-kill","atSeconds":60,"command":["node","/app/bench/ha/kube-fault.mjs","gateway-kill","wts-ha"]},{"name":"feed-failover","atSeconds":180,"command":["node","/app/bench/ha/kube-fault.mjs","feed-failover","wts-ha"]},{"name":"broker-one","atSeconds":300,"command":["node","/app/bench/ha/kube-fault.mjs","broker-one","wts-ha"]}]}
```

`gateway-restart`는 gateway 전체, `full-restart`는 gateway와 feed와 broker 전체를 재기동하되 PVC를 보존한다. `broker-quorum`은 broker를 1개로 줄여 실제 Pod 종료를 확인하고 15초 후 3개로 복구한다. quorum이 없는 동안의 서비스 가용성에는 5초 목표를 적용하지 않는다. 종료된 Pod의 복구 시간과 마지막 committed 적용의 누락 여부를 확인한다. `finally` 복구도 실패할 수 있으므로 시험 후 broker replicas=3, Ready=3과 Lease를 확인해야 한다. 지속적인 broker 손실, quorum 부족 시 commit 거절과 이전 writer의 fencing은 `deploy/ha/smoke.sh`의 실제 Kafka 시험으로 별도 검증한다.

```sh
WTS_HA_ALLOW_FAULTS=dedicated-test-namespace node bench/ha/kind-fault.mjs wts-ha-test wts-ha node-drain
WTS_HA_ALLOW_FAULTS=dedicated-test-namespace node bench/ha/kind-fault.mjs wts-ha-test wts-ha network-partition
```

kind 전용 장애 도구는 부하 Pod가 없는 gateway worker만 골라 PDB를 존중해 drain하거나, 그 컨테이너의 kind network를 15초 분리했다가 같은 주소로 복구한다. 실험 후 cordon, network, broker quorum 상태를 확인한다. 한 호스트의 가상 노드 시험이며 실제 zone 단절이나 LB endpoint 제거 검증을 대신하지 않는다. 외부 LB 환경에서는 해당 플랫폼의 실제 노드와 네트워크 장애 주입 절차를 별도로 사용한다.

## 회귀 범위

| 경계 | 자동 검증 |
| --- | --- |
| committed ID, payload, 요청 범위, 조용한 topic, offset 공백 | `bench/ha/oracle.test.mjs`, `tests/ha_protocol.rs` |
| snapshot 경계, 중간 chunk 단절, 첫 cursor 없는 대량 replay | `tests/ha_protocol.rs`, `examples/ha-state.test.mjs` |
| cursor 만료, scope 변경, auth key rotation, 정책 만료와 권한 회수 | `src/ha/cursor.rs`, `src/ha/auth/policy/tests.rs`, HTTPS provider 시험 |
| 같은 ID의 다른 payload, dedupe 창 만료, 이전 writer 재개, broker 1대와 다수 손실 | `tests/ha_broker.rs`, `deploy/ha/producer-failover.mjs` |
| 드라이버 중단, 재시작, 검증 실패, 두 번째 slot 실패 rollback | `deploy/k8s/ha/rollout.test.mjs` |
| 부하 상태에서 배포, rollback, 노드와 네트워크 장애 | 이 디렉터리의 실환경 도구와 외부 oracle |

`.github/workflows/ha-acceptance.yml`은 수동 실행 전용이다. `ha-acceptance` environment에 `WTS_HA_KUBECONFIG` secret, `WTS_HA_CONTEXT`와 `WTS_HA_SETTINGS_JSON` 변수를 준비해야 한다. 미리 만든 전용 namespace와 digest 고정 이미지를 사용하며, 일반 CI가 운영 클러스터를 생성하거나 변경하지 않는다. 수동 workflow의 일반 장애 시험은 가용성 5초 합격 판정과 분리되어 있다. 계획 배포 p99 게이트는 위의 지속 부하와 직렬 rollout 연결로 실행한다.

부하 Worker는 여러 클라이언트의 checkpoint를 10ms 단위의 append journal로 묶고 fsync가 끝난 뒤에만 적용 완료로 집계한다. 상태와 cursor는 같은 JSON record에 저장하며, 각 클라이언트의 다음 frame 처리는 해당 fsync를 기다린다. 16MiB마다 전체 최신 상태를 fsync와 atomic rename으로 압축한다. 재접속 시간에는 group commit 대기도 포함된다. 기본 예제의 개별 FileStore 동기 fsync와 이 부하 발생기의 저장 방식은 다르므로 클라이언트 디스크 비용을 함께 보고한다.

부하 Worker는 V8 heap 한도를 별도로 적용한다. 클라이언트 수신 대기열은 64KiB에서 socket pause, 32KiB 미만에서 resume하며 1MiB를 넘으면 연결을 닫는다. 스스로 수신을 pause한 동안에는 수신 타이머를 연장하고 resume 시 3초를 다시 시작한다. 서버의 live buffer 한계를 넘으면 재접속으로 처리하며, 이를 시험 실패 결과에서 숨기지 않는다.

현재 연결에서 caught-up 상태가 아니거나 committed 검증 경계보다 뒤처진 클라이언트가 하나라도 남으면 검증에 실패한다. 과거에 한 번 연결됐다는 사실로 초기 준비 완료를 판정하지 않는다. 로컬 Unix 소켓의 `GET /status`는 현재 caught-up 수와 종료 사유 집계만 반환하며 token, cursor, 체결 본문은 반환하지 않는다.

계획 배포의 재접속 p99는 해당 slot의 window가 있고 장애 시나리오 표시가 없는 샘플만 사용한다. 표시된 장애 재접속은 `fault_reconnect_p99_ms`로 분리한다. 계획 배포 샘플이 없으면 `reconnect_slo_status`는 `not-measured`이며 `requireReconnectSlo:true` 게이트는 통과하지 않는다. 일반 부하와 장애 시험은 위 예시처럼 명시적으로 `false`를 사용한다. 계획 배포 시험과 장애 주입 시험은 별도 세션으로 실행한다.

외부 LB를 시험할 때는 `urls`와 함께 `externalEgress: [{"cidr":"198.51.100.10/32","port":443}]`처럼 실제 LB 목적지 CIDR과 TCP 포트를 지정한다. DNS 이름의 주소가 여러 개이거나 바뀌면 허용 CIDR도 함께 관리한다. 내부 gateway 8443과 broker 9092 규칙은 유지하며 외부 목적지를 자동으로 전체 허용하지 않는다.

배포 드라이버는 각 slot을 변경하기 전에 `phase=begin`과 고유 `window`를 oracle에 전달하고 워커의 측정 시작 응답을 기다린다. 검증에는 해당 window에서 시작한 계획 재접속만 포함한다. 이전 slot과 배포 전 재접속은 제외하며, 표본이 없으면 `not-measured`로 실패한다. 재시작은 journal의 window 식별자와 측정기에 이미 수집된 표본을 재사용한다. 오류/중단 시 `cancel`은 활성 측정만 해제하며 기존 표본은 삭제하지 않는다. 취소 이후에 시작한 재접속은 해당 window의 표본이 아니므로, 취소 구간에서 교체가 끝났다면 같은 plan을 재개해도 `not-measured`로 실패할 수 있다. 드라이버는 표본을 만들기 위해 이미 교체한 Pod를 다시 변경하지 않는다. 해당 slot을 검증할 표본이 없으면 다음 slot을 진행하지 않는다. 호환되는 이전 버전으로 rollback할 때도 같은 표본 게이트를 적용하므로 자동 복구 완료를 보장하지 않는다. rollback에서도 표본이 없거나 측정기를 잃었다면 [수동 복구 조건과 절차](../../deploy/k8s/ha/README.md#표본이-없는-rollback의-수동-복구)를 따른다. 장애 표시는 해당 action 시작부터 committed 적용 복구 확인까지 유지하고 그 뒤 해제한다.

`measured_targets_met`는 1,000 clients와 10분 이상, 계획 재접속 p99 5초 이하, forward/rollback 각각 3개 slot 검증, feed 장애 주입부터 새 committed status까지 30초 이하가 모두 측정된 경우에만 참이다. 하나라도 측정하지 않은 개별 기능 시험은 이 값을 참으로 기록하지 않는다. 운영 SLO 입증은 별도의 외부 환경 검토가 필요하다.

계획 재접속 p99 5초는 클라이언트의 실제 disconnect부터 durable caught-up까지의 표본에 적용한다. 장애 후 read-only oracle 조회와 committed 경계 적용을 기다리는 `recoveryDeadlineMs`는 별도 검증 예산으로 기본 60,000ms이며 1~60,000 정수만 허용한다. 이 기한을 늘려도 5초를 넘은 계획 재접속 표본은 통과하지 않는다. oracle 조회가 일시 실패하면 같은 기한 안에서 재시도한다. 장애 주입 명령 자체는 자동 재시도하지 않는다.

개별 action의 복구 통계는 고유 fault ID로 해당 장애에서 시작한 재접속만 계산한다. 최종 보고서의 fault p99는 실행 전체의 장애 표본을 합산한다. 교체 Pod 안정화와 committed 경계의 적용 일치를 확인한 뒤 장애 구간 표시를 해제한다.

`GET /status`는 활성 구간의 종류와 committed 비교까지 통과한 action 이름도 반환한다. 호스트에서 실행하는 node drain/network-partition은 예약한 Pod 장애의 `completed_actions`와 `active_measurement`를 확인한 뒤 별도로 수행한다. 이 수동 주입의 클라이언트 지연을 자동 action 표본으로 보고하지 않으며, host 명령 결과와 최종 committed 비교 근거를 함께 보존한다.

부하 워커는 접속 실패를 `bench/ha/reasons.mjs`의 고정 사유로 나눠 센다. upgrade 전에는 `handshake-timeout`, `connect-aborted`, `rejected-502`, `rejected-503`, `rejected-other`, `connect-refused`, `connect-reset`, `connect-error`, 연결된 뒤의 프로토콜 오류는 `connection-error`다. 연결된 뒤 소켓이 reset되면 ws가 error 없이 닫으므로 `connection-closed`로 남는다. `disconnect_reasons`는 사유별 누적 횟수, `reasons`는 마지막 사유별 클라이언트 수다. `GET /status`는 둘 다 돌려준다. 끝까지 간 실행의 보고서에는 `disconnect_reasons`만 최상위에 있다. 초기 접속 실패처럼 예외로 중단된 실행의 보고서에는 그때의 상태 수집에 성공한 경우에만 둘 다 `diagnostics` 안에 있다. 사유를 읽는 기준은 [용량 시험 기록](../../docs/ha-capacity.md)의 판정 기준 절에 있다.
