# HA 용량 시험 기록

현재 결과는 한 호스트의 Docker와 kind에서 얻은 개발 검증이다. 운영 SLO, 실제 LB, 물리 zone 분산, CNI 정책 집행은 증명하지 않는다. 사용한 VM은 CPU 10개와 메모리 약 11.7GiB를 공유하며, control plane 1개와 worker 6개, broker 3개, feed 2개, gateway 3개와 부하 Pod가 같은 호스트를 사용한다. zone label은 3개에 각 worker 2개씩이다. Kubernetes Service의 TLS 경로를 사용했고 외부 LoadBalancer는 사용하지 않았다.

| 시험 | 결과 | 증거 |
| --- | --- | --- |
| 30 clients, 약 42초, 두 topic 합계 4,160 committed events | 클라이언트별 적용 ID와 payload 일치, 누락 0, 중복 적용 0 | `bench/results/ha/kind-smoke.json` |
| 1,000 clients, 압축 켠 edge 256MiB, 개별 동기 FileStore | 초기 접속 중 edge OOM. 성공으로 판정하지 않음 | Pod의 종료 reason `OOMKilled`, exit 137을 확인 |
| 1,000 clients, group journal, 계획 배포 첫 slot | 첫 slot 누락 0, 중복 적용 0, 재접속 p99 2.84초. 이후 부하 Pod가 2GiB에서 OOM되어 전체 시험 실패 | `bench/results/ha/kind-aborted-rollout.json` |
| 1,000 clients, 수신 백프레셔와 Worker heap 제한, 약 10분 | 부하 Pod peak 약 1.05GiB로 OOM은 없었으나 일부 클라이언트가 committed 검증 경계를 적용하지 못해 실패 | `bench/results/ha/kind-bounded.json`, `kind-bounded-cgroup.txt` |
| 1,000 clients, 공유 freshness 조회와 blocking reader 종료, 약 10분 | 현재 연결 1,000개 초기 catch-up. 첫 slot p99 2.80초와 누락/중복 0. 두 번째 slot과 rollback 검증에서 중단했으며 종료 시 190개만 caught-up이어서 전체 실패 | `bench/results/ha/kind-reader-catchup.json`, `kind-reader-rollout.json`, `kind-reader-cgroup.txt` |
| 30 clients, 첫 Kubernetes 장애 도구 시험 | feed failover 단계의 도구 실행 실패로 중단. DELETE 본문의 강제 종료 옵션과 식별 조건이 누락된 사실을 별도 HTTP 재현으로 확인해 수정했으며 장애 검증 성공으로 사용하지 않음 | `bench/results/ha/kind-fault-tool-failure.json` |
| 1,000 clients, pause 중 수신 deadline 연장, 약 10분 | 첫 slot 검증 실패. edge 하나가 512MiB에서 OOM으로 재시작했고 종료 시 199개만 caught-up. 전체 실패 | `bench/results/ha/kind-paused-client.json`, `kind-paused-rollout.json`, `kind-paused-cgroup.txt` |
| 1,000 clients, 프록시 TCP 백프레셔와 admission watch | 942개 caught-up 상태에서 클라이언트 적용 오류 10개로 중단. 배포 단계는 실행하지 않았으며 원인 분류를 추가해 조사 | `bench/results/ha/kind-flow-failure.json` |
| 1,000 clients, 같은 서버와 상세 오류 분류, 120초 정상 부하 | 연결 1,000개 유지, 적용 오류/재접속 없음. 마지막 committed 경계를 5초 안에 모두 적용하지 못해 실패. client event loop p99 약 381ms | `bench/results/ha/kind-flow-steady.json`, `kind-flow-steady-cgroup.txt` |
| 1,000 clients, 압축 해제, edge 512MiB, 개별 동기 FileStore | OOM 없이 60초 안에 602개만 초기 catch-up. 실패 | `bench/results/ha/kind-1000-initial-timeout.json` |

HA 기본 edge는 압축을 끄고 CPU request 250m/limit 2, memory request 128Mi/limit 512Mi로 조정했다. gateway는 CPU request 500m/limit 2, memory request 256Mi/limit 1Gi다. 부하 Pod는 CPU request 1/limit 4, memory request 512Mi/limit 2Gi다. 이 값은 운영 용량 산정값이 아니다. 정상 gateway 3개와 rollout surge 1개에 필요한 request를 각 zone 안의 적격 노드 2개에 예약하고, 1개 장애 시 나머지 2개가 트래픽과 replay를 처리하는지 따로 측정해야 한다.

개별 클라이언트의 동기 파일 교체와 fsync는 부하 발생기에도 큰 비용을 만든다. 현재 부하 도구는 Worker별 append journal에서 checkpoint들을 10ms씩 묶고 fsync 완료 후에만 durable 적용으로 집계한다. 이 저장 방식과 예제 FileStore의 결과를 혼용하지 않는다. 네트워크/서버 시간만 따로 재접속 시간으로 보고하지 않고 저장 대기까지 포함한다.

1,000개 연결, 두 topic 각 50 events/s의 정상 부하와 계획 배포, rollback, N-1, 강제 재접속, feed failover, broker quorum, 노드와 네트워크 장애의 결과는 각각 보고서로 남긴다. 가용성이 보장되지 않는 quorum 손실을 계획 배포 p99 5초 시험에 합산하지 않는다. 합격 여부와 별도로 client event loop 지연, Pod 재시작, CPU throttling, peak RSS, Ready 수와 lag를 검토한다. 실제 LB와 다중 AZ 결과는 #90에, 1,000 clients 병목 분석은 #91에 남긴다.

부하 발생기 OOM을 줄이기 위해 클라이언트는 미처리 frame이 64KiB에 이르면 소켓 수신을 pause하고 32KiB 미만에서 resume한다. 기존 1MiB 상한도 유지한다. Worker의 V8 old generation은 192MiB, young generation은 32MiB로 제한하고 부모 Node는 384MiB로 제한한다. JS heap 한도는 전체 RSS 한도가 아니므로 Pod의 메모리 한도와 OOM 여부를 계속 확인한다.

초기 부하 보고서의 `ready`는 한 번이라도 caught-up을 받은 클라이언트 수였으며 동시에 연결되어 있는 수가 아니다. 이후 판정은 현재 연결의 caught-up 상태를 추적하고, 검증 경계까지 영속화한 모든 클라이언트가 현재 caught-up 상태여야 통과하도록 강화했다. `/status`는 로컬 Unix 제어 소켓에서 현재 caught-up 수, offset 범위, 마지막 종료 사유 집계와 client event loop 지연을 제공한다.

재전송 중 TLS 프록시의 512MiB OOM을 추가로 관측했다. 프록시는 송신 대기가 64KiB에 도달하면 반대편 읽기를 pause하고 32KiB 미만이면 resume하며, 기존 1MiB 최종 상한은 유지한다. 16MiB 재전송과 일시 정지한 TLS 클라이언트의 회귀 시험은 수정 전 연결 종료를 재현했다. Kubernetes HA edge의 V8 old generation을 128MiB로 제한하지만 native/TLS/kernel 메모리까지 제한하는 값은 아니므로 전체 RSS와 cgroup OOM을 따로 확인한다.

수정한 DELETE 도구의 30명 장애 시험에서 feed failover는 장애 주입부터 새 committed status까지 11,055ms였다. 30명 모두 적용 ID와 payload가 일치했다. 이후 broker 1대 종료 직후 oracle 조회가 일시 실패해 전체 시험은 중단됐으며 `bench/results/ha/kind-fault-partial.json`에 보관했다. 초기 gateway 종료 단계는 교체 Pod 안정화 전에 비교했으므로 재접속 검증으로 인정하지 않는다. 도구는 삭제된 UID가 사라지고 교체 Pod들이 15초 연속 Ready가 될 때까지 기다리며, oracle의 읽기 전용 조회는 설정한 복구 기한 안에서 재시도하도록 수정했다.

다음 30명 실행에서는 gateway kill, feed failover, broker 1대 종료, quorum 손실, gateway 전체 재기동, 서비스 전체 재기동의 각 복구 뒤 committed ID와 payload 비교가 모두 통과했다. Feed failover는 10,506ms였고 quorum 손실 후에도 적용 누락과 중복은 0이었다. 최종 10분 전체 범위 조회는 15초 subprocess 제한으로 실패했으므로 전체 실행을 합격 처리하지 않았다. `bench/results/ha/kind-fault-six.json`에 단계별 근거와 최종 실패를 보관했다. 해당 실행의 action별 fault p99는 누적 표본이며 개별 장애 지연으로 인용하지 않는다. 최신 도구는 고유 fault ID로 분리하고 전체 검증 예산 안에서 큰 범위를 읽는다.

재전송 write timeout 뒤 checkpoint 위치가 후퇴할 수 있는 경로를 별도로 재현했다. Sink에 이벤트가 enqueue된 뒤 flush가 취소되면 전송 실패 전 위치로 checkpoint를 만들 수 있었다. 실제 WebSocket Sink 회귀에서 수정 전 이벤트 뒤의 오래된 checkpoint가 확인됐으며, 수정 후 전송 오류에서는 checkpoint를 추가하지 않는다. replay 용량 제한처럼 송신이 완료된 경계는 기존대로 checkpoint를 보존한다. 앞선 일반 적용 오류 10개의 상세 원인은 남아 있지 않아 모두 이 경로 때문이라고 단정하지 않는다.

checkpoint 전송 오류 수정 커밋 `2d5fb98`과 고정 이미지로 1,000명 초기 접속을 다시 시험했다. 4 workers/20ms, 4 workers/5ms, 8 workers/5ms 세 설정 모두 60초 안에 전원이 catch-up하지 못했다. 5ms/4 workers 실행은 진단 시 978명이 caught-up이었으며 나머지 실행도 적용 오류와 부하 Pod OOM은 없었다. 세 실행의 설정과 이미지 digest, 결과, cgroup 근거를 `bench/results/ha/kind-final-1000-*.json`과 같은 이름의 cgroup 파일에 보관했다. 초기 접속 실패로 이 실행들에서는 rollout을 시작하지 않았다. 1,000명 SLO는 충족했다고 판단하지 않으며 병목 분석과 자원 분리 환경 측정을 #91에 남긴다.

이 표와 artifact는 각 커밋 시점의 실행을 보존한다. 이후 최종 이미지의 30명 배포/rollback/장애 기능 검증과 노드 시험 결과, 실행 설정, image digest는 [#53의 검증 기록](https://github.com/yceffort/ws-trade-server/issues/53)에 추가한다. 기능 검증 통과를 1,000명 용량 검증이나 실제 외부 LB/물리 장애 도메인 검증으로 대신하지 않는다.

최종 data-plane 이미지의 30명 계획 배포는 forward 3개 slot과 rollback 3개 slot 모두 통과했다. 각 slot의 재접속 p99는 0.50~0.58초, committed 적용 누락과 중복은 0이었다. `bench/results/ha/kind-planned-rollout.json`에 source commit, image digest와 6개 독립 구간을 보관했다. 같은 실행에 너무 이르게 예약한 장애는 마지막 rollout 측정과 겹쳐 안전장치가 거부했고, 전체 실행은 실패로 유지했다. 장애 검증은 별도 세션에서 수행한다.

## 1,000 clients 병목 판정 기준

[#91](https://github.com/yceffort/ws-trade-server/issues/91)의 계단 측정(100, 250, 500, 1,000 clients, 설정당 3회)을 돌리기 전에 정한 기준이며 결과를 본 뒤에 고치지 않는다. 기존 보고서의 `connection-error`는 소켓 오류를 한 이름으로 뭉친 값이었다. 응답하지 않는 서버에 예제 클라이언트를 붙이면 수신 기한 3초가 handshake 중인 소켓을 끊는데 이것도 `connection-error`로 보고된다(`bench/ha/reasons.test.mjs`). 이제 부하 워커가 `bench/ha/reasons.mjs`로 ws의 오류 문구와 Node 오류 코드만 보고 고정 사유를 붙인다. `disconnect_reasons`는 실행 중 끊긴 횟수를 사유별로 누적한 값이고 `reasons`는 마지막 사유별 클라이언트 수다. 끝까지 간 실행의 보고서에는 `disconnect_reasons`만 있고, 예외로 중단된 실행의 보고서에는 상태 수집에 성공한 경우에만 둘 다 `diagnostics` 안에 있다.

| 부하 보고서의 사유 | 뜻 | 함께 볼 값 |
| --- | --- | --- |
| `connect-aborted`, `handshake-timeout` | 3초 안에 WebSocket upgrade 응답을 받지 못했다. 앞은 클라이언트의 수신 기한, 뒤는 ws의 handshake 기한이 먼저 끝난 경우다 | edge의 `tls_aborted`(TLS 단계에서 끊김)와 `client_closed`(edge가 gateway 연결을 기다리던 중 끊김). `tls_aborted`는 probe가 만드는 Pod당 10초에 6건을 빼고 본다 |
| `rejected-503` | gateway가 준비 상태가 아니거나 연결 한도를 넘어 upgrade를 거절했다 | edge의 `upstream_unavailable`, `wts_ha_source_age_seconds`, `wts_ha_broker_age_seconds` |
| `rejected-502` | edge가 gateway에 연결하지 못했거나, gateway가 503이 아닌 응답으로 upgrade를 거절했다 | edge의 `upstream_error`, `upstream_rejected`, gateway 컨테이너 재시작 |
| `connect-reset`, `connect-refused` | 접속을 받는 쪽이 없거나 받자마자 끊었다 | edge 컨테이너 재시작과 OOM, edge의 연결 한도 1,024 |
| `source-stale`, `retry-later`, `replay-limit` | gateway가 연결을 받은 뒤 스스로 끊었다 | `wts_ha_closed_total`, `wts_ha_replay_waiters`, `wts_ha_catch_up_seconds` |
| `receive-deadline`, `connection-closed` | 연결된 뒤 3초 동안 frame이 없었거나 소켓이 오류 frame 없이 닫혔다 | edge의 `buffer_limit`, `relay_error`, `upstream_closed` |

부하 발생기로 판정하는 경우는 부하 Pod의 event loop p99가 100ms 이상이거나 부하 노드 CPU가 80% 이상인데, 서버 노드 CPU는 80% 미만이고 `wts_ha_replay_waiters`가 128에 닿지 않으며 서버 쪽 사유(`rejected-503`, `rejected-502`, `source-stale`, `retry-later`, `replay-limit`)가 없는 때다. 서버로 판정하는 경우는 부하 Pod의 event loop p99가 100ms 미만이고 부하 노드 CPU가 80% 미만인데도 실패가 나오는 때이며, 위 표로 edge의 TLS 종료, gateway의 admission과 replay, broker 중 어디인지 가린다. 양쪽이 함께 포화하면 판정하지 않고 부하 노드를 늘려 다시 잰다. 100ms는 통과한 30 clients 실행의 p99 약 25ms와 실패한 1,000 clients 실행의 241~670ms 사이에서 고른 값이고, 80%와 함께 이 실험의 규약이다.

## AWS 실측 (2026-11-22)

[#90](https://github.com/yceffort/ws-trade-server/issues/90)과 [#91](https://github.com/yceffort/ws-trade-server/issues/91)의 측정이다. 서울 리전 AZ 3개의 EKS 1.35에서 AZ마다 4 vCPU Graviton spot 노드 2대에 gateway 3개, feed 2개, broker 3개를 두고 부하 발생기는 전용 on-demand 8 vCPU 노드에서 돌렸다. 클라이언트는 내부용 NLB(IP target, TCP 443, cross-zone 꺼짐, 등록 해제 지연 300초)를 거쳐 edge의 TLS로 붙었고 VPC CNI의 NetworkPolicy 집행을 켰다. 구성과 순서는 [`deploy/aws/README.md`](../deploy/aws/README.md), 버전과 노드 구성은 `bench/results/ha/aws-environment.txt`에 있다. 서버 노드는 spot이고 측정 중 세 번 교체됐다. 인터넷에 노출된 LB 경로, AZ 전체 장애(EBS 포함), 서로 다른 release의 혼합, on-demand 노드에서의 반복은 재지 않았다.

| 시험 | 결과 | 증거(`bench/results/ha/`) |
| --- | --- | --- |
| NLB 경로 smoke, 30 clients | 6,180 events 누락 0 중복 0, 끊김 0 | `aws-smoke-nlb.json` |
| 클러스터 내부 Service 경로, 30 clients | 정책 집행 상태에서 30개 모두 `connect-aborted`, 초기 접속 실패 | `aws-smoke-service-denied.json` |
| NetworkPolicy 허용과 거부 | 막혀야 하는 8개 경로가 모두 막히고 열려야 하는 경로는 열림. Deployment, StatefulSet, Job, owner 없는 Pod 모두 집행됨 | `aws-network-policy.txt` |
| 초기 접속 계단, 100, 250, 500, 1,000 clients 각 3회 | 12회 모두 60초 안에 전원 catch-up, 끊김 0, 누락 0 | `aws-stair.json` |
| 1,000 clients 계획 배포와 feed failover, 929초 | 90,700 events 누락 0 중복 0. 계획 재접속 p99 1.73초(slot별 1.49~2.11초), feed failover 10.7초, `measured_targets_met` 참 | `aws-rollout-1000.json` |
| 같은 배포, readiness gate 켬 | 91,720 events 누락 0 중복 0. 계획 재접속 p99 1.85초(slot별 1.30~1.96초) | `aws-rollout-1000-readiness-gate.json` |
| Pod 장애 6종, 100 clients | 6종 모두 복구 뒤 누락 0 중복 0. feed failover 10.8초 | `aws-faults-100.json` |
| AZ b의 서브넷 차단 180초, 1,000 clients | 88,608 events 누락 0 중복 0, 전원 복귀 | `aws-az-isolate-1000.json` |
| AZ b의 서버 인스턴스 종료 240초, 1,000 clients 2회 | 2회차 91,651 events 누락 0 중복 0. 종료 21~22초 뒤부터 gateway b의 클라이언트가 빠지고 77초, 106초에 전원 복귀. 1회차는 최종 보고서 수집 실패 | `aws-az-stop-1000.json`, `aws-timelines.txt` |

자원을 분리한 이 환경에서는 1,000 clients가 실패하지 않아 위 판정 기준을 적용할 실패가 없다. 계단 측정에서 부하 컨테이너의 CPU는 100 clients에서 0.48~0.53, 250에서 1.0, 500에서 1.9, 1,000에서 3.8~4.0 core로 client당 약 4 mCPU씩 늘었다. 1,000에서는 한도 4 core에 닿아 throttle이 256~292회, event loop p99가 213~220ms였다. 같은 실행에서 서버 노드 CPU는 최대 49%, gateway 컨테이너 0.64~1.02 core, edge 0.72~0.84 core, broker 0.62~0.83 core였고 replay 대기는 최대 1, replay 작업은 최대 8이었다. kind 호스트는 CPU 10개를 서버와 부하 발생기가 함께 썼고 부하 발생기 하나가 그중 4개를 필요로 한다. kind의 실패를 자원을 줄여 다시 만들어 보지는 않았으므로 이것은 kind 실패의 원인을 직접 확인한 것이 아니라 그 방향의 근거다. 1,000 clients를 넘겨 재려면 부하 Pod의 CPU 한도부터 올려야 한다.

계획 배포에서 readiness gate가 없으면 slot 교체 6번 모두 그 AZ에 healthy target이 없는 16초 구간이 있었다. 새 target이 `initial`인 동안 옛 target이 `draining`으로 넘어갔기 때문이다. 그 구간에도 재접속 p99는 1.5초 안팎이었다. gate를 켜면 그 구간이 없었고 slot마다 약 30초가 더 걸렸다(forward 3분 48초에서 5분 36초, rollback 4분 12초에서 6분 1초). gateway가 준비 상태를 잃으면 target이 등록 해제됐다가 다시 등록되고 healthy까지 약 30초가 걸린다. feed failover 때 gateway 3개가 모두 그랬고 그동안 클라이언트는 `initial` 상태의 target으로 다시 붙었다. 모든 연결에 1초 간격 heartbeat가 흘러 NLB의 idle timeout 350초에 닿은 연결은 없었다. 조용한 연결이 실제로 idle timeout에 닿는 경우는 재지 않았다.

Pod 장애의 복구 시간은 kind보다 길었고 원인은 가리지 않았다. 장애 재접속 p99와 장애 주입부터의 복구 p99는 gateway-kill 0.59초와 1.70초, feed-failover 35.0초와 38.1초, broker-one 56.5초와 59.6초, broker-quorum 81.1초와 81.6초, gateway-restart 25.1초와 26.2초, full-restart 102.4초와 103.4초다. kind의 30 clients 실행은 차례로 0.53초와 1.59초, 22.9초와 25.9초, 재접속 없음, 62.9초와 63.2초, 8.4초와 9.5초, 15.1초와 15.6초였다. 여섯 가지 중 gateway-kill을 뺀 다섯에서 gateway 3개의 NLB target이 모두 등록 해제됐다가 다시 등록됐다. 다시 등록되기까지 feed-failover는 약 9초, broker-one은 약 21초, broker-quorum은 약 37초, gateway-restart는 약 5초, full-restart는 약 64초였다(2초 간격 기록). broker 한 대를 내렸을 때 gateway 전체가 준비 상태를 잃는 것은 kind에서 없던 일이며 [#94](https://github.com/yceffort/ws-trade-server/issues/94)에서 다룬다.

AZ b의 서브넷을 차단하자 `receive-deadline` 353건과 `source-stale` 647건이 나왔다. 앞은 차단된 b 경로의 연결이 기한으로 끊긴 것, 뒤는 b와 무관한 gateway a와 c가 스스로 끊은 것으로 본다. 차단 17초 뒤 a와 c의 target이 등록 해제됐다가 33초에 다시 등록됐고, AZ a에 있던 active feed는 `ha-deadline`으로 재시작해 30초 뒤 다시 active가 됐다. 그동안 `rejected-503`이 5,834건 쌓였다. broker 한 대를 예고 없이 잃으면 그 AZ의 3분의 1이 아니라 전체 클라이언트가 한 번 끊긴다는 뜻이다([#94](https://github.com/yceffort/ws-trade-server/issues/94)). 로컬 재현으로 찾은 원인과 이후의 대응은 [설계 문서](ha-design.md#broker-한-대의-예고-없는-손실)에 적었고, 대응 뒤 AWS에서는 다시 재지 않았다. 이 실행은 상태 기록 도구의 버그로 차단 구간의 클라이언트 수 변화를 남기지 못했다. 인스턴스 종료에서는 EC2 종료가 OS 종료를 거쳐 gateway b가 스스로 drain했고 broker도 정상 종료해, gateway b의 337~345개만 끊겼다. gateway 2개가 500개씩 받는 동안 edge는 0.96~1.0 core로 단일 스레드 한계에 있었고 gateway 컨테이너는 0.95~1.04 core였다. 1,000 clients는 유지됐지만 N-1의 여유는 없다([#95](https://github.com/yceffort/ws-trade-server/issues/95)).

## 체결 로그 보관 용량 (2026-11-22)

[#98](https://github.com/yceffort/ws-trade-server/issues/98)의 측정이다. 단일 호스트 Compose(Kafka 4.1.1, 압축 없음, segment 16MiB)에서 feed 하나를 장중 모드로 182.85초 돌리며 `kafka-log-dirs`의 partition 크기, offset, committed 체결 수를 쟀다. 체결은 18,260건(초당 99.9건), offset은 21,912개였다. offset에는 상태 레코드와 transaction marker가 함께 들어 있어 체결 수와 같지 않다.

| 항목 | 값 |
| --- | --- |
| `wts.events.v1` replica 하나의 증가량 | 10,561,012바이트(초당 약 57.8KB) |
| 체결 1건당 디스크 | 약 578바이트(상태 레코드, marker, batch overhead 포함) |
| 하루 환산 | replica 하나에 약 4.65GiB, replication factor 3이면 클러스터 전체 약 14GiB |
| 개발 기본값 1GiB가 차는 시간 | 약 5.2시간 |

이슈에 적은 payload 추정(하루 약 4.27GiB, 1GiB까지 약 5.62시간)보다 디스크 실측이 약 9% 크다. 하루 환산은 3분 측정을 곱한 값이며 24시간 연속 측정이 아니다.

개발 환경은 `retention.bytes` 1GiB를 그대로 쓴다. 24시간 보관을 목표로 하는 검증 환경은 k8s의 `retentionBytes` 또는 Compose의 `WTS_HA_RETENTION_BYTES`를 8GiB(8589934592)로 둔다. 실측의 약 1.7배로, 식별자가 길거나 체결이 큰 경우와 삭제 단위인 segment(16MiB)만큼의 여유를 둔 값이다. 여기에 compact 토픽 몇십 MB를 더해도 settings 예시의 broker PVC 20Gi 안이다. 이 설정에서 실제로 24시간 동안 원래 cursor로 복구되는지는 장시간 시험을 하지 않아 확인하지 않았다.

보관 경계의 동작은 `deploy/ha/retention-boundary.mjs`가 확인한다. DeleteRecords로 로그 시작 위치를 앞당긴 뒤, 경계에 맞는 cursor는 이어서 복구되고 경계 앞의 events cursor와 state cursor는 `cursor-expired`, 경계 앞의 `start_at`은 `start-out-of-range`로 끝나며 저장 상태가 바뀌지 않는지 본다. 시간과 용량 retention도 broker가 로그 시작 위치를 앞당기는 일이라 클라이언트가 보는 결과는 같다. broker의 retention 검사 주기(기본 5분)와 segment 삭제 시점은 이 시험에 들어 있지 않다.

## edge 하나의 한계 (2026-11-23)

[#95](https://github.com/yceffort/ws-trade-server/issues/95)의 측정이다. AWS에서 gateway가 2개만 남아 Pod마다 500 연결을 받는 동안 edge가 0.96~1.0 core로 단일 스레드 한계에 있었다. edge만 따로 포화시키려고 `edge/capacity.mjs`로 쟀다. 합성 upstream이 연결마다 10ms마다 450바이트 프레임을 보내고, 가벼운 수신 클라이언트는 받은 수만 센다. Apple M1(고성능 코어 4개, 효율 코어 4개) 한 대에서 upstream, edge, 클라이언트를 함께 돌렸으며 결과는 `bench/results/ha/edge-capacity-m1.json`에 있다.

| edge worker | 연결 시도 | 연결 성공 | 초당 전달 | edge core | 접속 실패 |
| --- | --- | --- | --- | --- | --- |
| 1 | 600 | 600 | 53,556 | 0.79 | 없음 |
| 1 | 800 | 750 | 72,881 | 0.96 | `ECONNRESET` 50 |
| 1 | 1,000 | 899 | 87,118 | 0.95 | `ECONNRESET` 101 |
| 2 | 1,000 | 1,000 | 91,511 | 1.67 | 없음 |
| 2 | 1,800 | 1,800 | 129,334 | 1.87 | 없음 |

연결 성공은 측정 구간이 끝날 때 열려 있던 연결 수이며, 붙은 뒤 끊긴 연결은 모든 단계에서 0이었다. worker 하나는 이 호스트에서 약 750 연결(초당 약 73,000건)에 코어 하나를 다 썼다. 그 뒤에는 이미 붙은 연결의 전달은 유지했지만 새 TLS 접속이 `ECONNRESET`으로 끊겼다. TLS handshake도 같은 스레드가 처리하므로 재접속 폭주가 이 한계를 먼저 넘는다. AWS c7g에서는 500 연결에 1.0 core였으므로 코어당 한계는 호스트마다 다르다.

edge는 이제 `WTS_EDGE_WORKERS`만큼 Node cluster worker를 띄워 같은 포트를 나눠 받는다. 한 worker가 죽으면 그 자리만 다시 띄우고, 듣기도 전에 죽으면 전체를 실패로 끝낸다. k8s 배포는 `edgeWorkers` 기본값 2를 쓰고, worker마다 연결 상한(1,024)과 버퍼가 따로 있어 edge 메모리 limit을 512Mi에서 1Gi로 올렸다. worker 2개로는 1,800 연결까지 접속 실패가 없었다.

같은 600 연결에서 worker 2개는 1.11 core로 1개(0.79)보다 약 40% 더 썼다. 이 호스트의 효율 코어로 밀려난 영향으로 보지만 확인하지 않았다. N-1(1,000 clients, gateway 하나 손실)을 이 구성으로 AWS에서 다시 재지는 않았다. 로컬 전체 구성은 부하 발생기가 600~800 연결에서 먼저 포화되어 N-1 재측정에 쓰지 못했다.
