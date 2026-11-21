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

1,000개 연결, 두 topic 각 50 events/s의 정상 부하와 계획 배포, rollback, N-1, 강제 재접속, feed failover, broker quorum, 노드와 네트워크 장애의 결과는 각각 보고서로 남긴다. 가용성이 보장되지 않는 quorum 손실을 계획 배포 p99 5초 시험에 합산하지 않는다. 합격 여부와 별도로 client event loop 지연, Pod 재시작, CPU throttling, peak RSS, Ready 수와 lag를 검토한다. 실제 외부 LB와 다중 물리 장애 도메인 결과가 남기 전에는 #52와 #53을 닫지 않는다.

부하 발생기 OOM을 줄이기 위해 클라이언트는 미처리 frame이 64KiB에 이르면 소켓 수신을 pause하고 32KiB 미만에서 resume한다. 기존 1MiB 상한도 유지한다. Worker의 V8 old generation은 192MiB, young generation은 32MiB로 제한하고 부모 Node는 384MiB로 제한한다. JS heap 한도는 전체 RSS 한도가 아니므로 Pod의 메모리 한도와 OOM 여부를 계속 확인한다.

초기 부하 보고서의 `ready`는 한 번이라도 caught-up을 받은 클라이언트 수였으며 동시에 연결되어 있는 수가 아니다. 이후 판정은 현재 연결의 caught-up 상태를 추적하고, 검증 경계까지 영속화한 모든 클라이언트가 현재 caught-up 상태여야 통과하도록 강화했다. `/status`는 로컬 Unix 제어 소켓에서 현재 caught-up 수, offset 범위, 마지막 종료 사유 집계와 client event loop 지연을 제공한다.

재전송 중 TLS 프록시의 512MiB OOM을 추가로 관측했다. 프록시는 송신 대기가 64KiB에 도달하면 반대편 읽기를 pause하고 32KiB 미만이면 resume하며, 기존 1MiB 최종 상한은 유지한다. 16MiB 재전송과 일시 정지한 TLS 클라이언트의 회귀 시험은 수정 전 연결 종료를 재현했다. Kubernetes HA edge의 V8 old generation을 128MiB로 제한하지만 native/TLS/kernel 메모리까지 제한하는 값은 아니므로 전체 RSS와 cgroup OOM을 따로 확인한다.

수정한 DELETE 도구의 30명 장애 시험에서 feed failover는 장애 주입부터 새 committed status까지 11,055ms였다. 30명 모두 적용 ID와 payload가 일치했다. 이후 broker 1대 종료 직후 oracle 조회가 일시 실패해 전체 시험은 중단됐으며 `bench/results/ha/kind-fault-partial.json`에 보관했다. 초기 gateway 종료 단계는 교체 Pod 안정화 전에 비교했으므로 재접속 검증으로 인정하지 않는다. 도구는 삭제된 UID가 사라지고 교체 Pod들이 15초 연속 Ready가 될 때까지 기다리며, oracle의 읽기 전용 조회는 설정한 복구 기한 안에서 재시도하도록 수정했다.

다음 30명 실행에서는 gateway kill, feed failover, broker 1대 종료, quorum 손실, gateway 전체 재기동, 서비스 전체 재기동의 각 복구 뒤 committed ID와 payload 비교가 모두 통과했다. Feed failover는 10,506ms였고 quorum 손실 후에도 적용 누락과 중복은 0이었다. 최종 10분 전체 범위 조회는 15초 subprocess 제한으로 실패했으므로 전체 실행을 합격 처리하지 않았다. `bench/results/ha/kind-fault-six.json`에 단계별 근거와 최종 실패를 보관했다. 해당 실행의 action별 fault p99는 누적 표본이며 개별 장애 지연으로 인용하지 않는다. 최신 도구는 고유 fault ID로 분리하고 전체 검증 예산 안에서 큰 범위를 읽는다.
