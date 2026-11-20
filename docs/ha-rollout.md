# HA 배포와 검증 계획

상태: 미구현 계획. [데이터 및 복구 설계](ha-design.md)의 후속 작업이며 현재 `deploy/k8s`의 단일 Pod 예제를 HA 구성으로 오해하지 않도록 별도 overlay에서 진행한다. 아래 시간, 용량과 오류 예산은 검증 목표이지 달성한 성능 수치가 아니다.

## 배포 단위와 장애 도메인

| 단위 | 초기 설정 |
| --- | --- |
| gateway Deployment | zone별 slot 3개에 각각 gateway와 native TLS sidecar, replicas 1. 각 slot은 RollingUpdate, maxUnavailable 0, maxSurge 1이며 배포 드라이버가 한 slot씩 갱신 |
| readiness 안정화 | minReadySeconds 15, progressDeadlineSeconds 600, bootstrap 실패 시 rollout 중단 |
| gateway PDB | minAvailable 2. 자발적 eviction을 제어하며 rollout이나 노드 손실을 막지 않음 |
| producer | active/standby 2개, 별도 노드, 고정 logical shard와 Lease. broker fencing을 함께 적용 |
| broker 및 controller | 복제 3 및 quorum, PVC, 장애 도메인 분산, transaction 관련 내부 토픽 포함 |
| 네트워크 진입 | HA L4 LoadBalancer에서 WSS Service로 전달. TLS와 압축은 Pod의 edge에서 처리 |

gateway slot은 각각 다른 zone에 고정하고 전체 slot을 선택하는 required hostname anti-affinity를 적용한다. 최소 3개 장애 도메인과 N-1 처리 용량을 운영 전제로 한다. 갱신할 zone 안에서도 기존 Pod와 surge Pod가 다른 노드에 배치되어야 하므로 각 zone에 적격 hostname을 2개 이상, 전체 최소 6개 확보한다. 동시에 동작하는 gateway는 정상 3개, 계획 배포 중 최대 4개이며 각 zone에는 한 gateway의 surge 자원을 예약한다. 단순히 3개 노드에 CPU 여유만 확보해서는 이 배치를 충족하지 못한다. 필요한 분산 조건을 만족하지 못하면 조용히 한 노드에 몰아넣지 말고 rollout을 멈춘다. 노드 또는 zone 장애 처리 중에는 추가 rollout을 중지하고 남은 Pod의 부하를 확인한다.

일반적인 replicas 3 Deployment는 Pod 하나의 교체 후 운영 검증을 기다려 주지 않는다. 첫 구현은 별도 Kubernetes controller 대신 위의 replicas 1 Deployment 세 개와 직렬 배포 드라이버를 사용한다. Service와 PDB는 세 slot의 공통 label을 선택한다. 드라이버는 환경당 하나의 배포 잠금을 잡고 현재 slot의 검증 성공을 영속 기록한 뒤에만 다음 slot의 image/config를 patch한다. 세 Deployment를 한 번에 수정하는 apply/GitOps 동기화는 이 배포 경로에서 허용하지 않는다. 중단된 드라이버는 클러스터 revision과 이전 단계 결과를 재확인하며 실패나 결과 유실 시 다음 patch를 수행하지 않는다. 관리자 수동 변경은 이 보장 범위 밖이며 배포 계정의 권한과 파이프라인으로 단일 변경 경로를 운영한다.

PDB와 RollingUpdate의 역할은 다르다. Deployment의 rollout은 PDB에 의해 제한되지 않고 `maxUnavailable`과 `maxSurge` 설정을 따른다. 강제 Pod 삭제, OOM, 노드 전원 손실에도 PDB가 가용성을 보장하지 않는다. [Kubernetes disruption 문서](https://kubernetes.io/docs/concepts/workloads/pods/disruptions/)와 [Deployment 문서](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/)를 기준으로 검증한다.

운영 환경에 기존 Ingress가 있다면 같은 연결 및 drain 조건을 만족할 때만 대체한다. 자체 Ingress controller 한 개를 두어 새로운 단일 장애 지점을 만들지 않는다. Cloud LoadBalancer/Ingress의 구현과 인증서 공급자는 아직 선택하지 않았으며 실제 환경에서 검증하기 전에는 외부 경로의 SLO를 달성했다고 선언하지 않는다.

## readiness와 종료 상태 머신

`BOOTSTRAPPING -> READY`, `READY <-> DEGRADED`, `READY/DEGRADED -> DRAINING -> STOPPED`를 사용한다. source 장애로 freshness가 깨지면 DEGRADED로 내려가고, 복구 및 catch-up을 확인한 뒤 다시 READY가 될 수 있다. DEGRADED에서 기존 HA 연결에도 source-stale을 알리고 닫는다. BOOTSTRAPPING 중 종료 요청은 복원을 취소하고 종료하며 DRAINING은 되돌리지 않는다. `src/server.rs`, `src/http.rs`, `src/bridge.rs`, 연결 종료 경로가 같은 상태와 monotonic deadline을 공유해야 한다.

| endpoint | 의미 |
| --- | --- |
| `GET /livez` | event loop 및 관리 경로 응답. source/broker 일시 장애만으로 프로세스를 반복 재시작하지 않음 |
| `GET /readyz` | bootstrap 완료, 허용 schema/config, 인증 정책 신선도, 로그 catch-up과 source 상태가 유효하며 drain 중이 아님 |
| `POST /drain` | loopback에서만 받는 idempotent 관리 명령. readiness와 새 upgrade 허용을 같은 상태 변경으로 내림 |

현재 관리 HTTP는 이 API를 제공하지 않는다. 신규 endpoint는 loopback에만 바인딩하며 Service와 외부 프록시에 공개하지 않는다. `/metrics`와 `/livez`는 drain 완료까지 유지한다. broker adapter의 종료, 진행 중 HTTP 요청, 출력 큐, 스레드 join은 모두 남은 종료 예산을 공유하고 `select!` 취소로 cursor 적용을 잃지 않게 한다.

종료 유예는 초기 60초다. preStop 시간도 이 유예에 포함되고 routing 제거가 애플리케이션 signal 처리와 동시에 진행될 수 있다. terminating EndpointSlice를 모든 프록시가 같은 속도로 반영한다고 가정하지 않는다. [Pod 종료 절차](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-termination-flow) 참조.

1. preStop이 loopback `POST /drain`을 호출한다. 응답은 즉시 반환하고 상태 머신의 시작 시각을 기록한다. READY는 false, 신규 WebSocket upgrade는 HTTP 503과 제한된 Retry-After로 거절한다.
2. hook은 routing 반영 예산 20초를 기다린다. 이 시간에는 기존 연결, 인증 검사, 로그 소비와 관리 HTTP를 유지한다. 고정 20초는 전파 완료의 증명이 아니므로 실제 LB 측정 결과로 조정한다.
3. SIGTERM 시 재접속 통지를 최대 10초 범위로 분산한다. HA 클라이언트는 마지막 적용 cursor로 다른 Pod에 연결한다. 기존 `/ws`에는 기존 `server-shutdown` 의미를 유지한다.
4. 최대 10초 동안 flush와 소켓 종료를 마친 뒤 입력, hub, 관리 HTTP와 로그를 정리한다. edge는 native sidecar로 gateway 뒤에 종료한다. 남은 20초는 hook 실패, 스케줄링 지연 및 sidecar 종료의 여유다.

preStop 없이 SIGTERM만 와도 동일한 drain 상태 머신을 시작한다. 이미 시작된 drain을 다시 호출해 예산을 연장하지 않는다. 훅이 실패하거나 SIGKILL이 오면 통지 프레임이 없어도 client heartbeat/read deadline으로 재접속해야 한다. 새 admission을 거절하는 단계에서 listener를 먼저 없애거나 source 소비를 먼저 끊지 않는다.

현재 edge는 upstream 연결 실패를 일반 502로 바꿀 수 있으므로 HA 구현에서 upstream의 503과 제한된 Retry-After를 보존한다. 임의의 upstream 헤더를 모두 전달하지는 않는다. endpoint 전파 중 옛 Pod로 들어간 요청은 거절될 수 있으므로 개별 handshake 100% 성공을 약속하지 않고 제한된 재시도까지 포함해 접속 성공 시간을 측정한다.

## 인증과 설정 일관성

로컬 데모의 JSON token 파일 모드는 유지한다. HA 프로파일은 짧은 수명의 서명 토큰, issuer/audience/expiry/topic scope 검증과 버전이 있는 정책/JWKS 캐시를 사용한다. 인증 공급자 자체도 HA 요구를 충족해야 하며 구체적인 제품이나 클라우드는 이번 설계에서 선택하지 않는다.

폐기 정책은 온라인으로 갱신하고 캐시의 유효 기간을 최대 60초로 제한한다. 초기 목표는 폐기를 모든 활성 gateway와 기존 연결에 60초 안에 반영하는 것이다. 만료된 정책에서 새 연결이나 활성 세션을 계속 허용하지 않는다. 공급자 장애 시 fresh cache의 남은 수명까지만 동작하고 그 이후에는 준비 상태를 내리고 해당 세션을 종료한다. 토큰 exp도 연결 중에 계속 검사한다. 이 선택은 공급자 장기 장애 때 가용성보다 권한 검증을 우선한다.

새 검증 키를 먼저 모든 gateway에 배포해 확인한 뒤 서명 키를 바꾼다. 이전 토큰과 cursor의 최대 수명 및 replay 보관 기간 동안 이전 검증 키를 보존한다. 알 수 없는 key ID로 토큰을 받으면 무제한 조회를 시작하지 말고 bounded refresh 후 실패 처리한다. cursor는 서명만 검증하고 끝내지 않으며 최신 topic 권한을 다시 검사한다.

기존 Kubernetes projected Secret은 노드마다 갱신 시점이 달라 즉시 폐기 경로로 사용할 수 없다. [Secret 갱신 문서](https://kubernetes.io/docs/concepts/configuration/secret/#using-secrets-as-files-from-a-pod)를 따른다. 버전이 있는 immutable 설정과 config hash로 rollout 중의 호환 범위를 확인한다. 토큰, 개인 키, 전체 cursor, 실제 체결 본문을 로그에 넣지 않는다.

broker 접속은 TLS와 최소 ACL을 사용한다. producer만 write 권한, gateway와 replay worker는 필요한 read 권한을 갖는다. gateway에는 Kubernetes API token을 마운트하지 않고 producer의 Lease에는 필요한 namespace의 최소 RBAC만 부여한다. NetworkPolicy는 LB/클라이언트에서 edge, gateway에서 broker/인증 공급자, 필요한 DNS와 관측 경로만 허용하도록 운영 CNI에서 시험한다.

## 배포와 rollback 절차

1. 복제 로그, TLS/ACL, 상태 토픽, retention과 용량부터 준비한다. producer failover를 단독으로 검증한다.
2. 기존 배포와 분리한 Service에서 새 gateway가 같은 committed 데이터로 bootstrap하는지 비교한다. ring 데이터와 broker 데이터가 같다고 가정하지 않고 합성 oracle로 대조한다.
3. 기존 `/ws` 계약과 신규 복구 클라이언트 검증을 통과한 뒤 제한된 트래픽으로 시작한다. 프로토콜 선택은 클라이언트의 명시적 opt-in이다.
4. 직렬 배포 드라이버가 첫 slot만 갱신한다. 해당 Deployment가 신규 Pod의 준비 및 15초 안정화 뒤 이전 Pod 하나를 drain한다. 드라이버는 새 revision의 available replica 1개, 이전 Pod의 완전 종료, 전체 ready replica 3개 및 클라이언트 재접속/replay 검증 성공을 모두 확인한 뒤 다음 slot을 patch한다. 검증 대기 중 나머지 slot의 template은 그대로여서 다음 교체가 시작되지 않는다. 자동 검증과 실패 시 중단, 드라이버 재시작 시험을 #52와 #53의 완료 조건으로 둔다.
5. 준비 실패, quorum 저하, 복구 실패, 오류 예산 초과가 있으면 rollout을 중지한다. 자동 rollback이 데이터나 cursor 형식까지 복구해 준다고 가정하지 않는다.
6. 이전 gateway가 현재 envelope/schema/config를 읽을 수 있을 때만 이전 이미지로 되돌린다. N/N-1 혼합 버전을 먼저 시험하고 파괴적인 schema 변경은 새 log_id와 명시적 client 전환으로 분리한다.

기존 `/ws`에서 신규 복구 프로토콜로 전환하거나 그 반대로 되돌릴 때 보장 수준도 달라진다. events 모드 클라이언트를 legacy 경로로 자동 우회시키지 않는다. broker 데이터와 PVC를 rollback 명령의 정리 대상으로 넣지 않는다. 기존 링 경로와 benchmark 결과는 HA 구현 이후에도 별도로 유지한다.

## 검증 목표와 측정

첫 기준 부하는 1,000개 클라이언트가 기존 두 topic을 구독하고 각 topic에 초당 50건을 흘리는 합성 데이터다. 정상 클라이언트와 읽기를 멈춘 클라이언트를 구분한다. 각 시나리오는 최소 10분, 장애를 3회 반복하며 조건과 CPU, 메모리, 저장소, 버전 및 commit을 결과에 기록한다. 운영 SLO를 확정하기 전 장시간 soak와 실제 다중 장애 도메인 시험을 별도로 수행한다.

| 목표 | 판정 |
| --- | --- |
| 계획된 gateway 교체 | 접속 시도부터 재구독 및 live catch-up까지 p99 5초 이내 |
| gateway/노드 단일 손실 | 살아 있는 경로의 신규 접속 성공률 99.9% 이상, 5초 내 제한 재시도를 하나의 접속 시도로 집계 |
| feed active 손실 | 마지막 정상 source heartbeat부터 신규 leader의 첫 committed 데이터/상태까지 30초 이내 |
| 데이터 복구 | 지원하는 보관 범위와 부하 안에서 미복구 committed event 0, client 적용 중복 0 |
| broker quorum 손실 | 신규 commit을 성공으로 세지 않음, 오래된 데이터를 정상 상태로 표시하지 않음 |
| 인증 폐기 | 모든 gateway와 활성 세션에 60초 안에 반영 또는 fail closed |

source의 event ID와 broker transaction commit 목록을 oracle로 사용한다. events 모드 client가 요청한 시작 cursor 이후 또는 start_at 포함 위치부터 최종 검증 경계까지 권한이 있는 topic의 ID 집합, 순서, 최종 상태와 cursor를 비교한다. state 모드의 의도적인 snapshot 재설정 이전 이력은 전체 수신 oracle에 포함하지 않고 경계 시점의 상태 일치를 별도로 검사한다. 재전송 중 wire 중복은 허용하지만 적용 중복과 조용한 누락은 허용하지 않는다. MESSAGE 개수나 연속 Kafka offset만으로 성공 판정하지 않는다. broker가 기록하지 못한 외부 source 구간은 별도로 집계해 제외 사실을 숨기지 않는다.

| 필수 장애 시험 | 확인할 결과 |
| --- | --- |
| rolling update 및 이전 버전 rollback | 최소 ready 수, 신규 연결, 기존 연결 drain, replay와 호환성 |
| gateway SIGKILL, node drain, node 네트워크 단절 | 통지 없는 재접속, N-1 용량, 다른 Pod의 전체 topic 수신 |
| producer stop 및 긴 정지 후 재개 | fencing, 불명확한 commit, stable ID 및 generator 누적 상태 |
| broker 1개와 다수 broker 손실 | 단일 손실 복구, quorum 부족 시 쓰기 중단, 재시작 후 commit 데이터 |
| snapshot과 live 사이에 이벤트 도착 | 경계 중복 제거, 공백 없음, 조용한 topic 상태 유지 |
| retention 초과, scope/log_id 변경 | 명시적 오류, latest 또는 snapshot으로의 묵시적 우회 없음 |
| 1,000개 동시 재접속과 느린 소비자 | bounded 메모리/태스크, 건강한 소비자의 지연, 재시도 분산 |
| 키/정책 회전 및 공급자 장애 | 일관된 허용/폐기, freshness 만료 후 fail closed |

kind는 여러 node 컨테이너와 별도 client Job으로 Service DNS 및 ClusterIP 경로를 검사한다. port-forward는 진단용으로만 추가한다. 한 호스트의 kind로 실제 zone 손실, 외부 LB endpoint 전파, 저장소의 장애 도메인과 DR을 입증할 수 없다. 운영 승인은 해당 환경의 측정 결과를 필요로 한다.

## 관측과 실행 순서

추가 지표는 ready gateway 수, consumer 적용 지연, source heartbeat 나이, producer fencing 및 commit 실패, replay 대기/실패/bytes, cursor 만료, drain 단계 시간, 정책 freshness와 클라이언트 reconnect/catch-up 시간이다. broker offset이나 event ID를 metric label로 넣지 않는다. 기존 JSON stderr 수집 경로를 사용하며 연결별 로그 대신 장애와 상태 전환을 남긴다.

ready replica가 2개 미만, quorum/ISR 부족, source heartbeat 5초 이상 단절, 누적 consumer 지연 5초 초과, 정책 freshness 만료에 알림을 연결한다. 경보마다 rollout 중지, source 입력 중단, 재접속 유도, broker/인증 복구 중 어떤 조치가 필요한지 runbook을 둔다. 데이터가 없는 휴장 상태와 장애 상태는 별도 신호로 구분한다.

| 순서 | 후속 이슈 | 선행 조건과 완료 기준 |
| --- | --- | --- |
| 1 | [#46 입력 경계와 식별자](https://github.com/yceffort/ws-trade-server/issues/46) | 기존 계약 유지, bounded adapter와 stable envelope |
| 2 | [#47 로그와 producer 복구](https://github.com/yceffort/ws-trade-server/issues/47) | #46, 복제/transaction/fencing/checkpoint 장애 시험 |
| 3 | [#48 gateway 복원](https://github.com/yceffort/ws-trade-server/issues/48) | #46, #47, 전체 fan-out 및 snapshot 경계 |
| 4 | [#49 resume 프로토콜](https://github.com/yceffort/ws-trade-server/issues/49) | #48, 다른 Pod에서 cursor 복구와 client 적용 oracle |
| 4 | [#50 drain 상태 머신](https://github.com/yceffort/ws-trade-server/issues/50) | #48, bounded 종료와 routing 전파 시험 |
| 4 | [#51 인증 일관성](https://github.com/yceffort/ws-trade-server/issues/51) | #46, #48, 키 회전 및 정책 공급자 장애 시험 |
| 5 | [#52 HA 배포](https://github.com/yceffort/ws-trade-server/issues/52) | #47, #48, #50, #51, 실제 LB와 장애 도메인 검증 |
| 6 | [#53 HA 수용 시험](https://github.com/yceffort/ws-trade-server/issues/53) | #49~#52, 위 부하/장애 표 및 운영 runbook |

각 단계는 별도 worktree와 PR로 구현하고 CodeRabbit 지적과 필요한 회귀를 해결한다. Rust 변경에는 `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test`를 적용한다. 기존 프레임을 건드리면 README의 캡처 계약 대조 절차를 수행하고 `contract/`는 직접 편집하지 않는다. 새로운 HA 형식과 임계값은 README의 미관측 실험 목록에 명시한다. 모든 단계의 완료 전에는 기존 single Pod 배포가 HA를 지원한다고 표시하지 않는다.
