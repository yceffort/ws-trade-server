# 별도 HA 배포

기존 `deploy/k8s` 단일 Pod 예제와 분리한 HA 구성이다. `render.mjs`는 운영 환경의 zone, 이미지 digest, StorageClass와 허용 CIDR을 받아 Kubernetes List를 만든다. 선언한 배치, probe, 종료 예산과 직렬 배포의 구현을 제공하지만 실제 외부 LB, 물리 장애 도메인, N-1 용량의 검증 결과를 대신하지 않는다. 운영 전 [HA 검증 계획](../../../docs/ha-rollout.md)의 실제 환경 시험이 필요하다.

## 이미지와 준비

`docker build -f deploy/ha/Dockerfile -t <registry>/ws-trade-ha:<version> .`와 `docker build -f edge/Dockerfile -t <registry>/ws-trade-edge:<version> .`로 만든다. HA 이미지는 기본 이미지와 달리 `ha` feature의 gateway와 ha-feed, loopback 관리 probe를 포함한다. registry에 발행한 digest를 `settings.example.json`의 gatewayImage와 edgeImage에 넣는다. 예시의 0 digest, 문서 전용 CIDR, StorageClass는 실행 전에 반드시 교체한다. broker 이미지는 Kafka 4.1.1 digest로 고정한다.

Kubernetes 1.34 이상, native sidecar, exec probe, default deny NetworkPolicy를 집행하는 CNI, 세 개의 독립 zone을 전제로 한다. 각 zone에 taint 없는 적격 hostname 두 개 이상을 확보하고 `node deploy/k8s/ha/preflight.mjs <context> <settings.json>`으로 배치 조건을 확인한다. 이 검사는 실제 가용 CPU/메모리나 물리 독립성의 증명이 아니다. gateway 정상 3개와 surge 1개, feed 2개, broker 3개의 requests/limits 및 broker 장애 시 ISR/디스크 여유를 별도로 확보한다. `WaitForFirstConsumer`와 zone-aware PVC를 사용하는 StorageClass를 선택한다.

운영자가 다음 immutable Secret을 namespace에 공급한다. version은 settings.version과 일치해야 한다. private key, 토큰과 SASL 비밀번호를 저장소나 로그에 넣지 않는다. source.json의 log_id/epoch/topics는 초기화 전에 운영 입력 계약에 맞게 확정하며 이후 같은 log_id에서 변경하지 않는다.

| Secret | 필요한 파일과 역할 |
| --- | --- |
| wts-broker-VERSION | cluster-id, broker0.properties~broker2.properties, broker0.pem~broker2.pem, ca.crt, health.properties. broker/controller 전용 TLS 자료와 broker health credential |
| wts-admin-VERSION | admin.properties, ca.crt. 초기 topic/ACL Job 전용 admin credential, truststore 경로 /admin/ca.crt |
| wts-feed-VERSION | feed.json, ca.crt. TLS/SASL feed 역할, CA 경로 /credentials/ca.crt |
| wts-gateway-VERSION | gateway.json, ca.crt, cursor-keys.json, policy-client.json 및 선택적 공급자 credential. gateway 읽기 권한만 |
| wts-tls-VERSION | cert.pem, key.pem. 클라이언트가 검증하는 edge TLS 체인 |

broker 이름은 `wts-broker-{0,1,2}.wts-broker.NAMESPACE.svc`, node.id는 0~2다. INTERNAL SASL_SSL 9092, mTLS CONTROLLER 9093, 고정 KRaft voters 세 개, RF=3, min ISR=2, transaction 내부 토픽 RF=3/min ISR=2, 자동 토픽 생성 금지, unclean election 금지를 사용한다. `/config`의 인증서와 `/var/lib/kafka/data`의 PVC를 참조한다. CA의 SAN에는 실제 broker DNS가 있어야 한다. admin/broker와 controller 인증서 주체만 super.users로 등록하고 feed/gateway 권한은 bootstrap의 ACL로 제한한다. dev-secrets.mjs의 합성 설정을 운영 자격 증명으로 사용하지 않는다.

정책 공급자는 [공유 인증 계약](../../../docs/ha-auth.md)을 구현한 외부 HTTPS 서비스다. issuer/audience, topic 권한과 version을 모든 gateway에 동일하게 제공한다. 공급자 자체의 HA와 정책 일관성은 별도로 검증해야 한다. gateway는 Kubernetes API token을 마운트하지 않는다. feed는 이름이 고정된 Lease의 get/update 권한만 사용하며 projected token을 별도 경로에 마운트한다.

## 처음 배포

모든 kubectl 호출에 대상 context와 namespace를 명시한다. 렌더러는 secret 값을 출력하지 않지만 Secret 적용 입력은 별도로 취급한다. 처음에는 foundation, topic/ACL 및 source 초기화, workload 순서를 지킨다. 빈 데이터셋을 초기화하는 Job을 이미 운용 중인 log에 재실행하지 않는다.

```sh
node deploy/k8s/ha/render.mjs settings.json foundation > foundation.json
kubectl --context CONTEXT apply -f foundation.json
# namespace에 위 immutable Secret을 공급한 뒤 broker Ready를 기다린다.
kubectl --context CONTEXT -n NAMESPACE wait --for=jsonpath='{.status.readyReplicas}'=3 statefulset/wts-broker --timeout=300s
node deploy/k8s/ha/render.mjs settings.json bootstrap > bootstrap.json
kubectl --context CONTEXT apply -f bootstrap.json
kubectl --context CONTEXT -n NAMESPACE wait --for=condition=complete job/wts-bootstrap-VERSION --timeout=300s
node deploy/k8s/ha/render.mjs settings.json workloads > workloads.json
kubectl --context CONTEXT apply -f workloads.json
```

gateway slot a/b/c는 zone에 하나씩 고정되고 공통 required hostname anti-affinity를 사용한다. slot별 replicas=1, maxUnavailable=0/maxSurge=1, minReadySeconds=15다. 공통 PDB minAvailable=2는 자발적 eviction에만 적용한다. rollout, 강제 삭제, 노드 장애는 PDB가 막지 않는다. broker StatefulSet은 별도 PVC와 OnDelete 전략을 사용하며 gateway rollout 도구는 broker나 feed를 변경하지 않는다. broker PVC는 scale/delete에도 보존한다.

관리 HTTP는 loopback에만 열고 gateway exec probe로 `/livez`와 `/readyz`를 검사한다. edge TCP probe도 통과해야 Pod Ready가 된다. preStop은 즉시 drain을 시작하고 20초 기다린다. 전체 60초 grace 안에서 routing 20초, 재접속 분산 10초, flush 10초와 정리/sidecar 여유 20초를 사용한다. native sidecar는 gateway 종료 뒤에 내려간다. 실제 LB의 endpoint 제거 지연은 고정 대기 시간으로 입증되지 않으므로 직접 측정한다.

NetworkPolicy는 기본 거부 후 필요한 경로만 허용한다. policyCIDRs는 외부 정책 공급자, apiCIDRs는 Service DNAT 이후 실제 API server 주소까지 포함한다. clientCIDRs는 LoadBalancer 외부 클라이언트의 허용 범위이며 Service loadBalancerSourceRanges로 렌더링된다. Pod가 관측하는 SNAT 이후 source 주소는 gatewayIngressCIDRs에 지정한다. Service loadBalancerSourceRanges와 CNI source-NAT 동작은 제공자마다 다르므로 실제 연결과 거부 시험을 수행한다. 관리 endpoint를 Service에 공개하지 않는다.

## 직렬 배포와 rollback

초기 설치 이후 세 gateway Deployment를 일괄 apply하거나 GitOps로 동시에 변경하지 않는다. 새 version의 source ConfigMap과 Secret을 준비하되 log_id/epoch/topics 및 인증 호환성을 유지한다. 새 `settings`의 foundation 전체를 적용하면 broker Secret 참조도 바뀌므로 gateway-only 배포에서는 새 ConfigMap과 Secret만 명시적으로 준비한다.

다음 plan 파일과 검증 도구를 준비한다. 검증 도구는 실제 서비스 주소에서 재접속과 committed event 적용을 대조해야 하며 단순 TCP 성공이나 프로세스 생존으로 대체하지 않는다.

```json
{"plan":{"version":"v2","gatewayImage":"registry/gateway@sha256:<64 hex>","edgeImage":"registry/edge@sha256:<64 hex>"},"verifyCommand":["node","path/to/verified-reconnect-oracle.mjs"]}
```

`node deploy/k8s/ha/rollout.mjs CONTEXT NAMESPACE plan.json`은 환경 ConfigMap 잠금을 획득하고 원래 Pod template, 목표 template, 단계와 결과를 `wts-rollout-state`에 resourceVersion CAS로 보존한다. 현재 slot의 새 revision available=1, 이전 Pod 완전 종료, 전체 세 slot 안정화와 Pod Ready=3, oracle 성공을 모두 확인한 뒤 다음 slot을 patch한다. 일치하지 않는 수동 template 변경, 준비 실패, 검증 실패에서는 멈춘다. 동일 plan으로 재실행하면 완료된 slot을 건너뛰고 진행 중 slot의 현재 revision과 검증을 다시 확인한다. patch 직후 journal 기록 전에 중단돼도 목표 template을 재인식한다.

검증 명령은 `WTS_ROLLOUT_REQUEST` JSON으로 phase, planHash, slot, nonce, window와 rollback 여부를 받는다. patch 전 `phase=begin`에서는 해당 측정 구간을 시작하고 동일 planHash/slot/nonce/window와 `ok:true`로 응답해야 한다. 현재 Pod UID 목록이 추가된 `phase=verify`에서는 해당 window에서 시작한 재접속과 committed 적용을 비교하고 동일 식별자와 `ok:true`, `committed_missing:0`, `duplicate_applies:0`를 반환한다. stdout에는 JSON 하나만 출력한다. 이전 slot이나 배포 전 표본을 재사용하지 않으며 journal의 window는 재시작에도 유지한다. 실행 시간 10분, 출력 64 KiB 상한이며 그 외 오류는 다음 slot의 교체를 막는다. 배포 계정은 신뢰한 검증 명령만 사용한다. 실제 oracle 구현과 부하 조건은 장애 검증 작업에서 관리한다.

SIGTERM/SIGINT는 다음 patch를 중단하고 finally에서 잠금을 해제하며 journal을 보존한다. SIGKILL이나 호스트 손실에서는 잠금을 자동으로 탈취하지 않는다. 오래 멈춘 이전 드라이버가 다시 patch하는 것을 방지하기 위해서다. 운영자가 기존 드라이버 프로세스가 완전히 종료됐음을 확인하고 lock ConfigMap의 owner/created_at을 기록한 뒤 `kubectl --context CONTEXT -n NAMESPACE delete configmap wts-rollout-lock`을 수행한다. 살아 있는 드라이버의 잠금을 지우지 않는다. 이후 동일 plan으로 재개한다. RBAC의 ConfigMap create는 이름을 제한할 수 없으므로 전용 namespace와 배포 계정 사용을 전제로 한다.

`node deploy/k8s/ha/rollout.mjs CONTEXT NAMESPACE plan.json rollback`은 저장한 이전 Pod template 전체를 역순으로 복원하고 slot마다 같은 검증을 수행한다. 이미지뿐 아니라 version이 있는 설정 참조도 돌아간다. broker/PVC, 인증 공급자 정책 version과 데이터 schema를 되돌리지는 않는다. 이전 버전이 현재 log와 cursor를 읽는다는 호환성 검증이 선행되어야 한다. 현재 source와 호환되지 않는 rollback은 수행하지 않는다.

## 단일 호스트 개발 시험

`kind.yaml`은 control-plane 1개와 zone label이 있는 worker 6개를 만든다. 실제 물리 zone이 아니며 기본 kind CNI는 NetworkPolicy 강제 적용을 검증하지 못한다. 기존 context를 변경하지 말고 별도 kubeconfig와 이름으로 생성한다. `dev-secrets.mjs settings.json deploy/local/ha-k8s`는 새 합성 인증서, 역할 credential, JWT와 개발 전용 단일 policy Pod를 생성한다. 결과 디렉터리는 0700, Secret JSON은 0600이며 Git에 넣지 않는다. 같은 디렉터리가 있으면 cluster identity를 조용히 바꾸지 않고 실패한다. 개발 인증 자료는 7일, client JWT는 1시간이다.

기본 구조와 배포 중단/재개/실패/rollback 회귀는 `node --test deploy/k8s/ha/rollout.test.mjs`로 실행한다. 실제 kind 시험과 운영 환경 수용 시험은 별도의 증거를 보존한다. 단일 호스트 결과로 1,000 client SLO, 실제 LB drain이나 zone 손실의 가용성을 주장하지 않는다.

Kubernetes 동작은 [native sidecar](https://kubernetes.io/docs/concepts/workloads/pods/sidecar-containers/), [Pod 배치 제약](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/), [Lease](https://kubernetes.io/docs/concepts/architecture/leases/), [disruption](https://kubernetes.io/docs/concepts/workloads/pods/disruptions/) 문서를 기준으로 한다.

kind에 로컬 이미지를 넣을 때 `node deploy/k8s/ha/kind-images.mjs wts-ha-NAME ws-trade-ha:VERSION ws-trade-edge:VERSION`을 사용하면 각 node의 실제 OCI digest를 확인하고 digest 참조를 등록한다. 출력한 digest를 개발 settings에 넣는다. `client-job.mjs settings.json`은 Service 내부에서 TLS, 공통 인증과 cursor 재접속을 검사하는 별도 Job을 렌더링한다. 이 간단한 smoke는 committed event oracle나 SLO 검증을 대체하지 않는다.

`version`은 bootstrap Job 이름의 63자 제한을 지키도록 49자 이하로 설정한다. `clientCIDRs`는 LoadBalancer의 외부 클라이언트 허용 범위이고, `gatewayIngressCIDRs`는 Pod에 실제 도착하는 패킷의 출발지 범위다. `externalTrafficPolicy: Cluster`에서 SNAT가 발생하는 환경은 관측한 노드 또는 LB 주소 범위를 후자에 지정해야 한다. 운영 CNI에서 허용된 외부 WSS 접속과 비허용 주소 차단을 각각 확인해야 하며, kind 기본 CNI 시험은 이 검증을 대신하지 않는다.

Rollback은 실제 template이 바뀐 슬롯을 역순으로 복구한 뒤 변경하지 않은 슬롯을 확인한다. 실패한 슬롯 자체는 unready여도 다른 두 슬롯이 15초 이상 Ready이고 종료 중인 Pod가 없으면 복구를 시작한다. 복구한 슬롯의 안정화와 committed oracle 확인을 마치기 전에는 다음 슬롯을 변경하지 않는다. 손상된 journal은 자동 초기화하지 않고 중단한다.
