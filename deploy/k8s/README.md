# Kubernetes 실행

Kubernetes 1.33 이상에서 feed, gateway, TLS edge를 한 Pod로 실행한다. feed와 gateway는 같은 파일을 mmap하므로 메모리 기반 `emptyDir`을 공유한다. gateway는 링을 읽기 전용으로 마운트한다. Pod의 네트워크와 볼륨 공유는 [Pod 문서](https://kubernetes.io/docs/concepts/workloads/pods/), 볼륨 수명은 [emptyDir 문서](https://kubernetes.io/docs/concepts/storage/volumes/#emptydir)를 따른다.

Deployment는 replicas 1과 Recreate를 사용한다. Pod를 교체하면 링, snapshot, replay가 사라지고 재접속이 필요하다. replica 수를 늘리면 서로 다른 합성 feed가 생기므로 수평 확장이나 고가용성 구성으로 사용할 수 없다. Recreate는 업데이트 중 중단을 허용한다. 실서비스의 데이터 공급 분리와 상태 복구는 별도 설계가 필요하다.

edge는 `initContainers`의 `restartPolicy: Always`를 쓰는 native sidecar다. TLS listener가 열린 뒤 본 컨테이너를 시작하고, gateway 종료 뒤에 edge를 종료한다. 따라서 gateway의 3초 drain 동안 종료 프레임을 전달할 수 있다. Pod의 종료 유예는 15초다. [Kubernetes sidecar 문서](https://kubernetes.io/docs/concepts/workloads/pods/sidecar-containers/)에서 이 종료 순서와 1.33의 stable 지원을 확인할 수 있다.

## 로컬 검증

Docker, kind 0.32.0, kubectl 1.35, Node 22 이상, OpenSSL과 Bash가 필요하다. 저장소 루트에서 실행한다.

```sh
docker compose build
npm ci --prefix edge
bash deploy/kind-smoke.sh
```

검증은 digest로 고정한 Kubernetes 1.35.5 kind 이미지를 사용한다. 별도 클러스터 이름과 임시 KUBECONFIG를 만들기 때문에 기존 kubectl context를 바꾸지 않는다. 로컬 이미지를 적재한 뒤 API 서버 검증, 실제 배포, 클라이언트 Job에서 Service DNS 및 ClusterIP를 경유한 CA 검증 WSS 연결, 두 topic 수신, 잘못된 토큰 거절, feed 컨테이너 재시작 후 복구, Deployment 교체 때 종료 프레임, 새 Pod 수신과 Service 경로 복구를 확인한다. 임시 인증서에는 Service DNS 이름을 SAN으로 추가하고 TLS 호스트 검증을 유지한다. port-forward는 종료 프레임 확인용으로 함께 사용한다. 성공과 실패 모두 테스트 클러스터와 임시 인증 자료를 정리한다. GitHub Actions `Deployment`도 같은 검증을 실행한다.

## 다른 클러스터에 적용하기

아래 명령은 현재 kubectl context에 적용한다. 먼저 배포 대상 context를 선택하고, 클러스터 CPU 아키텍처용 이미지를 레지스트리에 올려 둔다. 저장소는 클라우드 계정이나 기존 클러스터를 자동 설정하지 않는다. `ws-trade` namespace는 이 데모 전용으로 사용한다.

```sh
kubectl config current-context
kubectl apply -f deploy/k8s/namespace.yaml
kubectl -n ws-trade create secret generic ws-trade-credentials \
  --from-file=tokens.json=/secure/ws-trade/tokens.json \
  --from-file=cert.pem=/secure/ws-trade/cert.pem \
  --from-file=key.pem=/secure/ws-trade/key.pem
```

인증 파일은 외부에서 제공한다. localhost 데모에는 `node deploy/prepare.mjs`로 만든 파일을 쓸 수 있다. 외부 호스트명으로 접속할 때는 그 이름을 포함한 신뢰 가능한 인증서가 필요하다. manifest에는 Secret 값을 포함하지 않는다. Secret은 암호화된 보관소 자체가 아니므로 클러스터의 접근 제어와 저장 시 암호화는 운영 환경에서 설정한다. 파일은 0440이며 Pod의 fsGroup 10001로 읽는다. `subPath` 없이 디렉터리를 마운트해 토큰 파일 갱신을 받을 수 있고 gateway가 주기적으로 다시 읽는다. kubelet의 반영 지연이 있으므로 즉시 폐기를 보장하지 않는다. 인증서 갱신은 rollout restart가 필요하다. [Secret 문서](https://kubernetes.io/docs/concepts/configuration/secret/)를 참고한다.

레지스트리 이미지 주소는 별도 Kustomize overlay에서 바꾼다. 아래는 `deploy/overlays/cluster/kustomization.yaml`의 예다. 이름과 digest는 실제 배포 값으로 교체한다. 비공개 레지스트리라면 imagePullSecrets도 추가한다.

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources: [../../k8s]
images:
  - name: ws-trade-server
    newName: registry.example.com/ws-trade-server
    digest: sha256:REPLACE_WITH_SERVER_DIGEST
  - name: ws-trade-edge
    newName: registry.example.com/ws-trade-edge
    digest: sha256:REPLACE_WITH_EDGE_DIGEST
```

```sh
kubectl apply --dry-run=server -k deploy/overlays/cluster
kubectl apply -k deploy/overlays/cluster
kubectl -n ws-trade rollout status deployment/ws-trade
kubectl -n ws-trade port-forward --address=127.0.0.1 service/ws-trade 8443:443
```

Service는 ClusterIP의 WSS 443만 노출한다. 평문 WS 8080과 관리 HTTP 8081은 Pod loopback에 바인딩한다. 외부 진입점은 준비하지 않는다. Ingress나 LoadBalancer를 붙일 때 WebSocket 업그레이드, TLS 종료 위치, 연결 유휴 시간과 인증서를 환경에 맞춰 정한다. 클러스터 내부의 다른 Pod에서 WSS 접근을 제한하려면 CNI가 지원하는 NetworkPolicy를 별도로 설정한다.

readiness는 두 topic 모두 최근 10초 이내의 snapshot이 있는지 검사한다. feed 정지 시 Service 대상에서 빠지지만 기존 WebSocket은 자동으로 끊기지 않는다. gateway liveness는 관리 HTTP의 응답만 확인해 데이터가 잠시 없다는 이유로 재시작하지 않는다. feed 프로세스가 종료되면 kubelet이 다시 시작한다. edge probe는 TCP listener만 검사한다. 실거래의 휴장과 거래 빈도는 이 합성 feed 정책에 포함되지 않는다.

```sh
kubectl -n ws-trade logs deployment/ws-trade -c gateway --follow
kubectl -n ws-trade exec deployment/ws-trade -c gateway -- curl -fsS http://127.0.0.1:8081/metrics
kubectl -n ws-trade rollout restart deployment/ws-trade
kubectl delete -k deploy/k8s
```

마지막 명령은 데모 namespace와 그 안의 Secret도 삭제한다. gateway는 JSON 이벤트를 stderr로 출력하고 edge와 feed는 stderr에 기동 및 오류를 기록한다. 로그 수집기는 클러스터의 stdout/stderr 수집 경로에 연결한다. 자원 request/limit는 합성 feed를 위한 시작값이며 부하 테스트로 조정해야 한다. 자동 확장, 무중단 배포, 인증서 자동 발급과 알림 연동까지 갖춘 운영 플랫폼은 아니다.
