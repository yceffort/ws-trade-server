# AWS 실측 환경

[#90](https://github.com/yceffort/ws-trade-server/issues/90)과 [#91](https://github.com/yceffort/ws-trade-server/issues/91)의 측정에만 쓰는 임시 EKS 구성이다. 운영 구성이 아니며 측정이 끝나면 지운다. 결과는 [용량 시험 기록](../../docs/ha-capacity.md)의 AWS 실측 절과 `bench/results/ha/aws-*`에 있다.

| 파일 | 하는 일 |
| --- | --- |
| `cluster.yaml` | eksctl 구성. 서울 리전 AZ 3개, EKS 1.35, AZ마다 4 vCPU Graviton spot 노드 2대, 부하 전용 on-demand 노드 그룹(기본 0대) |
| `up.sh` | 클러스터 생성, 노드 자동 축소 예약, NLB 전용 /28 서브넷, xfs StorageClass, AWS Load Balancer Controller |
| `measure.sh` | 부하 Job 하나를 돌리면서 HA 지표, 노드와 컨테이너 CPU, edge 사유 로그, 부하 Pod cgroup을 함께 수집 |
| `az-fault.sh` | AZ 하나의 서브넷을 전부 거부하는 NACL로 바꾸거나, 그 AZ의 서버 인스턴스를 종료 |
| `down.sh` | namespace, NLB 서브넷, 클러스터, ECR 저장소를 지우고 남은 자원을 세어 0이 아니면 실패 |

## 순서

준비물은 AWS 자격 증명, eksctl, helm, docker다. 아래 명령은 저장소 루트에서 실행한다. `up.sh`는 kubeconfig를 `deploy/local/aws/kubeconfig`에만 쓰므로 이후 kubectl 호출에는 `KUBECONFIG`를 그 파일로 지정한다.

1. `deploy/ha/Dockerfile`, `edge/Dockerfile`, `bench/ha/Dockerfile`로 이미지 세 개를 만들어 ECR 저장소 `wts-ha/gateway`, `wts-ha/edge`, `wts-ha/load`에 올리고 digest를 적어 둔다. 노드가 arm64이므로 arm64로 빌드한다.
2. `deploy/aws/up.sh 7`을 실행한다. 인자는 모든 노드 그룹을 0대로 줄일 때까지의 시간이다.
3. settings를 만든다. `zones`는 AZ 이름 세 개, `storageClass`는 `wts-gp3`, `gatewayIngressCIDRs`는 NLB 서브넷 세 개(`192.168.192.0/28`, `.16/28`, `.32/28`), `clientCIDRs`는 VPC CIDR, `apiCIDRs`는 `kubernetes` Service의 ClusterIP와 endpoint 주소다. `gatewayServiceAnnotations`에는 `service.beta.kubernetes.io/aws-load-balancer-type: external`, `aws-load-balancer-nlb-target-type: ip`, `aws-load-balancer-scheme: internal`, `aws-load-balancer-subnets: <NLB 서브넷 ID 세 개>`를 넣는다.
4. `render.mjs settings.json foundation`을 적용하고 gateway Service에 NLB 이름이 붙기를 기다린다. 그 이름을 settings의 `gatewayNames`에 넣은 뒤 `dev-secrets.mjs`를 실행해 `secrets.json`과 `development.json`을 적용한다. 인증서 SAN에 NLB 이름이 들어가야 클라이언트가 TLS를 검증한다. 새 Pod가 NLB target으로 healthy가 된 뒤에 Ready가 되게 하려면 workloads를 적용하기 전에 namespace에 `elbv2.k8s.aws/pod-readiness-gate-inject=enabled` label을 붙인다.
5. broker 3대가 Ready가 되면 `bootstrap`, `workloads` 순서로 적용한다.
6. NetworkPolicy 집행은 `aws eks update-addon --cluster-name wts-ha-aws --addon-name vpc-cni --configuration-values '{"enableNetworkPolicy":"true"}'`로 켠다. 이번 측정은 집행을 끈 상태에서 NLB 경로를 먼저 확인하고 켰다.
7. 부하 노드는 `aws eks update-nodegroup-config --cluster-name wts-ha-aws --nodegroup-name load --scaling-config minSize=0,maxSize=1,desiredSize=1`로 켠다. `options.json`에는 NLB 주소의 `urls`, NLB 서브넷의 `externalEgress`, `nodeSelector: {"wts/role":"load"}`와 `dedicated=load:NoSchedule` toleration을 넣는다.
8. 실행마다 `dev-token.mjs`로 client token을 새로 만들어 `wts-dev-client-VERSION` Secret을 교체한 뒤 `deploy/aws/measure.sh settings.json options.json 출력디렉터리`를 실행한다. token 수명이 한 시간이라 그보다 긴 측정은 token을 바꾸지 않으면 `auth-expired`로 끝난다.
9. 계획 배포는 부하 Job이 초기 접속을 마친 뒤 `rollout.mjs`를 `kube-verify.mjs`와 함께 실행한다. 이번 측정은 readiness gate가 없는 상태를 먼저 재려고 label을 나중에 붙였고, 그 뒤에 만들어진 Pod부터 gate가 적용됐다.
10. `deploy/aws/down.sh`로 지운다. 마지막 줄이 "남은 자원 없음"인지 확인한다. 점검은 리전의 EC2, EBS, ELB를 전부 세므로 같은 리전에 다른 자원이 있는 계정에서는 그만큼 남은 것으로 나온다.

## kind와 달랐던 것

2026-11-22 측정에서 관측한 것만 적는다.

- EBS의 ext4 볼륨은 최상위에 `lost+found`가 있고 Kafka는 로그 디렉터리에 그런 항목이 있으면 시작하지 않았다. StorageClass의 파일시스템을 xfs로 바꿔 해결했다.
- annotation 없는 `type: LoadBalancer` Service는 IP target NLB가 되지 않는다. target 종류와 scheme은 Service를 만들 때의 annotation으로 정한다.
- 숫자 targetPort를 쓰는 Service(정책 공급자, 443에서 8443)는 정책 컨트롤러가 ClusterIP와 Service port를 허용 목록에 넣었다. 이름 붙은 targetPort를 쓰는 gateway Service의 ClusterIP는 넣지 않았다. 그래서 클러스터 안에서 `wts-gateway.NAMESPACE.svc`로 붙는 경로는 정책을 켠 상태에서 막혔다.
- 돌아가는 클러스터에서 정책 집행을 켜는 순간 active feed가 broker 연결을 약 9초 잃고 `ha-deadline`으로 재시작했다.
- readiness gate가 없으면 slot을 교체할 때마다 새 target이 healthy가 되기 16초 전에 옛 target이 draining으로 넘어갔다. gate를 켜면 그 구간이 없었다.
- edge의 `tls_aborted` 기준선은 Pod마다 10초에 9건이었다. kind의 6건에 NLB health check가 더해진 값이다.
- spot 노드는 세 번 교체됐다(재조정 권고). CoreDNS 두 개가 모두 한 AZ에 있어서 AZ 장애는 CoreDNS가 없는 AZ에 주입했다.
- `job-run.mjs`는 kubectl 호출이 한 번 실패하면 보고서 없이 끝난다([#96](https://github.com/yceffort/ws-trade-server/issues/96)). `measure.sh`가 그때 부하 Pod의 보고서를 직접 기다린다.

## 비용

시간으로 붙는 요금은 시간당 약 0.6달러였고 부하 노드를 켜면 약 0.93달러였다(spot c7g.xlarge와 m7g.xlarge 6대, EKS 제어 영역 0.10달러, NLB 0.0225달러, on-demand c7g.2xlarge 0.3264달러). 트래픽으로 붙는 요금이 그보다 컸다. 1,000 clients는 NLB로 초당 약 57MB를 받았고 측정 전체에서 NLB가 288GB를 처리했다. NLB 용량 단위 요금(GB당 0.006달러)과 AZ 간 전송 요금(방향마다 GB당 0.01달러)이 여기에 붙는다. 부하 노드가 한 AZ에 있고 cross-zone이 꺼져 있어 트래픽의 3분의 2가 AZ를 넘는다. 1,000 clients를 15분 돌리는 한 번에 트래픽 요금만 약 1달러다. NAT gateway와 FIS는 쓰지 않았다.
