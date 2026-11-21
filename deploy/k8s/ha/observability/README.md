# HA 관측 스택

[HA 배포](../README.md)의 지표와 Pod 로그를 모으는 개발용 Prometheus, Loki, Grafana Alloy, Grafana 구성이다. 모두 replica 하나와 파일시스템 저장소를 쓰므로 수집 경로와 질의를 재현하는 용도이고, 관측 스택 자체의 고가용성이나 장기 보존을 제공하지 않는다. `scrape.yaml`, `prometheus-rules.yaml`, `loki-rules.yaml`은 그대로 두고 이 디렉터리의 `kustomization.yaml`이 함께 묶는다.

## 버전

| 구성 요소 | 이미지 | 실행 사용자 |
| --- | --- | --- |
| Prometheus | `prom/prometheus:v3.5.0@sha256:63805ebb8d2b3920190daf1cb14a60871b16fd38bed42b857a3182bc621f4996` | 65534 |
| Loki | `grafana/loki:3.5.3@sha256:3165cecce301ce5b9b6e3530284b080934a05cd5cafac3d3d82edcb887b45ecd` | 10001 |
| Grafana Alloy | `grafana/alloy:v1.10.0@sha256:1bc130e909116a4bf950eb5187a346007d3b796f96832b529acb14a64e41230f` | 473 |
| Grafana | `grafana/grafana:12.1.0@sha256:6ac590e7cabc2fbe8d7b8fc1ce9c9f0582177b334e0df9c927ebd9670469440f` | 472 |

## 설치

HA 배포의 settings에 `"metricsNamespace": "wts-observability"`를 처음 배포할 때부터 넣는다. 이 값이 있어야 gateway Pod에 metrics sidecar와 `wts-ha-metrics` Service, 이 namespace에서만 9090을 허용하는 NetworkPolicy가 생긴다. 렌더러는 sidecar 스크립트를 immutable source ConfigMap(`wts-source-VERSION`)에 넣으므로, 이미 배포한 version에 나중에 켜면 그 ConfigMap을 갱신할 수 없다. 그때는 version을 올려 새 ConfigMap과 Secret으로 배포한다. 같은 이름의 ConfigMap을 지우고 다시 만들면 kubelet이 immutable ConfigMap을 다시 읽지 않아, 옛 ConfigMap을 쓰는 Pod가 남은 노드의 새 Pod는 옛 내용을 받는다(kind에서 gateway 새 Pod 3개 중 feed와 같은 노드에 놓인 2개가 sidecar 스크립트를 찾지 못했다).

```sh
kubectl --context CONTEXT apply -k deploy/k8s/ha/observability
kubectl --context CONTEXT -n wts-observability create secret generic grafana-admin --from-literal=password="$(openssl rand -hex 16)"
kubectl --context CONTEXT -n wts-observability rollout status deployment/prometheus deployment/alloy deployment/grafana statefulset/loki
kubectl --context CONTEXT -n wts-observability port-forward svc/grafana 3000:3000
```

Grafana는 `grafana-admin` Secret이 생길 때까지 시작하지 않는다. 사용자 이름은 admin이고 비밀번호는 Secret에만 있다. `prometheus-rules.yaml`의 `wts_ha_expected_namespace`는 실제 HA namespace로 바꾼다(기본값 `wts-ha`). 바꾸지 않으면 다른 namespace에 배포했을 때 `WtsHaMetricsMissing`이 계속 울린다. 설정 파일은 CI(`.github/workflows/ha.yml`)가 `promtool check config`, `loki -verify-config`, `alloy fmt`로 검사한다.

## 무엇을 모으는가

Prometheus는 `scrape.yaml`의 `wts-ha` job으로 gateway sidecar의 `/metrics`를 5초마다 읽는다. sidecar가 넘기는 `/ha-metrics`에는 HA 지표와 일반 gateway 지표(`wts_*`)가 함께 들어 있다. `wts-observability` job은 같은 namespace의 Alloy와 Loki 자체 지표를 15초마다 읽는다.

Alloy는 Kubernetes API로 `app.kubernetes.io/part-of=wts-ha` Pod의 stdout과 stderr를 읽어 Loki로 보낸다. 노드 파일시스템이나 root 권한이 필요 없고 ClusterRole은 pods, pods/log의 읽기뿐이다. stream label은 값의 수가 정해진 것만 둔다.

| label | 값 |
| --- | --- |
| `namespace` | Pod namespace |
| `component` | Pod의 `app.kubernetes.io/component`(gateway, feed, broker, bootstrap 등) |
| `container` | gateway, edge, metrics, feed, broker 등 |
| `version` | Pod annotation `wts/config-version` |
| `level` | JSON 줄의 `level`(gateway, feed, edge만) |
| `job` | `loki.source.kubernetes.wts` 고정 |

Pod 이름은 label이 아니라 구조화 메타데이터 `pod`로 두므로 `{container="gateway"} | pod="POD"`로 찾는다. Alloy가 붙이는 `instance`(namespace/pod:container)는 Pod마다 늘어나므로 지우고, Loki의 `service_name` 자동 생성은 끈다. conn 같은 상세 식별자는 본문 JSON에만 있다. JSON이 아닌 broker 줄은 그대로 보낸다. Pod를 지운 뒤 Kubernetes API가 로그 대신 돌려주는 오류 문장(`unable to retrieve container logs for ...`)은 앱 로그로 저장하지 않고 `loki_process_dropped_lines_total{reason="kubernetes_log_unavailable"}`로만 센다.

```logql
{namespace="wts-ha", component="feed"} | json event="fields.event", reason="fields.reason" | event=~"ha_producer_active|ha_producer_stopped"
{namespace="wts-ha", container="edge"} | json | fields_event="edge_summary"
sum by (container) (count_over_time({namespace="wts-ha"} | json | __error__!="" [10m]))
```

## 보존, 저장, 자원

| 구성 요소 | 보존과 한도 | 저장 | requests / limits |
| --- | --- | --- | --- |
| Prometheus | 24시간 또는 1GB 중 먼저 닿는 쪽 | PVC 2Gi | 100m, 256Mi / 1 CPU, 512Mi |
| Loki | 72시간, 초당 4MB(버스트 8MB), stream당 초당 1MB, series당 label 10개, 168시간보다 오래된 줄 거절 | PVC 2Gi | 100m, 256Mi / 1 CPU, 512Mi |
| Alloy | 기본 재시도(500ms부터 5분까지 늘려 10번) | emptyDir | 50m, 128Mi / 500m, 256Mi |
| Grafana | 데이터 소스는 파일로 provision, 자체 상태는 보존하지 않음 | emptyDir | 50m, 128Mi / 500m, 256Mi |

PVC는 클러스터의 기본 StorageClass를 쓴다. 수신 한도를 넘는 push는 Loki가 거절하고, Alloy가 재시도를 다 쓰면 `loki_write_dropped_entries_total`이 늘어난다.

## 접근 경계

네 구성 요소 모두 ClusterIP Service만 둔다. namespace는 restricted Pod Security를 강제하고 컨테이너는 비root, 읽기 전용 루트 파일시스템, 모든 capability 제거로 실행한다. NetworkPolicy는 들어오는 연결을 기본 거부하고 Loki는 Alloy, Grafana, Prometheus에서만, Prometheus는 Grafana에서만, Alloy 지표는 Prometheus에서만 받는다. Grafana로 들어오는 경로는 없고 port-forward로만 연다. 익명 접근과 사용 통계 전송은 끈다. kind 기본 CNI는 NetworkPolicy를 집행하지 않으므로 실제 CNI에서 허용과 거부를 따로 확인해야 한다.

## 앱 로그 폐기와 수집기 실패

gateway의 로그 출력 큐가 넘쳐 버린 줄은 `wts_logs_dropped_total`로, 수집기가 Loki에 보내지 못한 줄은 Alloy의 `loki_write_dropped_entries_total`과 `loki_write_batch_retries_total`로 따로 본다. 앱은 stdout과 stderr에 쓰고 kubelet이 파일로 남기며 Alloy는 API로 읽기만 하므로, 수집기나 Loki가 멈춰도 체결 처리를 막지 않는다.

## kind에서 확인한 것

2026-11-22에 kind(control-plane 1개와 worker 6개, Kubernetes v1.37.0, Docker Desktop 4 CPU와 8GB)에 HA 개발 배포(namespace `wts-obs`)와 이 스택을 올려 확인했다. 단일 호스트 결과라 실제 장애 도메인이나 네트워크 정책 집행의 검증이 아니다.

- Prometheus가 gateway 세 개의 sidecar와 Alloy, Loki를 모두 `up`으로 수집했다. Grafana에는 Prometheus와 Loki 데이터 소스가 자동 등록됐고, Grafana를 거친 질의로 Ready gateway 수(3)와 gateway 로그 줄 수를 읽었다.
- stream label은 위 표의 여섯 개뿐이었다. `instance`, `pod`, `service_name`을 지우기 전에는 `instance` 값이 이미 23개였다.
- 30분 동안 gateway 722줄 중 716줄, edge 414줄 중 411줄, feed 3줄 모두가 JSON으로 읽혔다. 못 읽은 줄은 교체 전 edge 이미지의 평문 시작 줄 3개와, Pod 삭제 뒤 API가 돌려준 오류 문장 6개였다. 오류 문장은 이후 위의 drop 단계로 버린다.
- 지운 gateway Pod 두 개의 로그(424줄, 191줄)를 마지막 `edge_stopped`까지 Pod 이름으로 다시 찾았다. 교체 전 feed Pod 두 개의 로그도 남아 있었다.
- active feed의 프로세스를 25초 멈추자 Lease가 standby로 넘어갔고, 다시 깨운 옛 writer는 `ha_producer_stopped`(`reason="lease-lost"`)를 남기고 종료 코드 1로 재시작했다. 두 writer가 모두 Lease를 가졌다고 믿을 때 나올 `producer-fenced`는 재현하지 못했으므로, `loki-rules.yaml`의 `WtsHaUnexpectedFencing`이 실제로 울리는지는 확인하지 않았다.
- Alloy를 60초 0개로 내렸다 올리자 그동안 gateway 세 개가 쓴 18줄이 모두 들어왔다. Pod 수명 전체로도 gateway와 edge 컨테이너 여섯 개의 Loki 줄 수가 `kubectl logs`와 같아 빠짐도 중복도 없었다. 새 Alloy Pod의 저장 공간은 비어 있었는데 어떤 방식으로 이어 읽었는지는 문서에 없고 확인하지 않았다.
- Loki를 60초 내리자 `loki_write_batch_retries_total`이 0에서 6이 됐고 `loki_write_dropped_entries_total`은 0이었다. 그동안 gateway 세 개는 Ready였고 `wts_published_total`은 171,811에서 189,810으로 늘었다. Loki가 돌아온 뒤 gateway 로그 줄 수는 `kubectl logs`와 같았다.
- 3시간 로그에서 `password`, `secret`, Bearer 토큰, JWT, 개인 키 패턴에 걸린 줄은 broker 시작 설정 출력 90줄뿐이었고 값은 모두 `null`이었다.
- 실행 중인 Loki는 보존 3일, Prometheus는 보존 1일과 1GiB로 떴고 PVC 두 개는 2Gi로 묶였다. kind 노드 일곱 개의 메모리 합계는 약 3.7GiB였다.

확인한 장애는 60초짜리뿐이다. 그보다 긴 중단에서는 kubelet의 로그 회전(기본 컨테이너당 10MiB 파일 5개), Loki의 168시간 거절 기준, Alloy의 재시도 한도(기본값으로 계산하면 약 8.5분 뒤 batch를 버림, 측정하지 않음)가 유실 경계가 된다. 수집기가 멈춘 사이 지워진 Pod의 로그는 API로 읽을 수 없으므로 그 Pod의 읽지 못한 줄은 잃는다(Pod 삭제 뒤 API가 돌려준 오류 문장에서 본 동작이며, 중단 중 삭제는 시험하지 않았다).
