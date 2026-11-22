# WSS 합성 probe

설정한 고정 대상에 실제 WebSocket 클라이언트로 연결해 TLS와 서브프로토콜, 인증, 구독 또는 resume, 수신 본문의 상태 적용과 재접속을 반복 확인한다. 기존 STOMP Client와 LatestState, HA runClient/Application 및 committed oracle 비교를 재사용한다. 이 경로와 시간 한도는 WTS 실측이 아닌 이 저장소의 합성 운영 실험이다.

STOMP는 `/ws`에서 두 합성 공개 topic의 RECEIPT 뒤 MESSAGE를 받아 메모리 상태에 적용하고 연결을 닫은 뒤 다시 확인한다. message-id 연속성 검사는 프레임 순서만 확인한다. 체결 무유실, 단절 중 replay와 committed event 보장을 주장하지 않는다. HA는 `/experimental/ha/ws`에서 `ha-oracle head`의 시작 위치를 고정하고 설정된 권한 범위 topic만 events 모드로 적용한다. checkpoint 이후 일부러 연결을 닫고 cursor로 다시 붙어 추가 이벤트와 caught-up을 확인한다. 마지막 적용 위치까지 `ha-oracle range`를 읽어 기존 oracle 비교기로 event id와 본문 digest, 적용 건수를 대조한다. 토큰 권한 밖 이벤트는 기대값에 포함하지 않는다. 하나라도 실제 이벤트를 적용하지 못하면 성공하지 않는다.

지표에는 설정의 target(최대 8개), protocol(stomp/ha), 고정 stage(connect, tls, auth, subscribe, first_event, apply, reconnect, catch_up, oracle)만 사용한다. Kubernetes 수집기는 namespace와 Pod를 추가한다. 결과와 로그에는 URL, 토큰, cursor, event id, 체결 본문과 command 오류 출력을 남기지 않는다. CA 파일은 추가 신뢰 루트로 쓰며 인증서와 호스트 이름 검증을 끄지 않는다. `NODE_TLS_REJECT_UNAUTHORIZED=0`은 시작 시 거절한다.

## 로컬 합성 검증

```sh
npm ci --prefix edge --ignore-scripts
cargo build --bin gateway --bin feed
node --test probe/*.test.mjs
```

종단 시험이 임시 인증서와 토큰을 만들고 실제 feed, gateway, TLS edge를 시작해 정상 적용, 신뢰하지 않는 인증서, 인증 거절과 데이터 정지를 구분한다. HA 시험은 로컬 합성 WebSocket 서버로 checkpoint 재접속, 본문 대조 실패와 데이터 정지를 검증한다. 실제 Kafka transaction과 LB 장애 도메인은 [HA 수용 시험](../bench/ha/README.md)의 별도 범위다. fixture에 실제 계정이나 캡처 값은 없다.

## 지속 실행과 외부 환경

`config.example.json`을 복사해 사용자가 지정한 합성 계정, WSS 주소, CA와 token 파일 경로로 바꾼다. STOMP 토큰은 Bearer 접두사가 없어도 되고, HA 토큰은 기존 HA 클라이언트처럼 `Bearer ` 접두사를 포함한다. HA의 head/range command는 그 환경의 read-committed broker와 같은 source 설정을 읽는 `ha-oracle` 바이너리여야 한다. 임의 JSON fixture로 외부 환경의 oracle를 대신하지 않는다. 공개 CA 인증서라면 caFile을 생략한다. 샘플 토큰이나 내부 localhost 주소를 외부 서비스에 그대로 쓰지 않는다.

```sh
node probe/server.mjs /path/to/probe.json
curl http://127.0.0.1:9464/metrics
```

관리 포트는 기본 loopback이고 요청으로 target을 바꿀 수 없다. 설정 파일만 대상을 정한다. 실행마다 새 worker를 만들고 기본 힙 64MiB, 최대 시도 3회, 총 기한 최대 60초로 제한한다. HA 수신 대기는 기존 클라이언트의 1MiB 한도, pending event는 1MiB/10,000개, oracle 출력은 4MiB/10,000개로 제한한다. 파일 읽기는 일반 파일과 크기 상한을 검사한다. 다음 실행은 이전 실행 종료 뒤 1초에서 5분 사이의 설정된 intervalMs를 기다린다. worker가 전체 기한을 넘기거나 메모리 한도로 죽으면 부모가 종료하고 `wts_probe_worker_failures_total`을 올린다. 초기에는 성공과 지연 표본을 내지 않는다. 성공한 실행의 단계별 시간만 histogram에 넣으며 표본 없는 p99는 측정되지 않음으로 표시한다.

connect/TLS 시간은 소켓 생성부터 WebSocket open까지라 TLS만의 CPU 시간이 아니다. auth는 연결 시작부터 인증 응답까지이며 first_event/apply/catch_up은 해당 시도의 시작 또는 수신 시점을 기준으로 한다. STOMP apply는 첫 이벤트 수신부터 두 topic의 상태를 확인할 때까지, HA apply/catch_up은 시도 시작부터 마지막 checkpoint 적용/서버 caught-up까지다. 단계 시간은 서로 겹치므로 합산하지 않는다. 자동 reconnect 측정은 probe가 의도적으로 닫은 연결의 합성 표본이다. 계획 배포 재접속 p99와 장애 주입부터 복구까지의 시간은 [기존 부하 측정](../bench/ha/README.md)에서 따로 집계하고 이 histogram과 섞지 않는다. probe 정상은 1,000 clients 용량의 증거가 아니다.

## Kubernetes와 Grafana

```sh
docker build -f probe/Dockerfile --build-arg HA_IMAGE=YOUR_HA_IMAGE --build-arg EDGE_IMAGE=YOUR_EDGE_IMAGE -t YOUR_PROBE_IMAGE .
node probe/render.mjs /path/to/deployment.json > /tmp/wts-probe.json
kubectl --context TEST_CONTEXT apply -f /tmp/wts-probe.json
kubectl --context TEST_CONTEXT -n YOUR_NAMESPACE rollout restart deployment/wts-probe
```

`deployment.json`은 namespace, digest로 고정한 image, credentialsSecret, sourceConfigMap, config를 갖는다. config는 위 실행 설정이다. Secret에는 token, ca.crt와 gateway.json을 두고 source ConfigMap에는 source.json을 둔다. STOMP만 검사하면 HA command는 필요 없다. 렌더러는 token 내용을 읽거나 Secret을 만들지 않는다. 설정 ConfigMap을 바꾼 뒤에는 위 restart를 실행해 새 설정을 적용한다. 네트워크 정책은 같은 namespace의 gateway WSS(8443), broker oracle(9092)와 wts-observability namespace의 metrics 수집만 추가 허용한다. 외부 LB를 검사할 때는 기존 CNI 정책에서 사용자가 지정한 대상 경로가 허용되어야 한다.

[관측 스택](../deploy/k8s/ha/observability/README.md)을 다시 apply하면 `wts-probe` Service의 metrics를 5초마다 수집하고 Grafana WTS 폴더에 클라이언트 적용 probe 화면을 등록한다. probe-rules.yaml의 `wts_probe_expected_namespace` 기본값 wts-ha를 실제 설치 namespace로 바꾼다. namespace가 여럿이면 각 namespace를 별도 record로 선언한다. 기대 namespace의 전체 Service 소실도 WtsProbeMissing으로 알리며 다른 namespace의 성공이 이를 가리지 않는다.

WtsProbeFailed는 적용 실패, WtsProbeWorkerFailed는 실행기 실패, WtsProbeStale은 실행 주기와 기한을 합친 다음 완료 시각 초과, WtsProbeMissing은 수집 실패 또는 endpoint 전체 소실이다. 실패 단계에서 TLS 신뢰, 인증 만료, 구독 권한, 합성 feed 정지, source/broker freshness, oracle 접근을 차례로 확인하고 복구 알림을 확인한다. 외부 통지는 [로컬 알림 경로](../docs/ha-runbook.md#alert-response) 검증 후 사용자가 지정한 대상으로만 설정한다.
