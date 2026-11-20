# HA gateway의 준비와 종료

[#50](https://github.com/yceffort/ws-trade-server/issues/50)의 상태 머신이다. HA 입력을 지정하면 listener와 loopback 관리 HTTP를 먼저 열고 백그라운드에서 snapshot 복원과 catch-up을 수행한다. `start_managed`는 bootstrap 중에도 반환하며 라이브러리 호출자는 `Server::wait_ready`로 준비를 기다릴 수 있다. 기본 mmap 실행의 시작 및 종료 방식은 유지한다.

| 상태 | 새 upgrade | 기존 HA 연결 | 관리 API |
| --- | --- | --- | --- |
| BOOTSTRAPPING | 503, Retry-After 1 | 없음 | livez 200, readyz 503 |
| READY | 허용 | 데이터와 heartbeat | livez/readyz 200 |
| DEGRADED | 503, Retry-After 1 | source-stale 후 종료 | livez 200, readyz 503 |
| DRAINING | 503, Retry-After 1 | 정해진 시각까지 유지 후 draining | livez 200, readyz 503 |
| STOPPED | listener 종료 | 종료 | 관리 listener 종료 |

준비 상태는 복제본의 bootstrap, 최신 log end 대비 lag, source/broker 신선도와 정책 신선도를 기반으로 판단한다. 지금은 로컬 인증 정책을 사용하며 공유 정책은 #51에서 연결한다. DEGRADED가 잠깐 지나가도 이전 연결은 세대 번호 변경을 확인해 종료한다. broker 조회의 일시 오류는 복원 위치를 유지하며 재시도한다. 복제 worker가 broker 오류로 종료되면 기존 live 구독을 실패 처리한 뒤 완전 snapshot에서 재복원한다. schema, source identity와 순번 오류를 정상 입력으로 간주해 건너뛰지 않는다.

`GET /livez`, `GET /readyz`, `POST /drain`은 loopback 관리 listener에서만 제공한다. `/metrics`와 `/snapshots`도 유지한다. `POST /drain`은 최초 호출 시각을 보존하고 즉시 반환한다. 반복 호출, SIGTERM과 회복된 신선도는 종료 예산을 연장하거나 READY로 되돌리지 않는다. accept 경로와 실제 WebSocket upgrade callback이 같은 상태를 확인하므로 drain 중 지연된 handshake도 503으로 거절한다. edge는 허용된 Retry-After를 전달한다.

기본 예산은 routing 전파 대기 20초, HA 연결별 재접속 통지 분산 10초, flush 10초다. 이 동안 기존 stream, 인증 검사와 관리 HTTP를 유지한다. HA 연결은 마지막 영속화된 cursor로 재접속하며 기존 `/ws`에는 `server-shutdown` 오류를 보낸다. 그 뒤 입력 worker, hub, 관리 HTTP를 정리한다. broker 호출은 최대 10초 설정만 허용해 종료가 무한 대기하지 않게 한다. Pod의 전체 grace는 preStop과 sidecar 정리를 포함해 60초로 설정하며 실제 routing 전파 속도는 #52/#53에서 측정한다.

preStop은 `container-probe ha-drain`을 호출하고 20초를 기다리는 형태로 연결한다. SIGTERM만 와도 같은 drain을 시작한다. 관리 POST만으로도 상태 머신은 끝까지 진행하고 gateway 프로세스가 종료된다. 강제 종료에서는 통지 대신 클라이언트의 3초 수신 deadline이 복구를 시작한다.

`container-probe ha-live`, `ha-ready`는 loopback API를 0.8초 제한으로 호출한다. 2xx만 성공이며 접속 오류, timeout, 503은 실패한다. `WTS_HTTP_PORT`는 1부터 65535까지의 loopback 포트만 바꿀 수 있고 기본은 8081이다. Pod IP를 향하는 httpGet 대신 컨테이너 내부 exec probe로 실행한다. 실제 manifests는 #52의 별도 HA overlay에서 연결한다.

기본 및 HA Rust 검증과 lifecycle 시험은 bootstrap 거절, ready 전이, 중복 drain, 늦은 handshake, 기존 stream 유지, 관리 API 및 exec probe, 종료 후 포트 반환을 검사한다. 실제 Kafka 시험은 신선도 상실과 회복, HA source-stale 및 지연된 draining 통지를 검사한다. 이 시험은 외부 LB의 routing 제거 시간을 증명하지 않는다.

복제 오류 후 snapshot을 다시 읽을 때 기존 HA와 STOMP 구독자를 먼저 `source-stale`로 종료한다. 재복원 중 이미 전달한 체결을 기존 STOMP 세션에 재발행하지 않는다. 실패 관측은 이전 ready 관측보다 우선한다.
