# 컨테이너로 실행하기

Rust feed와 gateway, Node TLS 프록시를 실행하는 합성 체결 데모다. 실제 증권사의 배포 구조를 관측한 결과가 아니다. Docker Engine과 Compose v2 이상, 호스트의 Node 22 이상, OpenSSL이 필요하다. 이미지 안에는 Rust 컴파일러나 소스, 캡처 원본을 넣지 않는다.

```sh
node deploy/prepare.mjs
docker compose up --build --detach --wait
npm ci --prefix edge
node deploy/check.mjs
docker compose logs --follow gateway
docker compose down --volumes
```

준비 스크립트는 새 `deploy/local/`에 1일짜리 localhost 인증서와 무작위 데모 토큰을 만든다. 기존 디렉터리는 덮어쓰지 않는다. 부모 디렉터리는 0700, 파일은 Compose의 bind mount에서 UID 10001이 읽을 수 있도록 0444다. 실제 운영 키를 만드는 절차가 아니며 토큰과 인증서는 만료 전에 별도로 교체해야 한다. 데모를 다시 준비하려면 실행을 내린 뒤 기존 디렉터리를 안전한 곳으로 옮기고 다시 실행한다. 검증기는 생성한 인증서를 CA로 지정하며 TLS 검증을 끄지 않는다.

외부 파일은 `WTS_SECRETS_DIR=/절대/경로 docker compose up --detach --wait`로 지정한다. 디렉터리에는 `tokens.json`, `cert.pem`, `key.pem`이 필요하다. 토큰 형식은 루트 README의 인증 절을 따른다. Compose는 파일 자체를 마운트하므로 원자적 파일 교체 후에는 `docker compose up --detach --force-recreate`로 반영한다. 인증서는 프로세스 시작 때 읽는다. 이미지나 환경 변수에 토큰, 개인 키를 넣지 않는다.

외부에 공개하는 포트는 호스트 loopback의 WSS 8443 하나다. `WTS_PORT`로 호스트 포트를 바꾼다. gateway의 평문 WS는 Compose 네트워크 안에서만 열고, 관리 HTTP는 gateway 컨테이너의 loopback에만 바인딩한다. 다른 호스트에 공개하려면 TLS 인증서와 방화벽, Compose의 공개 주소를 배포 환경에 맞게 바꿔야 한다. TLS는 edge에서 끝나므로 edge와 gateway 사이의 네트워크도 신뢰 경계에 포함된다.

feed는 쓰기 가능한 named volume을, gateway는 같은 volume을 읽기 전용으로 사용한다. 세 서비스는 UID 10001, 읽기 전용 루트 파일 시스템, capability 제거로 실행한다. Rust 이미지의 tini가 PID 1로 SIGTERM을 자식에게 전달한다. gateway가 먼저 종료되어 `server-shutdown` 프레임을 전할 때까지 edge가 남도록 Compose 의존 순서를 정했다. [Compose의 시작 및 종료 순서](https://docs.docker.com/compose/how-tos/startup-order/)를 따른다. 프로세스 로그는 표준 출력과 표준 오류로 보내고 gateway 이벤트는 JSON으로 출력한다.

gateway의 readiness는 허용한 두 topic 모두에 최근 10초 이내 체결 snapshot이 있는지 검사한다. 이것은 항상 데이터를 만드는 합성 feed용 정책이며 휴장이나 거래가 드문 실종목의 운영 정책이 아니다. liveness는 관리 HTTP 응답, feed 검사는 링 파일 존재, edge 검사는 TCP listener만 확인한다. TLS, 토큰, 압축, 프레임 전달은 아래 종단 검증에서 별도로 검사한다. Compose는 unhealthy 상태만으로 컨테이너를 재시작하지 않는다.

```sh
docker compose build
npm ci --prefix edge
node deploy/context-smoke.mjs
bash deploy/compose-smoke.sh
```

검증은 별도 Compose 프로젝트와 임시 인증 자료, 임의의 loopback 포트를 사용한다. 양쪽 topic 수신, 잘못된 토큰 거절, non-root와 읽기 전용 루트, feed 중지 시 readiness 실패와 복구, 전체 종료 시 종료 프레임을 확인하고 컨테이너와 volume을 지운다. Bash도 필요하다. GitHub Actions의 `Deployment` 워크플로에서 같은 절차를 실행한다.

context 검증은 합성 파일만 담은 임시 디렉터리에 실제 `.dockerignore`를 적용하고 Docker의 COPY 결과를 검사한다. 인증 파일, 캡처, node_modules가 build context에서 제외되는지 확인하며 저장소의 실제 인증 파일이나 캡처를 읽지 않는다.

이 구성은 단일 feed 실험이다. volume을 지우면 링 데이터가 사라지고 gateway를 재시작하면 snapshot과 replay가 초기화된다. 이미지 기본 태그는 `ws-trade-server:local`, `ws-trade-edge:local`이며 `WTS_SERVER_IMAGE`, `WTS_EDGE_IMAGE`로 바꿀 수 있다. 레지스트리 업로드, 인증서 자동 갱신, 부하 분산과 무중단 배포를 제공하지 않는다.
