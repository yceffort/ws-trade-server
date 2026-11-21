# 실제 WebSocket 클라이언트 테스트

합성 feed, Rust gateway, TLS 프록시와 Node WebSocket 클라이언트를 실제 소켓으로 연결한다. 기존 soak의 서버 실행 및 클라이언트 코드를 재사용하며 짧은 기능 검증으로 실행한다. Node 22 이상, OpenSSL, Rust가 필요하다. 저장소 루트에서 다음 명령을 실행한다.

```sh
cargo build --locked --bin gateway --bin feed
npm ci --prefix edge --ignore-scripts
node tests/websocket-e2e.test.mjs
```

테스트가 임시 링, 토큰과 인증서를 만들고 비어 있는 포트로 서버를 시작한다. 평문 WS와 인증서를 신뢰한 WSS 양쪽에서 CONNECT, 구독 성공 RECEIPT, 국내 및 미국 MESSAGE 수신, 구독 해지, 연결 종료와 재접속을 검증한다. MESSAGE는 해당 구독의 성공 RECEIPT 이후여야 하고 해지 성공 RECEIPT 뒤에는 해당 구독으로 오면 안 된다. 본문의 키 순서와 JSON 값의 타입, content-length와 연결 내 message-id 순번도 확인한다. 잘못된 토큰과 신뢰하지 않은 인증서가 거절되는지 검사한다. 성공과 실패 모두 테스트가 만든 프로세스, 소켓과 임시 파일을 정리한다.

이 파일은 node:test를 사용하지만 `node`로 직접 실행한다. 신호 처리와 자원 정리를 같은 프로세스에서 실행해 SIGINT와 SIGTERM을 받으면 검사를 취소하고 정리를 기다린다. `node --test`의 기본 프로세스 격리에서는 바깥 runner에 신호를 보내면 테스트 프로세스가 먼저 종료되어 정리할 기회를 잃을 수 있다. SIGKILL이나 시스템 강제 종료 뒤의 정리는 보장하지 않는다.

이미 실행한 실험 서버에는 아래 클라이언트만 연결할 수 있다. 원격 주소는 WSS만 허용하며 평문 WS는 localhost, 127.0.0.1과 ::1에서만 허용한다. 토큰 파일은 토큰 문자열 한 줄이며 gateway의 토큰 배열 JSON과 다르다. 두 공개 합성 topic인 `/topic/v1/kr/stock/trade/A005930`과 `/topic/v1/us/stock/trade/US19990122001`에 지속적으로 데이터가 들어와야 한다. 낮은 발행률에서는 단계별 5초 제한을 넘길 수 있다. `--ca-file`은 테스트 CA를 쓸 때 지정하고 공개 CA 인증서에서는 생략한다.

```sh
node examples/websocket-check.mjs --url wss://localhost:8443/ws --token-file /path/to/token.txt --ca-file /path/to/cert.pem
```

이 명령은 지정한 서버를 시작하거나 종료하지 않고 자기 연결의 구독과 해지만 수행한다. 전체 실행 제한은 15초이며 성공하면 단계별 경과 시간과 두 연결에서 받은 topic별 건수를 JSON으로 출력한다. 실패하면 원문 토큰, URL과 본문 대신 실패 단계를 출력하고 종료 코드 1을 반환한다. 경과 시간은 테스트 단계의 실행 시간이며 서비스 p99 지연이 아니다.

재접속 뒤 새 구독으로 수신이 재개되는지를 검사한다. STOMP의 message-id가 연속이어도 접속이 끊겼던 동안의 체결을 모두 받았다는 뜻은 아니다. HA cursor와 committed event 복구는 `examples/ha-client.mjs`, `bench/ha/`와 해당 Rust 통합 테스트에서 검증한다. 이 코드는 짧은 기능 검증이며 부하 용량이나 실제 장애 도메인의 가용성을 입증하지 않는다.
