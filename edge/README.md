# 전송 계층 실험

공개 Open API가 요구하는 TLS와 [ws의 프록시 API](https://github.com/websockets/ws/blob/master/doc/ws.md)를 참고한 선택적 실험이다. Rust gateway 앞에서 TLS를 종료하며 프레임 본문은 바꾸지 않는다. 먼저 gateway를 `127.0.0.1:8080`에 띄운다. Node 22 이상과 OpenSSL이 필요하다.

```sh
npm ci --prefix edge
mkdir -p edge/certs
openssl req -x509 -newkey rsa:2048 -noenc -days 1 -subj /CN=localhost -addext subjectAltName=DNS:localhost,IP:127.0.0.1 -keyout edge/certs/key.pem -out edge/certs/cert.pem
node edge/run.js ws://127.0.0.1:8080/ws 8443 edge/certs/cert.pem edge/certs/key.pem
```

인증서와 키는 gitignore에 포함된다. 테스트도 임시 디렉터리에 인증서를 만들고 제거한다. 클라이언트는 CA를 명시해 검증하며 검증 해제 옵션을 사용하지 않는다.

```sh
npm test --prefix edge
node edge/measure.js ws://127.0.0.1:8080/ws
node edge/measure.js wss://127.0.0.1:8443/ws edge/certs/cert.pem
```

측정기는 WebSocket 및 CONNECT 완료 시간과 100회의 구독 RECEIPT 왕복 지연 p50, p99를 출력한다. 제한이나 인증을 켜지 않은 gateway에서 실행한다. WS 직결과 WSS 프록시의 차이에는 프록시 한 번을 더 거치는 비용도 포함된다. 같은 경로에서 TLS만 비교하려면 `node edge/run.js ws://127.0.0.1:8080/ws 8082`로 평문 프록시를 하나 더 띄우고 `ws://127.0.0.1:8082/ws`를 비교한다. 실행 순서를 바꾸어 반복하고 한 번의 결과를 성능 결론으로 쓰지 않는다.

프록시는 기본적으로 loopback에서 수신하고 동시 TCP 연결을 기본 1024개로 제한한다. 메시지는 64 KiB, 송신 대기는 1 MiB, 백엔드 연결 대기는 5초까지 허용한다. 키 배포와 인증서 자동 갱신은 다루지 않는다.

압축은 기본 비활성이다. WSS 프록시 인자 마지막에 `deflate`를 붙이면 클라이언트가 제안할 때만 permessage-deflate를 협상한다.

```sh
node edge/run.js ws://127.0.0.1:8080/ws 8443 edge/certs/cert.pem edge/certs/key.pem deflate
node edge/compare.js 1000
```

비교기는 매번 별도 Node 프로세스에서 평문 WS 프록시, echo 백엔드와 클라이언트를 띄우고 동일한 합성 MESSAGE 1000개를 왕복한다. 데이터 SHA-256과 해제된 본문 일치를 확인한다. 소켓의 read/write 바이트는 WebSocket 헤더와 압축된 payload를 포함하고 TCP/IP 헤더는 제외한다. CPU와 최대 RSS는 프록시만의 수치가 아니라 세 역할을 모두 포함한 프로세스 전체 수치다. 이 벤치는 TLS를 제외해 압축 설정만 비교한다. 실제 팬아웃 부하의 성능 결론으로 일반화하지 않는다.

컨테이너에서는 `WTS_BACKEND`, `WTS_EDGE_HOST`, `WTS_EDGE_PORT`, `WTS_TLS_CERT_FILE`, `WTS_TLS_KEY_FILE`, `WTS_COMPRESSION=deflate`로 설정한다. 위치 인자가 있으면 해당 환경 변수보다 우선한다. `WTS_EDGE_HOST=0.0.0.0`은 외부 인터페이스에도 listener를 연다. 서비스용 이미지와 실행 절차는 [배포 안내](../deploy/README.md)에 있다.

`WTS_EDGE_WORKERS`(기본 1, 최대 16)를 2 이상으로 두면 Node cluster worker 여러 개가 같은 포트를 나눠 받는다. 프로세스 하나가 TLS handshake와 중계를 모두 처리해 코어 하나에서 막히는 것을 피하려는 설정이다([#95](https://github.com/yceffort/ws-trade-server/issues/95)). 연결 상한과 버퍼는 worker마다 따로 있고, 로그에는 `worker` 필드가 붙는다. 종료 신호는 primary가 모든 worker에 전달한다. 한 worker가 죽으면 그 자리만 다시 띄우고, 듣기도 전에 죽으면 전체를 종료 코드 1로 끝낸다. `node edge/capacity.js 200,400,600`은 합성 upstream으로 edge 하나가 받을 수 있는 연결 수를 잰다. 결과와 해석은 `docs/ha-capacity.md`에 있다.
