# 전송 계층 실험

공개 Open API가 요구하는 TLS와 [ws의 프록시 API](https://github.com/websockets/ws/blob/master/doc/ws.md)를 참고한 선택적 실험이다. Rust gateway 앞에서 TLS를 종료하며 프레임 본문은 바꾸지 않는다. 먼저 gateway를 `127.0.0.1:8080`에 띄운다. Node 22 이상과 OpenSSL이 필요하다.

```sh
npm ci --prefix edge
mkdir -p edge/certs
openssl req -x509 -newkey rsa:2048 -noenc -days 1 -subj /CN=localhost -addext subjectAltName=DNS:localhost,IP:127.0.0.1 -keyout edge/certs/key.pem -out edge/certs/cert.pem
node edge/run.mjs ws://127.0.0.1:8080/ws 8443 edge/certs/cert.pem edge/certs/key.pem
```

인증서와 키는 gitignore에 포함된다. 테스트도 임시 디렉터리에 인증서를 만들고 제거한다. 클라이언트는 CA를 명시해 검증하며 검증 해제 옵션을 사용하지 않는다.

```sh
npm test --prefix edge
node edge/measure.mjs ws://127.0.0.1:8080/ws
node edge/measure.mjs wss://127.0.0.1:8443/ws edge/certs/cert.pem
```

측정기는 WebSocket 및 CONNECT 완료 시간과 100회의 구독 RECEIPT 왕복 지연 p50, p99를 출력한다. 제한이나 인증을 켜지 않은 gateway에서 실행한다. WS 직결과 WSS 프록시의 차이에는 프록시 한 번을 더 거치는 비용도 포함된다. 같은 경로에서 TLS만 비교하려면 `node edge/run.mjs ws://127.0.0.1:8080/ws 8082`로 평문 프록시를 하나 더 띄우고 `ws://127.0.0.1:8082/ws`를 비교한다. 실행 순서를 바꾸어 반복하고 한 번의 결과를 성능 결론으로 쓰지 않는다.

프록시는 loopback에서만 수신하고 동시 TCP 연결을 기본 1024개로 제한한다. 메시지는 64 KiB, 송신 대기는 1 MiB, 백엔드 연결 대기는 5초까지 허용한다. 키 배포와 인증서 자동 갱신은 다루지 않는다.
