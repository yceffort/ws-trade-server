# gateway 로그

현재의 `eprintln!`은 서버가 왜 연결을 닫았는지 구분하지 못하고 클라이언트의 connection-id를 그대로 출력했다. 링 유실과 디코딩 실패가 반복되면 stderr 쓰기도 체결 경로에 쌓였다. [tracing](https://docs.rs/tracing/latest/tracing/) 이벤트와 [별도 출력 스레드](https://docs.rs/tracing-appender/latest/tracing_appender/non_blocking/)로 바꾸고, 원인은 구조화 필드와 누적 메트릭의 증분으로 남긴다.

```sh
cargo build --bin gateway
WTS_LOG_FORMAT=json RUST_LOG=ws_trade_server=info ./target/debug/gateway --http-addr 127.0.0.1:8081 2>/tmp/ws-gateway.jsonl
WTS_LOG_FORMAT=text RUST_LOG=ws_trade_server::connection=debug,ws_trade_server=info ./target/debug/gateway
```

빌드가 성공한 뒤 바이너리 실행에만 stderr 리디렉션을 적용한다. Cargo의 빌드 메시지와 진단은 JSON 로그 파일에 들어가지 않는다.

기본 형식은 text, 기본 필터는 `ws_trade_server=info`다. JSON은 한 줄에 이벤트 하나이며 timestamp, level, target, fields가 있다. 이벤트 이름은 `fields.event`에 들어간다. 라이브러리는 subscriber를 설치하지 않고 gateway가 한 번 초기화한다. feed와 loadgen의 기존 CLI 출력은 유지한다. 특히 loadgen의 stdout JSON은 바꾸지 않는다.

| 수준 | 이벤트 | 용도 |
| --- | --- | --- |
| INFO | gateway_started, shutdown_requested, shutdown_complete | 시작 주소와 정상 종료 확인 |
| INFO | ring_waiting, ring_opened, feed_restarted | 입력 데이터 수명 확인 |
| INFO | server_summary | 10초마다 연결, 구독, 큐, 발행 및 적재, 로그 폐기 수 확인 |
| WARN | server_anomalies | 직전 요약 이후 링 덮어쓰기, seq 누락, 디코딩 및 본문 실패, 느린 연결 차단, 로그 폐기 수 |
| WARN | auth_reload_failed, http_accept_failed, websocket_accept_failed | 인증 파일 및 수락 장애 |
| DEBUG | connection_opened, connection_closed | 서버 내부 conn 번호와 종료 reason으로 연결 추적 |
| DEBUG | handshake_failed, authentication_rejected, subscription_rejected, slow_consumer_dropped | 연결 단위 상세 원인 |
| ERROR | invalid_arguments, auth_config_rejected, gateway_start_failed | 시작 실패 |

`conn`은 해당 gateway 프로세스 안에서 매긴 번호다. 클라이언트 connection-id, authorization, 원본 프레임, 체결 본문은 로그에 넣지 않는다. gateway의 필터는 자체 target만 허용하므로 `RUST_LOG=trace`로 올려도 외부 WebSocket 라이브러리가 원본 프레임을 출력하지 않는다.

출력 큐는 1024줄이다. 출력이 밀리면 로그를 버리고 체결 처리는 계속한다. `logs_dropped_total`은 요약과 로컬 `/metrics`에서 읽으며 최대 10초 간격으로 갱신된다. 이 값은 수집기나 파일 시스템의 쓰기 실패 수가 아니라 큐에 넣지 못한 로그 수다. 모든 로그의 보존을 보장하지 않는다. 경고도 포화 시 버려질 수 있으므로 이 카운터와 외부 수집기의 상태를 함께 본다.

요약 수집은 정상 종료와 병행하고 1초가 지나면 취소한다. SIGINT/SIGTERM에서는 gateway 종료 후 출력 guard를 정리한다. 강제 종료와 출력 장치 장애에서는 마지막 로그가 유실될 수 있다. 출력의 저장, 회전, 검색은 stderr를 받는 실행 환경에서 담당한다.

체결마다 로그를 남기지 않는다. 링 유실과 디코딩 실패는 기존 메트릭을 증가시키고 요약 태스크가 증분을 출력한다. 인증 파일 오류는 실패 상태로 바뀔 때 한 번, 복구 시 한 번만 남긴다. 새 이벤트를 추가할 때도 원본 메시지나 사용자 입력 대신 고정된 사유 코드와 서버가 만든 식별자를 사용한다.
