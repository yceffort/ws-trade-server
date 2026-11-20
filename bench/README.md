# 부하 실험

gateway에 연결을 많이 붙이고 체결을 흘려서 메모리, CPU, 전달 지연을 잰다. `loadgen`이 체결을 링에 쓰는 일과 클라이언트 N개가 받는 일을 한 프로세스에서 같이 하므로, 같은 시계로 "쓴 시각"과 "받은 시각"을 잴 수 있다. 실제 `feed`와 동시에 띄우지 않는다(링의 쓰는 쪽은 하나다).

## 사용법

```sh
bench/run.sh <이름> [--poll-us N] [loadgen 인자...]
bench/run.sh conn-1000 --connections 1000 --probes 50 --seconds 20
bench/run.sh poll-1000us --poll-us 1000 --connections 200
```

스크립트는 `ulimit -n`을 올리고, 릴리스 빌드한 `gateway`를 임시 링과 빈 포트로 띄우고, `loadgen`을 돌리면서 1초마다 gateway의 RSS와 CPU를 `ps`로 읽고, 끝나면 결과를 `bench/results/<이름>.json`에 쓴다. gateway와 임시 파일은 어떤 경우에도 남기지 않는다. `jq`가 필요하다. `--url`, `--ring`, `--out`은 스크립트가 정하므로 넘기지 않는다.

`loadgen`을 직접 쓸 때는 gateway를 먼저 띄우고 같은 `--ring`을 준다. 워밍업은 1초 이상으로 둔다. gateway의 링 브리지는 링 파일이 생긴 뒤 최대 0.5초 늦게 열고 열린 시점의 끝에서부터 읽으므로, 워밍업이 짧으면 처음 체결이 전달되지 않아 `delivery.ratio`가 1보다 작아진다.

| 인자 | 기본값 | 뜻 |
| --- | --- | --- |
| `--url` | `ws://127.0.0.1:8080/ws` | gateway 주소 |
| `--ring` | feed와 같은 기본 경로 | 쓸 링 파일 |
| `--connections` | 100 | 클라이언트 수 |
| `--symbols` | feed와 같은 3종목 | 쓸 종목. 모든 클라이언트가 전부 구독한다 |
| `--rate` | 5 | 종목당 초당 평균 체결 수 |
| `--stalled` | 0 | 구독만 하고 읽지 않는 클라이언트 수(`--connections`에 포함) |
| `--probes` | 50 | 지연을 재는 클라이언트 수(`--connections`에 포함) |
| `--warmup` | 3 | 연결을 다 붙인 뒤 측정 전에 흘리는 초 |
| `--seconds` | 20 | 측정 시간(초) |
| `--out` | 표준 출력 | 결과 JSON 파일 |

연결은 200개씩 묶어 차례로 연다. 일반 클라이언트는 `message-id`의 순번이 1부터 빠짐없이 이어지는지 보고, 탐침은 거기에 더해 본문을 파싱해 지연을 잰다. 멈춘 클라이언트는 측정이 끝날 때 소켓을 읽어 보고, 쌓인 데이터를 비운 뒤 EOF나 오류가 나면 서버가 끊은 것으로 센다.

## 지연을 재는 법

체결을 링에 쓰기 직전에 `(종목, cumulative_volume)`별로 `Instant`를 기록하고, 탐침은 프레임을 읽은 직후에 `Instant`를 찍은 뒤에야 본문을 파싱한다. 둘을 짝짓는 일은 실행이 끝난 뒤에 한다(실행 중에는 잠금도 조회도 없다). 쓴 시각이 측정 구간 안인 표본만 지연으로 세고, 구간 밖이거나 짝이 없는 표본은 버리지 않고 건수로 남긴다. 백분위는 nearest-rank 방식이다. 오름차순 n개에서 p백분위는 `ceil(p/100 * n)`번째 값이라 표본이 적으면 p99가 max와 같아진다.

받은 시각은 클라이언트 태스크가 깨어나 프레임을 읽는 시각이다. 연결 수가 코어 수보다 훨씬 많으면 loadgen 자신의 스케줄링 지연이 지연에 섞이고, loadgen과 gateway가 같은 기계의 코어를 나눠 쓴다.

## 결과 필드

`delivery`가 없는 결과 파일은 이 필드를 넣기 전에 만든 것이다. 거기 있는 `messages.expected`, `messages.ratio`는 받은 시각으로 센 수를 나눈 값이라 빠진 체결이 없어도 1을 넘을 수 있다.

| 필드 | 뜻 |
| --- | --- |
| `config` | 실행 조건. loadgen 인자 전부(종목은 topic으로 적는다) |
| `connect.ready`, `connect.failed`, `connect.millis` | 준비된 연결 수, 실패 수, 전부 붙이는 데 걸린 밀리초 |
| `clients.normal`, `clients.probes`, `clients.stalled` | 준비된 클라이언트 수(역할별) |
| `written.total` | 링에 쓴 체결 수(워밍업 포함) |
| `written.measured`, `written.per_sec` | 측정 구간에 쓴 체결 수와 초당 수 |
| `messages.total` | 받은 `MESSAGE` 총수(워밍업 포함, 멈춘 클라이언트 제외) |
| `messages.measured`, `messages.per_sec` | 측정 구간에 받은 수와 초당 수. 받은 시각으로 세므로 쓴 수와 견주지 않는다 |
| `delivery.expected` | 기대치. `written.measured` x 탐침 수 |
| `delivery.delivered` | 측정 구간에 쓴 체결을 탐침이 받은 수. 쓴 기록과 짝지어 세므로 워밍업 때 쓴 체결과 측정이 끝난 뒤에 쓴 체결은 넣지 않는다 |
| `delivery.ratio` | `delivered` / `expected`. 빠진 체결이 없으면 1이다. 마지막 1초 안에 도착하지 못한 체결은 빠진 것으로 센다. 탐침이 없으면 null |
| `seq_gap_clients` | `message-id` 순번이 1부터 빠짐없이 이어지지 않은 클라이언트 수 |
| `unexpected_disconnects` | 준비된 뒤 서버가 끊은 일반 클라이언트 수(탐침 포함) |
| `stalled_kicked` | 서버가 끊은 멈춘 클라이언트 수 |
| `latency_us.samples` | 지연 표본 수 |
| `latency_us.p50`, `p90`, `p99`, `max` | 지연(마이크로초). 표본이 없으면 null |
| `latency_us.unmatched` | 쓴 기록과 짝이 없는 표본 수. 0이 아니면 이 도구나 링을 의심한다 |
| `latency_us.unparsed` | 본문을 읽지 못한 표본 수 |
| `latency_us.outside_window` | 쓴 시각이 측정 구간 밖이라 뺀 표본 수 |
| `gateway.poll_us` | gateway의 `--poll-us` |
| `gateway.rss_kb_idle` | 연결을 붙이기 전 gateway의 RSS(KB) |
| `gateway.rss_kb_max` | 측정 구간의 RSS 최댓값(KB) |
| `gateway.cpu_pct_mean`, `cpu_pct_max` | 측정 구간 `ps %cpu`의 평균과 최댓값. `ps`의 값은 구간 값이 아니라(macOS는 감쇠 평균, Linux는 프로세스 수명 평균) 앞 구간의 부하가 섞인다 |
| `gateway.cpu_pct_from_time` | 측정 구간 첫 표본과 끝 표본 사이 누적 CPU 시간의 증가분을 경과 시간으로 나눈 값 |
| `gateway.samples` | 측정 구간 안의 `ps` 표본 수 |
| `environment` | OS, CPU 모델, 코어 수, `rustc --version`, 커밋 해시(커밋이 없으면 null) |

CPU는 코어 하나를 100으로 센다. 측정 구간이 짧으면 표본이 몇 개뿐이므로 `gateway.samples`를 함께 본다.
