# 부하 실험

gateway에 연결을 많이 붙이고 체결을 흘려서 메모리, CPU, 전달 지연을 잰다. `loadgen`이 체결을 링에 쓰는 일과 클라이언트 N개가 받는 일을 한 프로세스에서 같이 하므로, 같은 시계로 "쓴 시각"과 "받은 시각"을 잴 수 있다. 실제 `feed`와 동시에 띄우지 않는다(링의 쓰는 쪽은 하나다).

## 사용법

```sh
bench/run.sh <이름> [--poll-us N] [loadgen 인자...]
bench/run.sh conn-1000 --connections 1000 --probes 50 --seconds 20
bench/run.sh poll-1000us --poll-us 1000 --connections 200
```

스크립트는 `ulimit -n`을 올리고, 릴리스 빌드한 `gateway`를 임시 링과 빈 포트로 띄우고, `loadgen`을 돌리면서 1초마다 gateway의 RSS와 CPU를 `ps`로 읽고, 끝나면 결과를 `bench/results/<이름>.json`에 쓴다. gateway와 임시 파일은 어떤 경우에도 남기지 않는다. `jq`가 필요하다. `--url`, `--ring`, `--out`은 스크립트가 정하므로 넘기지 않는다.

Linux에서 재려면 `bench/linux.sh`로 같은 명령을 컨테이너 안에서 돌린다(Docker 필요). gateway는 코어 0-3, loadgen은 4-9에 `taskset`으로 고정하고, 바꾸려면 `GATEWAY_CPUS`, `LOADGEN_CPUS`를 준다. 고정한 코어는 결과의 `environment`에 남는다.

```sh
bench/linux.sh bench/run.sh linux-conn-1000 --connections 1000
GATEWAY_CPUS=0-1 LOADGEN_CPUS=2-9 bench/linux.sh bench/run.sh linux-conn-1000-2core --connections 1000
```

`loadgen`을 직접 쓸 때는 gateway를 먼저 띄우고 같은 `--ring`을 준다. 워밍업은 1초 이상으로 둔다. gateway의 링 브리지는 링 파일이 생긴 뒤 최대 0.5초 늦게 열고 열린 시점의 끝에서부터 읽으므로, 워밍업이 짧으면 처음 체결이 전달되지 않아 `delivery.ratio`가 1보다 작아진다.

| 인자 | 기본값 | 뜻 |
| --- | --- | --- |
| `--url` | `ws://127.0.0.1:8080/ws` | gateway 주소 |
| `--ring` | feed와 같은 기본 경로 | 쓸 링 파일 |
| `--connections` | 100 | 클라이언트 수 |
| `--symbols` | feed와 같은 3종목 | 쓸 종목. 모든 클라이언트가 전부 구독한다. 종목 코드는 서로 달라야 한다(받은 체결을 코드로 쓴 기록과 짝짓는다) |
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
| `environment` | OS, CPU 모델, 코어 수, `rustc --version`, 커밋 해시(커밋이 없으면 null), `gateway_cpus`와 `loadgen_cpus`(코어를 고정하지 않았으면 null) |

CPU는 코어 하나를 100으로 센다. 측정 구간이 짧으면 표본이 몇 개뿐이므로 `gateway.samples`를 함께 본다.

## macOS 루프백 결과를 읽을 때

연결 5,000개 이상의 결과에는 gateway가 아니라 측정 환경의 한계가 섞여 있다. 클라이언트와 서버가 같은 기계에 있으면 연결 하나가 소켓 두 개를 쓰고, 그 버퍼가 모두 한 커널의 네트워크 메모리에서 나온다. 2026-11-19에 연결 10,000개 조건(Apple M5, macOS)에서 확인한 것은 다음과 같다.

- gateway 워커 스레드 10개의 호출 스택을 `sample`로 5초 동안 뜨면 스레드마다 표본의 78~100%가 `sendto` 안이다. 같은 조건에서 gateway가 쓴 CPU는 코어 1개 안팎이고, loadgen 워커는 표본의 약 75%가 대기 상태였다.
- 한 실행에서 `netstat -m`의 `requests for memory denied`가 2,090,227 늘었다. 연결 1,000개와 5,000개 실행에서는 늘지 않았다. 연결 5,000개 실행 중간에는 네트워크 메모리의 41.3%가 쓰이고 있었다.
- 그 실행은 20초짜리 측정이 15분 넘게 걸렸다.

그래서 `conn-10000*.json`의 지연은 서버 설계의 수치로 쓰지 않는다. 같은 조건을 Linux에서 재면(`linux-conn-10000.json`) p50 14.7ms, p99 41.5ms이고 세 번 돌려 p50이 14.7~15.0ms였다. 그때 gateway 워커 4개는 CPU를 24%씩 고르게 썼고 `/proc/net/sockstat`의 TCP 메모리는 226쪽이었다.

결과를 비교하기 전에 `connect.failed`가 0인지, `written.per_sec`가 조건(종목 수 x `--rate`)과 맞는지 먼저 본다. 둘 중 하나라도 어긋난 실행은 버린다.

## Linux 결과를 읽을 때

`linux-*.json`은 같은 기계(Apple M5)의 OrbStack 가상 머신(Linux 7.0.14, aarch64, 10코어)에서 쟀다. 클라이언트와 서버가 한 커널에 있다는 점은 macOS 측정과 같다.

이 가상 머신에서는 짧은 sleep이 요청한 만큼 자지 않는다. 50us, 200us, 1000us를 요청하면 모두 약 3ms(최대 4.5ms)를 잔다(같은 측정이 macOS에서는 79us, 304us, 1512us). 그래서 `--poll-us`가 0이 아닌 결과에는 링을 읽기까지의 대기가 0~4.5ms 섞이고, `linux-poll-50`, `linux-poll-200`, `linux-poll-1000`은 서로 구별되지 않는다. 링 대기를 뺀 전달 지연은 `--poll-us 0`으로 잰 `linux-conn-*-poll0.json`에 있다. 이때 gateway의 코어 하나는 링을 읽는 스레드가 계속 쓴다.
