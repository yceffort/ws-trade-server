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

## CI에서 돌리기

`.github/workflows/bench.yml`이 서버나 측정 도구를 건드린 PR마다, 그리고 Actions 탭에서 직접 실행할 때 `bench/ci.sh`를 돌린다. 조건은 둘이고 각각 10초씩 세 번 돌려 실행별 값과 중앙값을 실행 요약에 표로 적는다. PR이면 기준 브랜치도 같은 러너에 받아 번갈아 돌려 나란히 적는다.

| 조건 | 연결 | 종목당 초당 건수 | 보는 것 |
| --- | --- | --- | --- |
| `steady` | 1,000 | 10 | gateway가 여유 있을 때의 지연 |
| `saturated` | 2,000 | 20 | 코어 하나가 꽉 찼을 때 초당 몇 프레임을 보내는가 |

조건의 이름은 2코어 러너 기준이다. gateway와 loadgen에 코어를 하나씩 주면 `saturated`에서 gateway 코어가 100%가 되고 지연이 초 단위로 밀린다(2026-11-20 첫 실행에서 초당 약 11만 프레임). 코어가 많은 기계에서는 같은 조건이 포화에 이르지 않는다.

공용 러너는 이웃의 부하에 따라 값이 흔들리므로 지연과 처리량은 실패 조건이 아니다. 같은 실행 안의 기준 값과만 견준다. 실패 조건은 전달이 온전한지다. 모든 실행에서 연결 실패, 순번 빠짐, 예상 밖 끊김, 짝 없는 표본이 0이어야 하고, `steady`에서는 `delivery.ratio`가 1이어야 한다. 결과 JSON은 `bench-results` 아티팩트로 남는다.

로컬에서도 같은 스크립트를 쓸 수 있다. 횟수, 시간, 조건은 환경 변수로 바꾼다.

```sh
bench/ci.sh                                   # 지금 코드만
bench/ci.sh ../ws-trade-server-main           # 기준 체크아웃과 번갈아
BENCH_ROUNDS=2 BENCH_SECONDS=5 BENCH_SCENARIOS="steady:500:5" bench/ci.sh
```

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

## 쓰기 묶음

gateway의 연결 태스크는 송신 큐에 이미 쌓여 있는 프레임을 4개까지 쓰기 버퍼에 모은 뒤 소켓에 한 번만 쓴다(`src/connection/batch.rs`). 그 전에는 프레임마다 한 번씩 썼다. 아래는 묶기 전(`linux-batch-off-*.json`)과 뒤(`linux-batch-4-*.json`)를 `bench/ci.sh`로 같은 가상 머신에서 번갈아 10초씩 세 번 돌린 중앙값이다(2026-11-20, 연결 10,000개, gateway 코어 0-3, loadgen 코어 4-9, `--poll-us 200`).

| 종목당 초당 건수 | 보낼 프레임/초 | p50 ms | p99 ms | gateway CPU % | RSS MB | 받은 프레임/초 | 끊긴 연결 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 5 | 15만 | 16.9 → 17.7 | 44.9 → 45.7 | 104 → 113 | 142 → 150 | 14.7만 → 15.6만 | 0 → 0 |
| 40 | 120만 | 25.8 → 20.5 | 70.4 → 51.6 | 355 → 340 | 141 → 166 | 117만 → 120만 | 0 → 0 |
| 80 | 240만 | 242 → 48.8 | 494 → 155 | 399 → 389 | 168 → 167 | 239만 → 242만 | 0 → 0 |
| 120 | 360만 | 1,882 → 613 | 3,940 → 1,398 | 392 → 391 | 521 → 274 | 266만 → 341만 | 413~1,582 → 0 |

초당 5건에서는 큐에 프레임이 하나씩만 있어 묶을 것이 없다. 그 줄의 차이는 잡음이다. 같은 코드를 양쪽에 두고 돌려도 p50이 17.2ms와 17.8ms, CPU가 103~119%로 흔들렸다. 초당 40건부터 프레임이 큐에 쌓이고, 상한을 16으로 두고 세어 보면 쓰기 한 번에 평균 2.3개(초당 80건에서는 5.1개)가 나갔다. 초당 120건은 4코어가 감당하지 못하는 부하다. 묶기 전에는 송신 큐가 넘친 연결이 실행마다 413~1,582개 끊겼고, 묶은 뒤에는 끊긴 연결이 없었다.

쓰기 횟수가 절반쯤으로 줄어도 CPU는 4%쯤만 준다. `perf`로 보면 gateway CPU의 46~50%가 커널의 TCP 경로이고, 나머지는 비동기 런타임과 웹소켓 라이브러리에 함수마다 4% 미만으로 흩어져 있다. 그래서 초당 5건처럼 묶을 것이 없는 부하의 지연은 이 변경으로 줄지 않는다.

상한을 4로 둔 것은 메모리 때문이다. 웹소켓 라이브러리의 쓰기 버퍼는 한번 커지면 줄지 않아서 묶은 만큼의 메모리가 연결마다 남는다. 상한 16(`linux-batch-16-*.json`)과 4(`linux-batch-4r-*.json`)를 같은 방법으로 번갈아 재면 초당 80건에서 p50은 25.5ms 대 57.3ms로 16이 낮지만 RSS가 219MB 대 173MB로 46MB 더 들고, 초당 40건에서는 p50 19.1ms 대 22.1ms에 RSS 199MB 대 166MB였다.

이 표의 값은 같은 기계에서 다른 빌드와 테스트가 함께 돌던 때에 잰 것이다. 절대값보다 같은 줄의 앞뒤 차이를 본다. 결과 파일의 `environment.commit`은 null이다. 저장소를 git worktree로 받아 컨테이너에 물리면 그 안에서 저장소를 읽지 못한다. `off`와 `4`는 `feat/27-logging`의 28a4e54에 `bench/run.sh`의 포트 읽기 수정을 얹은 코드와, 거기에 쓰기 묶음을 더한 코드다. `16`과 `4r`은 그 바로 앞 커밋인 ee996ae에서 같은 변경의 상한만 바꿔 잰 것이다(두 커밋의 차이는 문서 하나다).
