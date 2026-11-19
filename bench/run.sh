#!/usr/bin/env bash
# 한 조건을 처음부터 끝까지 돌려 bench/results/<이름>.json을 만든다. 사용법은 bench/README.md.
set -euo pipefail
cd "$(dirname "$0")/.."

[ $# -ge 1 ] && [[ "$1" != --* ]] || { echo "사용법: bench/run.sh <이름> [--poll-us N] [loadgen 인자...]" >&2; exit 2; }
name=$1
shift
command -v jq >/dev/null || { echo "jq가 필요하다" >&2; exit 1; }

poll_us=200
connections=100
loadgen_args=()
while [ $# -gt 0 ]; do
  case $1 in
    --poll-us) poll_us=${2:?}; shift 2 ;;
    --connections) connections=${2:?}; loadgen_args+=("$1" "$2"); shift 2 ;;
    --url | --ring | --out) echo "$1은 스크립트가 정한다" >&2; exit 2 ;;
    *) loadgen_args+=("$1"); shift ;;
  esac
done

# 연결마다 파일 디스크립터를 하나 쓴다. gateway와 loadgen 모두 이 한도를 물려받는다.
need=$((connections + 256))
ulimit -n 65536 2>/dev/null || ulimit -n "$(ulimit -Hn)" 2>/dev/null || true
[ "$(ulimit -n)" -ge "$need" ] || { echo "ulimit -n이 $(ulimit -n)라 연결 $connections개를 못 연다(필요 $need). 한도를 올릴 수 없다." >&2; exit 1; }

target=${CARGO_TARGET_DIR:-target}/release
cargo build --release --quiet --bin gateway --bin loadgen

work=$(mktemp -d)
gateway_pid=""
loadgen_pid=""
cleanup() {
  [ -z "$loadgen_pid" ] || kill "$loadgen_pid" 2>/dev/null || true
  [ -z "$gateway_pid" ] || kill "$gateway_pid" 2>/dev/null || true
  [ -z "$gateway_pid" ] || wait "$gateway_pid" 2>/dev/null || true
  rm -rf "$work"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

"$target/gateway" --addr 127.0.0.1:0 --ring "$work/ring" --poll-us "$poll_us" 2>"$work/gateway.log" &
gateway_pid=$!
port=""
for _ in $(seq 50); do
  # gateway는 로그 한 줄을 여러 번에 나눠 쓴다. read는 줄바꿈까지 쓰인 줄만 성공하므로 덜 쓰인 줄에서 잘린 포트를 뽑지 않는다.
  while IFS= read -r line; do
    case $line in *"수신 대기"*) port=${line##*:} ;; esac
  done <"$work/gateway.log"
  [ -z "$port" ] || break
  sleep 0.1
done
[ -n "$port" ] || { echo "gateway가 뜨지 않았다:" >&2; cat "$work/gateway.log" >&2; exit 1; }

now() { perl -MTime::HiRes=time -e 'printf "%.3f", time'; }
# 시각, RSS(KB), ps의 %cpu, 누적 CPU 시간(초), 측정 구간 안인지(1/0)를 한 줄로 쓴다.
sample() {
  local in_window=0
  if grep -q '^phase=measure' "$work/loadgen.err" 2>/dev/null && ! grep -q '^phase=trailing' "$work/loadgen.err"; then in_window=1; fi
  ps -o rss=,%cpu=,time= -p "$gateway_pid" | awk -v t="$(now)" -v w="$in_window" '{
    n = split($3, p, ":"); s = 0; for (i = 1; i <= n; i++) s = s * 60 + p[i]
    print t, $1, $2, s, w }'
}

idle_rss=$(sample | awk '{print $2}')
"$target/loadgen" --url "ws://127.0.0.1:$port/ws" --ring "$work/ring" ${loadgen_args[@]+"${loadgen_args[@]}"} --out "$work/loadgen.json" 2>"$work/loadgen.err" &
loadgen_pid=$!
while kill -0 "$loadgen_pid" 2>/dev/null; do
  sample >>"$work/samples.txt" || true
  sleep 1
done
status=0
wait "$loadgen_pid" || status=$?
loadgen_pid=""
if [ "$status" -ne 0 ]; then cat "$work/loadgen.err" >&2; echo "loadgen 실패($status)" >&2; exit "$status"; fi

# ps의 %cpu는 구간 값이 아니다(macOS는 감쇠 평균, Linux는 프로세스 수명 평균). 구간 앞의 부하가 섞이므로 누적 CPU 시간의 증가분도 함께 남긴다.
gateway=$(awk -v idle="$idle_rss" -v poll="$poll_us" '
  $5 == 1 {
    n++; if ($2 > rss) rss = $2; sum += $3; if ($3 > max) max = $3
    if (n == 1) { t0 = $1; c0 = $4 } t1 = $1; c1 = $4
  }
  END {
    if (n == 0) { print "null"; exit }
    printf "{\"poll_us\":%d,\"rss_kb_idle\":%d,\"rss_kb_max\":%d,\"cpu_pct_mean\":%.1f,\"cpu_pct_max\":%.1f,\"cpu_pct_from_time\":%s,\"samples\":%d}\n",
      poll, idle, rss, sum / n, max, (t1 > t0 ? sprintf("%.1f", (c1 - c0) / (t1 - t0) * 100) : "null"), n
  }' "$work/samples.txt")

cpu=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || awk -F': ' '/model name/ { print $2; exit }' /proc/cpuinfo)
cores=$(sysctl -n hw.ncpu 2>/dev/null || nproc)
commit=$(git rev-parse --verify --quiet HEAD 2>/dev/null || true)
environment=$(jq -n --arg os "$(uname -srm)" --arg cpu "$cpu" --argjson cores "$cores" --arg rustc "$(rustc --version)" --arg commit "$commit" \
  '{os: $os, cpu_model: $cpu, cores: $cores, rustc: $rustc, commit: (if $commit == "" then null else $commit end)}')

mkdir -p bench/results
jq --argjson gateway "$gateway" --argjson environment "$environment" '. + {gateway: $gateway, environment: $environment}' \
  "$work/loadgen.json" >"bench/results/$name.json"
echo "bench/results/$name.json"
