#!/usr/bin/env bash
# 같은 조건을 여러 번 돌려 표로 요약한다. CI(.github/workflows/bench.yml)가 부르고, 로컬에서도 돌릴 수 있다.
# 기준 체크아웃 경로를 주면 기준과 지금 코드를 번갈아 돌려 나란히 적는다. 사용법은 bench/README.md.
set -euo pipefail
cd "$(dirname "$0")/.."

base=${1:-}
rounds=${BENCH_ROUNDS:-3}
args=(--connections "${BENCH_CONNECTIONS:-2000}" --probes 50 --rate "${BENCH_RATE:-20}" --warmup 2 --seconds "${BENCH_SECONDS:-10}")

# 번갈아 돌려야 러너의 상태가 시간에 따라 바뀌어도 한쪽에만 쏠리지 않는다.
for round in $(seq "$rounds"); do
  [ -z "$base" ] || (cd "$base" && bench/run.sh "ci-base-$round" "${args[@]}" >/dev/null)
  bench/run.sh "ci-head-$round" "${args[@]}" >/dev/null
done

# 한 실행이 한 줄이고, 마지막 줄은 열마다의 중앙값이다.
rows() {
  local label=$1
  shift
  jq -rs --arg label "$label" '
    def ms: if . == null then "-" else (. / 10 | round / 100 | tostring) end;
    def median: map(select(. != null)) | sort | if length == 0 then null else .[(length - 1) / 2 | floor] end;
    def row(name): "| \(name) | \(.p50 | ms) | \(.p90 | ms) | \(.p99 | ms) | \(.cpu // "-") | \(.rss_mb // "-") | \(.per_sec // "-") | \(.ratio // "-") |";
    map({
      p50: .latency_us.p50, p90: .latency_us.p90, p99: .latency_us.p99,
      cpu: .gateway.cpu_pct_from_time, rss_mb: ((.gateway.rss_kb_max // 0) / 1024 | round),
      per_sec: (.messages.per_sec | round), ratio: .delivery.ratio
    })
    | (to_entries[] | . as $run | $run.value | row("\($label) \($run.key + 1)")),
      ({p50: (map(.p50) | median), p90: (map(.p90) | median), p99: (map(.p99) | median), cpu: (map(.cpu) | median),
        rss_mb: (map(.rss_mb) | median), per_sec: (map(.per_sec) | median), ratio: (map(.ratio) | median)} | row("**\($label) 중앙값**"))
  ' "$@"
}

{
  echo "### 벤치마크: 연결 ${args[1]}개, 종목당 초당 ${args[5]}건, ${args[9]}초씩 ${rounds}회"
  echo
  echo "| 실행 | p50 ms | p90 ms | p99 ms | gateway CPU % | RSS MB | 받은 프레임/초 | 전달 비율 |"
  echo "| --- | --- | --- | --- | --- | --- | --- | --- |"
  [ -z "$base" ] || rows "기준" "$base"/bench/results/ci-base-*.json
  rows "이번" bench/results/ci-head-*.json
  echo
  echo "공용 러너에서 잰 값이라 실행마다 흔들린다. 기준과 이번 코드를 같은 러너에서 번갈아 돌린 것끼리만 견준다."
} >>"${GITHUB_STEP_SUMMARY:-/dev/stdout}"

# 지연은 러너에 따라 달라지므로 실패 조건으로 쓰지 않는다. 전달이 온전한지만 본다.
jq -es 'all(.[]; .connect.failed == 0 and .seq_gap_clients == 0 and .unexpected_disconnects == 0
        and .latency_us.unmatched == 0 and .latency_us.samples > 0 and (.delivery.ratio // 1) == 1)' \
  bench/results/ci-head-*.json >/dev/null || { echo "전달이 온전하지 않은 실행이 있다(연결 실패, 순번 빠짐, 끊김, 짝 없는 표본, 전달 비율)" >&2; exit 1; }
