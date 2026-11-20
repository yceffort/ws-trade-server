#!/usr/bin/env bash
# 정해 둔 조건을 여러 번 돌려 표로 요약한다. CI(.github/workflows/bench.yml)가 부르고, 로컬에서도 돌릴 수 있다.
# 기준 체크아웃 경로를 주면 기준과 지금 코드를 번갈아 돌려 나란히 적는다. 사용법은 bench/README.md.
set -euo pipefail
cd "$(dirname "$0")/.."

base=${1:-}
rounds=${BENCH_ROUNDS:-3}
seconds=${BENCH_SECONDS:-10}
# 이름:연결 수:종목당 초당 건수. 2코어 러너에서 steady는 gateway 코어의 일부만 쓰고, saturated는 코어 하나로 다 보내지 못한다.
scenarios=${BENCH_SCENARIOS:-steady:1000:10 saturated:2000:20}

# 이전 호출이 남긴 결과가 표와 검사에 섞이지 않게 지운다.
rm -f bench/results/ci-head-*.json
[ -z "$base" ] || rm -f "$base"/bench/results/ci-base-*.json

# 번갈아 돌려야 러너의 상태가 시간에 따라 바뀌어도 한쪽에만 쏠리지 않는다.
for round in $(seq "$rounds"); do
  for scenario in $scenarios; do
    IFS=: read -r name connections rate <<<"$scenario"
    args=(--connections "$connections" --probes 50 --rate "$rate" --warmup 2 --seconds "$seconds")
    # CARGO_TARGET_DIR를 같이 쓰면 두 체크아웃의 실행 파일이 같은 경로에 놓여 한쪽이 다른 쪽 것을 돌린다. 기준은 그 아래 base/에 빌드한다.
    [ -z "$base" ] || (
      cd "$base"
      [ -z "${CARGO_TARGET_DIR:-}" ] || export CARGO_TARGET_DIR="$CARGO_TARGET_DIR/base"
      bench/run.sh "ci-base-$name-$round" "${args[@]}" >/dev/null
    )
    bench/run.sh "ci-head-$name-$round" "${args[@]}" >/dev/null
  done
done

# 한 실행이 한 줄이고, 마지막 줄은 열마다의 중앙값이다.
rows() {
  local label=$1
  shift
  jq -rs --arg label "$label" '
    def ms: if . == null then "-" else (. / 10 | round / 100 | tostring) end;
    def median: map(select(. != null)) | sort | length as $n
      | if $n == 0 then null elif $n % 2 == 1 then .[($n - 1) / 2] else (.[$n / 2 - 1] + .[$n / 2]) / 2 end;
    def rounded(unit): if . == null then null else (. / unit | round * unit * 10000 | round / 10000) end;
    def row(name): "| \(name) | \(.p50 | ms) | \(.p90 | ms) | \(.p99 | ms) | \(.cpu // "-") | \(.rss_mb // "-") | \(.per_sec // "-") | \(.ratio // "-") |";
    map({
      p50: .latency_us.p50, p90: .latency_us.p90, p99: .latency_us.p99,
      cpu: .gateway.cpu_pct_from_time, rss_mb: ((.gateway.rss_kb_max // 0) / 1024 | round),
      per_sec: (.messages.per_sec | round), ratio: .delivery.ratio
    })
    | (to_entries[] | . as $run | $run.value | row("\($label) \($run.key + 1)")),
      ({p50: (map(.p50) | median), p90: (map(.p90) | median), p99: (map(.p99) | median), cpu: (map(.cpu) | median | rounded(0.1)),
        rss_mb: (map(.rss_mb) | median | rounded(1)), per_sec: (map(.per_sec) | median | rounded(1)),
        ratio: (map(.ratio) | median | rounded(0.0001))} | row("**\($label) 중앙값**"))
  ' "$@"
}

{
  for scenario in $scenarios; do
    IFS=: read -r name connections rate <<<"$scenario"
    echo "### $name: 연결 ${connections}개, 종목당 초당 ${rate}건, ${seconds}초씩 ${rounds}회"
    echo
    echo "| 실행 | p50 ms | p90 ms | p99 ms | gateway CPU % | RSS MB | 받은 프레임/초 | 전달 비율 |"
    echo "| --- | --- | --- | --- | --- | --- | --- | --- |"
    [ -z "$base" ] || rows "기준" "$base"/bench/results/ci-base-"$name"-*.json
    rows "이번" bench/results/ci-head-"$name"-*.json
    echo
  done
  echo "공용 러너에서 잰 값이라 실행마다 흔들린다. 기준과 이번 코드를 같은 러너에서 번갈아 돌린 것끼리만 견준다."
} >>"${GITHUB_STEP_SUMMARY:-/dev/stdout}"

# 지연과 처리량은 러너에 따라 달라지므로 실패 조건으로 쓰지 않는다. 전달이 온전한지만 본다.
# saturated는 일부러 넘치게 흘리므로 마지막 1초 안에 못 온 체결이 생긴다. 전달 비율은 steady에서만 따진다.
jq -es 'all(.[]; .connect.failed == 0 and .seq_gap_clients == 0 and .unexpected_disconnects == 0
        and .latency_us.unmatched == 0 and .latency_us.samples > 0)' bench/results/ci-head-*.json >/dev/null &&
  jq -es 'all(.[]; (.delivery.ratio // 1) == 1)' bench/results/ci-head-steady-*.json >/dev/null ||
  { echo "전달이 온전하지 않은 실행이 있다(연결 실패, 순번 빠짐, 끊김, 짝 없는 표본, steady의 전달 비율)" >&2; exit 1; }
