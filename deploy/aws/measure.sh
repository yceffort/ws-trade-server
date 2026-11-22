#!/bin/bash
# 부하 Job 하나를 돌리면서 판정에 필요한 서버 쪽 값(HA 지표, 노드와 컨테이너 CPU, edge 사유 로그, 부하 Pod cgroup)을 함께 모은다.
# 사용: deploy/aws/measure.sh <settings.json> <options.json> <출력 디렉터리>
set -uo pipefail
SETTINGS=$1 OPTIONS=$2 OUT=$3
cd "$(dirname "$0")/../.."
NS=$(node -p "JSON.parse(require('fs').readFileSync('$SETTINGS')).namespace")
CONTEXT=$(kubectl config current-context)
mkdir -p "$OUT"
STARTED=$(date -u +%Y-%m-%dT%H:%M:%SZ)
gateways() { kubectl -n "$NS" get pods -l app.kubernetes.io/component=gateway --field-selector=status.phase=Running -o name; }

# replay_waiters 같은 gauge는 끝난 뒤의 값으로는 포화 여부를 알 수 없어 실행 중에 5초마다 남긴다.
(while :; do
  for POD in $(gateways); do
    echo "# $(date -u +%H:%M:%S) $POD" >> "$OUT/ha-metrics.txt"
    kubectl -n "$NS" exec "$POD" -c gateway -- curl --silent --max-time 2 http://127.0.0.1:8081/ha-metrics 2>/dev/null \
      | grep -E '^wts_ha_(replay_waiters|replay_workers|active_connections|source_age_seconds|broker_age_seconds) ' >> "$OUT/ha-metrics.txt"
  done
  { echo "# $(date -u +%H:%M:%S)"; kubectl top nodes --no-headers 2>/dev/null; kubectl -n "$NS" top pods --containers --no-headers 2>/dev/null; } >> "$OUT/top.txt"
  sleep 5
done) &
SAMPLER=$!
trap 'kill $SAMPLER 2>/dev/null' EXIT

node bench/ha/job-run.mjs "$CONTEXT" "$SETTINGS" "$OPTIONS" "$OUT"
STATUS=$?
kill $SAMPLER 2>/dev/null; wait $SAMPLER 2>/dev/null

LOAD=$(kubectl -n "$NS" get pods -l app.kubernetes.io/component=ha-load -o name | head -1)
# job-run.mjs는 kubectl 호출이 한 번만 실패해도 보고서 없이 끝난다(AZ 장애 시험에서 관측). 그때는 부하 Pod가 보고서를 쓸 때까지 직접 기다린다.
if [ ! -s "$OUT/report.json" ] && [ -n "$LOAD" ]; then
  for _ in $(seq 240); do
    kubectl -n "$NS" exec "$LOAD" -c load -- cat /state/report.json > "$OUT/report.json" 2>/dev/null && [ -s "$OUT/report.json" ] && { STATUS=0; break; }
    sleep 5
  done
fi
[ -z "$LOAD" ] || kubectl -n "$NS" exec "$LOAD" -c load -- sh -c 'cat /sys/fs/cgroup/memory.peak /sys/fs/cgroup/memory.events /sys/fs/cgroup/cpu.stat' > "$OUT/load-cgroup.txt" 2>/dev/null
for POD in $(gateways); do
  kubectl -n "$NS" logs "$POD" -c edge --since-time="$STARTED" 2>/dev/null | grep edge_summary > "$OUT/edge-$(basename "$POD").log"
done
# 끝난 Pod가 남아 있으면 다음 실행의 job-run.mjs가 보고서 없이 종료한 Pod로 읽는다. Pod가 사라질 때까지 기다린다.
kubectl -n "$NS" delete job wts-ha-load --ignore-not-found --wait --timeout=120s >/dev/null
kubectl -n "$NS" delete pods -l app.kubernetes.io/component=ha-load --ignore-not-found --grace-period=1 --wait --timeout=120s >/dev/null
exit $STATUS
