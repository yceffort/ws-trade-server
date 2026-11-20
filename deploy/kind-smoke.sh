#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
temporary=$(mktemp -d)
export KUBECONFIG="$temporary/kubeconfig"
cluster="wts-smoke-$(node -p 'require("crypto").randomUUID()')"
forward_pid= client_pid=
cleanup() {
  result=$?
  trap - EXIT
  if [ "$result" -ne 0 ]; then
    kubectl -n ws-trade get pods || true
    kubectl -n ws-trade describe pods || true
    kubectl -n ws-trade logs -l app=ws-trade --all-containers --tail=100 || true
  fi
  for pid in "$client_pid" "$forward_pid"; do
    if [ -n "$pid" ]; then kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; fi
  done
  if ! kind delete cluster --name "$cluster"; then result=1; fi
  rm -rf "$temporary"
  exit "$result"
}
trap cleanup EXIT
# 기존 kubeconfig와 클러스터는 사용하지 않는다. kind 0.32.0이 배포한 양쪽 CPU용 이미지다.
kind create cluster --name "$cluster" --wait 120s --image kindest/node:v1.35.5@sha256:ce977ae6d65918d0b58a5f8b5e940429c2ce42fa3a5619ec2bbc60b949c0ac95
kind load docker-image ws-trade-server:local ws-trade-edge:local --name "$cluster"
node deploy/prepare.mjs "$temporary/secrets"
kubectl apply -f deploy/k8s/namespace.yaml
kubectl -n ws-trade create secret generic ws-trade-credentials --from-file="$temporary/secrets"
kubectl apply --dry-run=server -k deploy/k8s
kubectl apply -k deploy/k8s
kubectl -n ws-trade rollout status deployment/ws-trade --timeout=120s
forward() {
  kubectl -n ws-trade port-forward --address=127.0.0.1 service/ws-trade :443 > "$temporary/forward.log" 2>&1 &
  forward_pid=$!
  for attempt in {1..20}; do
    if grep -q 'Forwarding from' "$temporary/forward.log"; then break; fi
    kill -0 "$forward_pid"
    sleep 1
  done
  endpoint="wss://$(sed -n 's/^Forwarding from \(127.0.0.1:[0-9]*\).*/\1/p' "$temporary/forward.log")/ws"
  test "$endpoint" != 'wss:///ws'
}
forward
node deploy/check.mjs "$endpoint" "$temporary/secrets"
node deploy/check.mjs "$endpoint" "$temporary/secrets" reject
pod=$(kubectl -n ws-trade get pod -l app=ws-trade -o jsonpath='{.items[0].metadata.name}')
restarts=$(kubectl -n ws-trade get pod "$pod" -o jsonpath='{.status.containerStatuses[?(@.name=="feed")].restartCount}')
kubectl -n ws-trade exec "$pod" -c feed -- sh -c 'kill -TERM 1'
for attempt in {1..30}; do
  count=$(kubectl -n ws-trade get pod "$pod" -o jsonpath='{.status.containerStatuses[?(@.name=="feed")].restartCount}')
  if [ "$count" -gt "$restarts" ]; then break; fi
  sleep 1
done
test "$count" -gt "$restarts"
kubectl -n ws-trade wait --for=condition=Ready "pod/$pod" --timeout=60s
node deploy/check.mjs "$endpoint" "$temporary/secrets"
node deploy/check.mjs "$endpoint" "$temporary/secrets" shutdown > "$temporary/shutdown.log" 2>&1 &
client_pid=$!
for attempt in {1..10}; do
  if grep -q ready-for-shutdown "$temporary/shutdown.log"; then break; fi
  kill -0 "$client_pid"
  sleep 1
done
grep -q ready-for-shutdown "$temporary/shutdown.log"
# Pod template을 갱신해 Recreate와 sidecar의 종료 순서를 실제로 거친다.
kubectl -n ws-trade rollout restart deployment/ws-trade
wait "$client_pid"
client_pid=
cat "$temporary/shutdown.log"
kill "$forward_pid" 2>/dev/null || true
wait "$forward_pid" 2>/dev/null || true
forward_pid=
kubectl -n ws-trade rollout status deployment/ws-trade --timeout=120s
test "$(kubectl -n ws-trade get pod -l app=ws-trade -o jsonpath='{.items[0].metadata.name}')" != "$pod"
forward
node deploy/check.mjs "$endpoint" "$temporary/secrets"
