#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
temporary=$(mktemp -d)
export WTS_SECRETS_DIR="$temporary/secrets" WTS_PORT=0
export COMPOSE_PROJECT_NAME="wts-smoke-$(node -p 'require("crypto").randomUUID()')"
client_pid=
cleanup() {
  result=$?
  trap - EXIT
  if [ "$result" -ne 0 ]; then docker compose logs --no-color --tail 100 || true; fi
  if [ -n "$client_pid" ]; then kill "$client_pid" 2>/dev/null || true; wait "$client_pid" 2>/dev/null || true; fi
  if ! docker compose down --volumes --timeout 10; then result=1; fi
  rm -rf "$temporary"
  exit "$result"
}
trap cleanup EXIT
node deploy/prepare.js "$WTS_SECRETS_DIR"
docker compose up --detach --no-build --wait --wait-timeout 120
endpoint="wss://$(docker compose port edge 8443)/ws"
node deploy/check.js "$endpoint" "$WTS_SECRETS_DIR"
node deploy/check.js "$endpoint" "$WTS_SECRETS_DIR" reject
for service in feed gateway edge; do
  container=$(docker compose ps --quiet "$service")
  test "$(docker inspect --format '{{.Config.User}} {{.HostConfig.ReadonlyRootfs}}' "$container")" = '10001:10001 true'
done
# feed만 멈추면 gateway 자체는 살아 있어도 새 체결을 제공할 준비는 되지 않는다.
docker compose stop feed
for attempt in {1..20}; do
  if ! docker compose exec -T gateway container-probe ready; then break; fi
  sleep 1
done
if docker compose exec -T gateway container-probe ready; then
  echo 'feed 중지 후에도 gateway readiness가 성공했습니다.' >&2
  exit 1
fi
docker compose exec -T gateway container-probe live
docker compose start feed
for attempt in {1..20}; do
  if docker compose exec -T gateway container-probe ready; then break; fi
  sleep 1
done
docker compose exec -T gateway container-probe ready
node deploy/check.js "$endpoint" "$WTS_SECRETS_DIR"
node deploy/check.js "$endpoint" "$WTS_SECRETS_DIR" shutdown > "$temporary/shutdown.log" 2>&1 &
client_pid=$!
for attempt in {1..10}; do
  if grep -q ready-for-shutdown "$temporary/shutdown.log"; then break; fi
  kill -0 "$client_pid"
  sleep 1
done
grep -q ready-for-shutdown "$temporary/shutdown.log"
docker compose stop --timeout 10
wait "$client_pid"
client_pid=
cat "$temporary/shutdown.log"
container=$(docker compose ps --all --quiet gateway)
test "$(docker inspect --format '{{.State.ExitCode}}' "$container")" = 0
