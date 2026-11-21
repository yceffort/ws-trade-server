#!/bin/sh
set -eu
port=${WTS_HTTP_PORT:-8081}
case "$port" in ''|*[!0-9]*) exit 2 ;; esac
[ "$port" -ge 1 ] && [ "$port" -le 65535 ] || exit 2
case "${1:-}" in
  ha-live) curl --fail --silent --show-error --max-time 0.8 "http://127.0.0.1:$port/livez" >/dev/null ;;
  ha-ready) curl --fail --silent --show-error --max-time 0.8 "http://127.0.0.1:$port/readyz" >/dev/null ;;
  ha-drain) curl --fail --silent --show-error --max-time 0.8 -X POST "http://127.0.0.1:$port/drain" >/dev/null ;;
  feed) test -s /data/feed.ring ;;
  live) curl --fail --silent --show-error --max-time 2 http://127.0.0.1:8081/metrics >/dev/null ;;
  ready)
    # 합성 feed가 흘러야 준비 상태다. 시장 휴장 여부를 판단하는 운영 정책은 아니다.
    curl --fail --silent --show-error --max-time 2 http://127.0.0.1:8081/snapshots |
      jq -e --arg topics "${WTS_TOPICS:?}" --argjson now "$(date +%s)" '
        . as $snapshots | ($topics | split(",")) as $required |
        ($required | length) > 0 and all($required[];
          . as $topic | ($snapshots[$topic].updated_at_ms // 0) >= (($now - 10) * 1000))
      ' >/dev/null
    ;;
  *) echo "사용법: container-probe feed|live|ready|ha-live|ha-ready|ha-drain" >&2; exit 2 ;;
esac
