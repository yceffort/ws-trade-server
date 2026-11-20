#!/bin/sh
set -eu
case "${1:-}" in
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
  *) echo "사용법: container-probe feed|live|ready" >&2; exit 2 ;;
esac
