#!/usr/bin/env bash
# 인자로 받은 명령을 Linux 컨테이너에서 돌린다. 사용법은 bench/README.md.
# 예: bench/linux.sh bench/run.sh linux-conn-1000 --connections 1000
set -euo pipefail
cd "$(dirname "$0")/.."
docker build --quiet --tag ws-trade-server-bench bench >/dev/null
# 빌드 산출물과 내려받은 크레이트는 볼륨에 둬서 맥의 target/과 섞이지 않게 한다.
exec docker run --rm --ulimit nofile=65536:65536 \
  --volume "$PWD":/work --volume ws-trade-server-target:/target --volume ws-trade-server-registry:/usr/local/cargo/registry \
  --workdir /work --env CARGO_TARGET_DIR=/target \
  --env GATEWAY_CPUS="${GATEWAY_CPUS-0-3}" --env LOADGEN_CPUS="${LOADGEN_CPUS-4-9}" \
  ws-trade-server-bench "$@"
