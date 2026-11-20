#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
export COMPOSE_PROJECT_NAME="wts-ha-test-$(node -e 'process.stdout.write(require("node:crypto").randomBytes(6).toString("hex"))')"
export WTS_HA_TEST_CONFIG="$PWD/deploy/local/ha"
export WTS_HA_ALLOW_FAULTS=owned-test-cluster
compose=(docker compose -f deploy/ha/compose.yaml)
cleanup() {
  status=$?
  trap - EXIT
  "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT
node deploy/ha/prepare.mjs
cargo build --locked --features ha --bins
"${compose[@]}" up -d --wait --wait-timeout 180
deploy/ha/bootstrap.sh
cargo test --locked --features ha --test ha_broker synthetic_checkpoint_fencing_and_snapshot_are_atomic -- --ignored --nocapture
mkdir -p deploy/local/ha/results
node deploy/ha/producer-failover.mjs | tee deploy/local/ha/results/producer-failover.json
cargo test --locked --features ha --test ha_broker replicated_log_survives_broker_loss_and_rejects_no_quorum -- --ignored --nocapture
# 외부 입력 시험은 다른 빈 데이터셋에서 시작한다. 이 스크립트가 만든 프로젝트의 volume만 지운다.
"${compose[@]}" down --volumes
"${compose[@]}" up -d --wait --wait-timeout 180
deploy/ha/bootstrap.sh
cargo test --locked --features ha --test ha_broker external_replay_gap_and_lost_ack_use_committed_input_window -- --ignored --nocapture
