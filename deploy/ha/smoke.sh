#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
suffix=$(node -e 'process.stdout.write(require("node:crypto").randomBytes(6).toString("hex"))')
[[ "$suffix" =~ ^[0-9a-f]{12}$ ]]
export COMPOSE_PROJECT_NAME="wts-ha-test-$suffix"
export WTS_HA_TEST_CONFIG="$PWD/deploy/local/ha"
export WTS_HA_ALLOW_FAULTS=owned-test-cluster
compose=(docker compose -f deploy/ha/compose.yaml)
cleanup() {
  status=$?
  trap - EXIT
  if ((status != 0)); then node deploy/ha/diagnose.mjs || true; fi
  if ! "${compose[@]}" down --volumes --remove-orphans; then
    echo "Compose 정리 실패: $COMPOSE_PROJECT_NAME" >&2
    if ((status == 0)); then status=1; fi
  fi
  exit "$status"
}
trap cleanup EXIT
node deploy/ha/prepare.mjs
npm ci --prefix edge --ignore-scripts
cargo build --locked --features ha --bins
cargo test --locked --features ha --lib https_policy_revocation_and_provider_outage_obey_the_sixty_second_bound -- --ignored --nocapture
"${compose[@]}" up -d --wait --wait-timeout 180
deploy/ha/bootstrap.sh
cargo test --locked --features ha --test ha_broker synthetic_checkpoint_fencing_and_snapshot_are_atomic -- --ignored --nocapture
cargo test --locked --features ha --test ha_broker recovery_survives_more_than_4096_aborted_positions -- --ignored --nocapture
cargo test --locked --features ha --lib cancelled_reader_releases_its_permit_after_native_cleanup -- --ignored --nocapture
cargo test --locked --features ha --test ha_replica -- --ignored --nocapture
cargo test --locked --features ha --test ha_protocol -- --ignored --nocapture
WTS_HA_SHARED_TEST=1 cargo test --locked --features ha --test ha_protocol -- --ignored --nocapture
cargo test --locked --features ha --test ha_lifecycle -- --ignored --nocapture
mkdir -p deploy/local/ha/results
node deploy/ha/producer-failover.mjs | tee deploy/local/ha/results/producer-failover.json
cargo test --locked --features ha --test ha_broker replicated_log_survives_broker_loss_and_rejects_no_quorum -- --ignored --nocapture
# 외부 입력 시험은 다른 빈 데이터셋에서 시작한다. 이 스크립트가 만든 프로젝트의 volume만 지운다.
"${compose[@]}" down --volumes
"${compose[@]}" up -d --wait --wait-timeout 180
deploy/ha/bootstrap.sh
cargo test --locked --features ha --test ha_broker external_replay_gap_and_lost_ack_use_committed_input_window -- --ignored --nocapture
cargo test --locked --features ha --lib unclear_commit_result_restores_committed_input -- --ignored --nocapture
