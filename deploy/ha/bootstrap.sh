#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
compose=(docker compose -f deploy/ha/compose.yaml)
admin=(--bootstrap-server kafka1:9092 --command-config /config/admin.properties)
topic() { "${compose[@]}" exec -T kafka1 /opt/kafka/bin/kafka-topics.sh "${admin[@]}" "$@"; }
acl() { "${compose[@]}" exec -T kafka1 /opt/kafka/bin/kafka-acls.sh "${admin[@]}" --add "$@" >/dev/null; }
# 기본 1GiB는 개발용이다. 목표 처리량에서 24시간을 보관하려면 docs/ha-capacity.md의 산정대로 늘린다.
retention_ms=${WTS_HA_RETENTION_MS:-86400000}
retention_bytes=${WTS_HA_RETENTION_BYTES:-1073741824}
[[ $retention_ms =~ ^[0-9]+$ && $retention_bytes =~ ^[0-9]+$ ]] && ((retention_ms >= 60000 && retention_bytes >= 16777216))
topic --create --if-not-exists --topic wts.events.v1 --partitions 1 --replication-factor 3 --config min.insync.replicas=2 --config cleanup.policy=delete --config retention.ms="$retention_ms" --config retention.bytes="$retention_bytes" --config max.message.bytes=1048576
# create의 --config는 새 토픽에만 들어간다. 이미 있는 토픽에도 같은 값을 적용한다.
"${compose[@]}" exec -T kafka1 /opt/kafka/bin/kafka-configs.sh "${admin[@]}" --alter --entity-type topics --entity-name wts.events.v1 --add-config "retention.ms=$retention_ms,retention.bytes=$retention_bytes" >/dev/null
for name in wts.checkpoint.v1 wts.snapshot.v1; do
  topic --create --if-not-exists --topic "$name" --partitions 1 --replication-factor 3 --config min.insync.replicas=2 --config cleanup.policy=compact --config max.message.bytes=1048576 --config segment.bytes=16777216 --config segment.ms=60000 --config min.cleanable.dirty.ratio=0.1 --config max.compaction.lag.ms=120000
done
for name in wts.events.v1 wts.checkpoint.v1 wts.snapshot.v1; do
  acl --allow-principal User:feed --topic "$name" --operation Write --operation Read --operation Describe
done
for name in wts.events.v1 wts.snapshot.v1; do
  acl --allow-principal User:gateway --topic "$name" --operation Read --operation Describe
done
acl --allow-principal User:feed --transactional-id wts- --resource-pattern-type prefixed --operation Write --operation Describe
acl --allow-principal User:feed --allow-principal User:gateway --group wts-reader- --resource-pattern-type prefixed --operation Read
topic --describe
