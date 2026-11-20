#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
compose=(docker compose -f deploy/ha/compose.yaml)
admin=(--bootstrap-server kafka1:9092 --command-config /config/admin.properties)
topic() { "${compose[@]}" exec -T kafka1 /opt/kafka/bin/kafka-topics.sh "${admin[@]}" "$@"; }
acl() { "${compose[@]}" exec -T kafka1 /opt/kafka/bin/kafka-acls.sh "${admin[@]}" --add "$@" >/dev/null; }
topic --create --if-not-exists --topic wts.events.v1 --partitions 1 --replication-factor 3 --config min.insync.replicas=2 --config cleanup.policy=delete --config retention.ms=86400000 --config retention.bytes=1073741824 --config max.message.bytes=1048576
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
