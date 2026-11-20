use super::State;
use crate::metrics::Metrics;
use std::sync::atomic::Ordering;

impl State {
    pub(super) fn metrics(&self, commands_queued: usize) -> Metrics {
        Metrics {
            stomp_connections: self.clients.len(),
            subscriptions: self.clients.values().map(|client| client.subs.len()).sum(),
            queued_frames: self
                .clients
                .values()
                .map(|client| client.queue.max_capacity() - client.queue.capacity())
                .sum(),
            queue_capacity: self.clients.values().map(|client| client.queue.max_capacity()).sum(),
            commands_queued,
            published_total: self.published,
            enqueued_frames_total: self.enqueued,
            slow_disconnections_total: self.slow_disconnections,
            ring_overruns_total: self.bridge.ring_overruns.load(Ordering::Relaxed),
            sequence_missing_total: self.bridge.sequence_missing.load(Ordering::Relaxed),
            sequence_resets_total: self.bridge.sequence_resets.load(Ordering::Relaxed),
            decode_failures_total: self.bridge.decode_failures.load(Ordering::Relaxed),
            invalid_trades_total: self.bridge.invalid_trades.load(Ordering::Relaxed),
        }
    }
}
