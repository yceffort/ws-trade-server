use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub(crate) struct BridgeCounters {
    pub ring_overruns: AtomicU64,
    pub sequence_missing: AtomicU64,
    pub sequence_resets: AtomicU64,
    pub decode_failures: AtomicU64,
    pub invalid_trades: AtomicU64,
}

impl BridgeCounters {
    pub fn observe_seq(&self, last: u64, seq: u64) {
        if seq < last {
            self.sequence_resets.fetch_add(1, Ordering::Relaxed);
        } else if seq.saturating_sub(last) > 1 {
            self.sequence_missing.fetch_add(seq - last - 1, Ordering::Relaxed);
        }
    }
}

#[derive(Clone, Copy, Serialize)]
pub struct Metrics {
    pub logs_dropped_total: u64,
    pub stomp_connections: usize,
    pub subscriptions: usize,
    pub queued_frames: usize,
    pub queue_capacity: usize,
    pub commands_queued: usize,
    pub published_total: u64,
    pub enqueued_frames_total: u64,
    pub slow_disconnections_total: u64,
    pub latest_skipped_total: u64,
    pub ring_overruns_total: u64,
    pub sequence_missing_total: u64,
    pub sequence_resets_total: u64,
    pub decode_failures_total: u64,
    pub invalid_trades_total: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn gaps_are_counts_and_restarts_are_separate_events() {
        let metrics = BridgeCounters::default();
        for (last, seq) in [(1, 2), (2, 7), (7, 7), (7, 1), (u64::MAX, 0)] {
            metrics.observe_seq(last, seq);
        }
        assert_eq!(metrics.sequence_missing.load(Ordering::Relaxed), 4);
        assert_eq!(metrics.sequence_resets.load(Ordering::Relaxed), 2);
        assert_eq!(metrics.ring_overruns.load(Ordering::Relaxed), 0);
    }
}
