use serde::Serialize;
use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

// STOMP 세션이 끝난 사유. session::run이 돌려주는 값과 같다.
const CLOSE_REASONS: &[&str] = &[
    "peer_closed",
    "client_disconnect",
    "read_error",
    "write_error",
    "write_timeout",
    "invalid_frame",
    "invalid_command",
    "unsupported_command",
    "unsupported_message",
    "command_queue_full",
    "idle_timeout",
    "server_shutdown",
    "hub_closed",
    "slow_consumer",
    "auth_invalidated",
    "auth-expired",
    "auth-revoked",
    "rate-limit-exceeded",
    "unknown-topic",
    "too-many-subscriptions",
];
// 세션을 시작하기 전에 거절한 사유.
const REJECT_REASONS: &[&str] =
    &["connection_limit", "not_ready", "handshake_failed", "handshake_timeout", "auth-invalid", "auth-expired"];
const SUBSCRIPTION_REASONS: &[&str] =
    &["forbidden-topic", "unknown-topic", "too-many-subscriptions", "rate-limit-exceeded"];

// 사유 label은 고정 목록과 other로만 만든다. 목록 밖 사유가 와도 시계열 수가 늘지 않는다.
pub(crate) struct Reasons {
    names: &'static [&'static str],
    counts: Box<[AtomicU64]>,
}

impl Reasons {
    pub fn new(names: &'static [&'static str]) -> Self {
        Self { names, counts: (0..=names.len()).map(|_| AtomicU64::new(0)).collect() }
    }

    pub fn add(&self, reason: &str) {
        let index = self.names.iter().position(|name| *name == reason).unwrap_or(self.names.len());
        self.counts[index].fetch_add(1, Ordering::Relaxed);
    }

    pub fn write(&self, output: &mut String, name: &str) {
        let _ = writeln!(output, "# TYPE {name} counter");
        for (reason, count) in self.names.iter().chain(&["other"]).zip(&self.counts) {
            let _ = writeln!(output, "{name}{{reason=\"{reason}\"}} {}", count.load(Ordering::Relaxed));
        }
    }
}

// hub 상태 밖에서 여러 태스크가 올리는 카운터. hub와 함께 만들어져 함께 0에서 시작한다.
pub(crate) struct Counters {
    started: SystemTime,
    pub closed: Reasons,
    pub rejected: Reasons,
    pub subscription_rejections: Reasons,
}

impl Default for Counters {
    fn default() -> Self {
        Self {
            started: SystemTime::now(),
            closed: Reasons::new(CLOSE_REASONS),
            rejected: Reasons::new(REJECT_REASONS),
            subscription_rejections: Reasons::new(SUBSCRIPTION_REASONS),
        }
    }
}

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

// JSON 필드 이름에 wts_ 접두사를 붙이고, _total로 끝나는 것만 counter로 낸다.
// 시작 시각이 바뀌면 counter가 0부터 다시 센 것이다. 응답이 없는 것(미수집)과 구분된다.
pub(crate) fn prometheus(metrics: &Metrics, counters: &Counters) -> String {
    let mut output = String::new();
    let started = counters.started.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs_f64();
    let _ = writeln!(output, "# TYPE wts_start_time_seconds gauge\nwts_start_time_seconds {started}");
    for (name, value) in [
        ("logs_dropped_total", metrics.logs_dropped_total),
        ("stomp_connections", metrics.stomp_connections as u64),
        ("subscriptions", metrics.subscriptions as u64),
        ("queued_frames", metrics.queued_frames as u64),
        ("queue_capacity", metrics.queue_capacity as u64),
        ("commands_queued", metrics.commands_queued as u64),
        ("published_total", metrics.published_total),
        ("enqueued_frames_total", metrics.enqueued_frames_total),
        ("slow_disconnections_total", metrics.slow_disconnections_total),
        ("latest_skipped_total", metrics.latest_skipped_total),
        ("ring_overruns_total", metrics.ring_overruns_total),
        ("sequence_missing_total", metrics.sequence_missing_total),
        ("sequence_resets_total", metrics.sequence_resets_total),
        ("decode_failures_total", metrics.decode_failures_total),
        ("invalid_trades_total", metrics.invalid_trades_total),
    ] {
        let kind = if name.ends_with("_total") { "counter" } else { "gauge" };
        let _ = writeln!(output, "# TYPE wts_{name} {kind}\nwts_{name} {value}");
    }
    counters.closed.write(&mut output, "wts_connections_closed_total");
    counters.rejected.write(&mut output, "wts_connections_rejected_total");
    counters.subscription_rejections.write(&mut output, "wts_subscription_rejections_total");
    output
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

    #[test]
    fn unknown_reasons_fold_into_other_without_new_series() {
        let reasons = Reasons::new(&["a", "b"]);
        for reason in ["a", "x", "y", "a"] {
            reasons.add(reason);
        }
        let mut output = String::new();
        reasons.write(&mut output, "wts_test_total");
        assert_eq!(
            output,
            "# TYPE wts_test_total counter\nwts_test_total{reason=\"a\"} 2\nwts_test_total{reason=\"b\"} 0\nwts_test_total{reason=\"other\"} 2\n"
        );
    }
}
