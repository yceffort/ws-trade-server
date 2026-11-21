use serde::Serialize;
use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

// 송신 큐 대기와 소켓 쓰기의 경계(초). 100us부터 5초까지다.
const LATENCY_BOUNDS: &[f64] =
    &[0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0];

// 고정 경계의 histogram. label이 없어 시계열 수는 경계 수에 +Inf, _sum, _count를 더한 만큼으로 일정하다.
pub(crate) struct Histogram {
    bounds: &'static [f64],
    counts: Box<[AtomicU64]>,
    // 마이크로초로 쌓는다. 나노초면 평균 1초인 관측이 초당 10만 건일 때 이틀 만에 넘쳐 되돌아간다.
    sum_micros: AtomicU64,
}

impl Histogram {
    pub fn new(bounds: &'static [f64]) -> Self {
        Self { bounds, counts: (0..=bounds.len()).map(|_| AtomicU64::new(0)).collect(), sum_micros: AtomicU64::new(0) }
    }

    pub fn observe(&self, elapsed: Duration) {
        let seconds = elapsed.as_secs_f64();
        let index = self.bounds.iter().position(|bound| seconds <= *bound).unwrap_or(self.bounds.len());
        self.counts[index].fetch_add(1, Ordering::Relaxed);
        self.sum_micros.fetch_add(u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    pub fn write(&self, output: &mut String, name: &str) {
        let _ = writeln!(output, "# TYPE {name} histogram");
        let mut total = 0;
        for (index, count) in self.counts.iter().enumerate() {
            total += count.load(Ordering::Relaxed);
            let _ = match self.bounds.get(index) {
                Some(bound) => writeln!(output, "{name}_bucket{{le=\"{bound}\"}} {total}"),
                None => writeln!(output, "{name}_bucket{{le=\"+Inf\"}} {total}"),
            };
        }
        let sum = self.sum_micros.load(Ordering::Relaxed) as f64 / 1e6;
        let _ = writeln!(output, "{name}_sum {sum}\n{name}_count {total}");
    }
}

// hub 상태 밖에서 여러 태스크가 올리는 카운터. hub와 함께 만들어져 함께 0에서 시작한다.
pub(crate) struct Counters {
    started: SystemTime,
    pub closed: Reasons,
    pub rejected: Reasons,
    pub subscription_rejections: Reasons,
    // hub가 발행을 처리하기 시작한 때부터 쓰는 태스크가 그 MESSAGE를 큐에서 꺼낼 때까지.
    pub queue_wait: Histogram,
    // 묶음의 프레임을 만들어 웹소켓에 넘기고 flush가 끝날 때까지. 클라이언트가 받거나 적용한 시각이 아니다.
    pub socket_write: Histogram,
}

impl Default for Counters {
    fn default() -> Self {
        Self {
            started: SystemTime::now(),
            closed: Reasons::new(CLOSE_REASONS),
            rejected: Reasons::new(REJECT_REASONS),
            subscription_rejections: Reasons::new(SUBSCRIPTION_REASONS),
            queue_wait: Histogram::new(LATENCY_BOUNDS),
            socket_write: Histogram::new(LATENCY_BOUNDS),
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
    counters.queue_wait.write(&mut output, "wts_queue_wait_seconds");
    counters.socket_write.write(&mut output, "wts_socket_write_seconds");
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
    fn histogram_buckets_are_cumulative_and_sum_in_seconds() {
        let histogram = Histogram::new(&[0.025, 0.05]);
        for millis in [10, 30, 30, 70] {
            histogram.observe(Duration::from_millis(millis));
        }
        let mut output = String::new();
        histogram.write(&mut output, "wait");
        assert_eq!(
            output,
            "# TYPE wait histogram\nwait_bucket{le=\"0.025\"} 1\nwait_bucket{le=\"0.05\"} 3\nwait_bucket{le=\"+Inf\"} 4\nwait_sum 0.14\nwait_count 4\n"
        );
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
