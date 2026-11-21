use super::{Lifecycle, Protocol, SharedAuth};
use crate::{
    hub::Hub,
    metrics::{Histogram, Reasons},
};
use std::{
    fmt::Write,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

// serve_socket이 연결을 닫는 사유. 목록 밖 사유는 other로 센다.
const CLOSE_REASONS: &[&str] = &[
    "auth-invalid",
    "auth-expired",
    "auth-revoked",
    "auth-required",
    "auth-policy-stale",
    "forbidden-scope",
    "ha-disabled",
    "invalid-request",
    "invalid-resume",
    "invalid-cursor",
    "cursor-expired",
    "start-out-of-range",
    "replay-limit",
    "retry-later",
    "snapshot-too-large",
    "slow-client",
    "source-error",
    "source-stale",
    "heartbeat-timeout",
    "connection-closed",
    "draining",
    "server-shutdown",
];

pub(crate) struct Closes(Reasons);
impl Default for Closes {
    fn default() -> Self {
        Self(Reasons::new(CLOSE_REASONS))
    }
}

// 복구 지연의 경계(초). replay와 snapshot 전송은 10초 기한 안에 끝나야 한다.
const RECOVERY_BOUNDS: &[f64] = &[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];

pub(crate) struct Recovery(pub Histogram);
impl Default for Recovery {
    fn default() -> Self {
        Self(Histogram::new(RECOVERY_BOUNDS))
    }
}

#[derive(Default)]
pub(crate) struct Counters {
    pub connections: AtomicU64,
    pub active: AtomicU64,
    pub resumes: AtomicU64,
    pub caught_up: AtomicU64,
    pub replay_bytes: AtomicU64,
    pub snapshot_bytes: AtomicU64,
    pub checkpoints: AtomicU64,
    pub replay_limits: AtomicU64,
    pub cursor_rejections: AtomicU64,
    pub slow_clients: AtomicU64,
    pub drain_notices: AtomicU64,
    pub auth_errors: AtomicU64,
    pub closes: Closes,
    // resume 처리를 시작한 때부터 caught-up을 보낼 때까지. worker 대기와 snapshot 또는 replay 전송을 포함한다.
    pub catch_up: Recovery,
    // replay를 시작한 때부터 live 구독을 붙인 시점의 끝 위치에 닿을 때까지. caught-up을 보낸 resume만 센다.
    pub replay: Recovery,
}
pub(crate) struct Connection(Arc<Counters>);
impl Drop for Connection {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::Relaxed);
    }
}
impl Counters {
    pub fn connection(self: &Arc<Self>) -> Connection {
        self.connections.fetch_add(1, Ordering::Relaxed);
        self.active.fetch_add(1, Ordering::Relaxed);
        Connection(self.clone())
    }
    pub fn closed(&self, reason: &str) {
        self.closes.0.add(reason);
        let counter = match reason {
            "replay-limit" => Some(&self.replay_limits),
            "invalid-cursor" | "cursor-expired" => Some(&self.cursor_rejections),
            "slow-client" => Some(&self.slow_clients),
            "draining" => Some(&self.drain_notices),
            reason if reason.starts_with("auth-") || reason == "forbidden-scope" => Some(&self.auth_errors),
            _ => None,
        };
        if let Some(counter) = counter {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub struct Management {
    pub lifecycle: Lifecycle,
    pub protocol: Option<Protocol>,
    pub auth: Option<SharedAuth>,
}
impl Management {
    pub(crate) async fn prometheus(&self, hub: &Hub) -> String {
        let view = hub.replica_view().await.ok();
        let mut output = String::new();
        let mut metric = |name: &str, value: f64| {
            let kind = if name.ends_with("_total") { "counter" } else { "gauge" };
            let _ = writeln!(output, "# TYPE wts_ha_{name} {kind}\nwts_ha_{name} {value}");
        };
        metric("gateway_ready", u8::from(self.lifecycle.ready()) as f64);
        metric("policy_fresh", u8::from(self.auth.as_ref().is_none_or(SharedAuth::fresh)) as f64);
        metric("policy_age_seconds", self.auth.as_ref().and_then(SharedAuth::age_seconds).unwrap_or(0.0));
        metric(
            "log_position_lag",
            view.as_ref().and_then(|v| v.log_end.map(|end| end.saturating_sub(v.next) as f64)).unwrap_or(f64::NAN),
        );
        metric("source_age_seconds", view.as_ref().map_or(f64::NAN, |v| v.source_age_seconds()));
        metric("broker_age_seconds", view.as_ref().and_then(|v| v.broker_age_seconds()).unwrap_or(f64::NAN));
        metric("drain_elapsed_seconds", self.lifecycle.drain_started().map_or(0.0, |at| at.elapsed().as_secs_f64()));
        if let Some(protocol) = &self.protocol {
            let counters = &protocol.counters;
            for (name, counter) in [
                ("connections_total", &counters.connections),
                ("active_connections", &counters.active),
                ("resumes_total", &counters.resumes),
                ("caught_up_total", &counters.caught_up),
                ("replay_bytes_total", &counters.replay_bytes),
                ("snapshot_bytes_total", &counters.snapshot_bytes),
                ("checkpoints_total", &counters.checkpoints),
                ("replay_limits_total", &counters.replay_limits),
                ("cursor_rejections_total", &counters.cursor_rejections),
                ("slow_clients_total", &counters.slow_clients),
                ("drain_notices_total", &counters.drain_notices),
                ("auth_errors_total", &counters.auth_errors),
            ] {
                metric(name, counter.load(Ordering::Relaxed) as f64);
            }
            metric("replay_workers", (32 - protocol.workers.available_permits()) as f64);
            metric("replay_waiters", (128 - protocol.waiting.available_permits()) as f64);
        }
        let _ = writeln!(output, "wts_ha_gateway_phase{{phase=\"{:?}\"}} 1", self.lifecycle.phase());
        if let Some(protocol) = &self.protocol {
            protocol.counters.closes.0.write(&mut output, "wts_ha_closed_total");
            protocol.counters.catch_up.0.write(&mut output, "wts_ha_catch_up_seconds");
            protocol.counters.replay.0.write(&mut output, "wts_ha_replay_seconds");
        }
        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closes_keep_existing_counters_and_fold_unknown_reasons() {
        let counters = Counters::default();
        for reason in ["slow-client", "unexpected"] {
            counters.closed(reason);
        }
        let mut output = String::new();
        counters.closes.0.write(&mut output, "wts_ha_closed_total");
        assert!(output.contains("wts_ha_closed_total{reason=\"slow-client\"} 1\n"));
        assert!(output.contains("wts_ha_closed_total{reason=\"other\"} 1\n"));
        assert_eq!(counters.slow_clients.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn recovery_histograms_use_the_ten_second_bounds() {
        let counters = Counters::default();
        counters.catch_up.0.observe(std::time::Duration::from_millis(1200));
        let mut output = String::new();
        counters.catch_up.0.write(&mut output, "wts_ha_catch_up_seconds");
        assert!(output.contains("wts_ha_catch_up_seconds_bucket{le=\"1\"} 0\n"));
        assert!(output.contains("wts_ha_catch_up_seconds_bucket{le=\"2.5\"} 1\n"));
        assert!(output.contains("wts_ha_catch_up_seconds_bucket{le=\"10\"} 1\n"));
        assert!(output.contains("wts_ha_catch_up_seconds_sum 1.2\nwts_ha_catch_up_seconds_count 1\n"));
    }
}
