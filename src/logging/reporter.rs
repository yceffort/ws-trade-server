use crate::{hub::Hub, metrics::Metrics};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing_appender::non_blocking::ErrorCounter;

pub struct Reporter {
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl Reporter {
    pub(super) fn start(hub: Hub, dropped: ErrorCounter) -> Self {
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(10));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut previous = None;
            loop {
                let finish = tokio::select! { _ = &mut stopped => true, _ = tick.tick() => false };
                hub.record_log_drops(dropped.dropped_lines() as u64);
                let Some(metrics) = hub.metrics().await else { return };
                summary(metrics, previous);
                previous = Some(metrics);
                if finish {
                    return;
                }
            }
        });
        Self { stop: Some(stop), task: Some(task) }
    }

    pub async fn finish(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(mut task) = self.task.take()
            && tokio::time::timeout(Duration::from_secs(1), &mut task).await.is_err()
        {
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for Reporter {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

pub(super) fn summary(now: Metrics, previous: Option<Metrics>) {
    tracing::info!(
        event = "server_summary",
        connections = now.stomp_connections,
        subscriptions = now.subscriptions,
        queued_frames = now.queued_frames,
        queue_capacity = now.queue_capacity,
        commands_queued = now.commands_queued,
        published_total = now.published_total,
        enqueued_frames_total = now.enqueued_frames_total,
        latest_skipped_total = now.latest_skipped_total,
        logs_dropped_total = now.logs_dropped_total
    );
    let delta = |get: fn(Metrics) -> u64| get(now).saturating_sub(previous.map_or(0, get));
    let ring_overruns = delta(|m| m.ring_overruns_total);
    let sequence_missing = delta(|m| m.sequence_missing_total);
    let decode_failures = delta(|m| m.decode_failures_total);
    let invalid_trades = delta(|m| m.invalid_trades_total);
    let slow_disconnections = delta(|m| m.slow_disconnections_total);
    let log_drops = delta(|m| m.logs_dropped_total);
    if [ring_overruns, sequence_missing, decode_failures, invalid_trades, slow_disconnections, log_drops]
        .iter()
        .any(|n| *n > 0)
    {
        tracing::warn!(
            event = "server_anomalies",
            ring_overruns,
            sequence_missing,
            decode_failures,
            invalid_trades,
            slow_disconnections,
            log_drops
        );
    }
    let sequence_resets = delta(|m| m.sequence_resets_total);
    if sequence_resets > 0 {
        tracing::info!(event = "feed_restarted", count = sequence_resets);
    }
}
