use super::*;
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
struct Buffer(Arc<Mutex<Vec<u8>>>);
impl io::Write for Buffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn json_filter_excludes_external_targets_even_at_trace_level() {
    let bytes = Buffer::default();
    let (writer, worker) = NonBlockingBuilder::default().finish(bytes.clone());
    let subscriber = subscriber(writer, "json", "trace").unwrap();
    tracing::dispatcher::with_default(&subscriber, || {
        tracing::info!(target: "ws_trade_server::test", event = "safe_event", conn = 7);
        tracing::trace!(target: "tungstenite", body = "sensitive-test-marker");
    });
    drop(worker);
    let output = String::from_utf8(bytes.0.lock().unwrap().clone()).unwrap();
    let rows: Vec<serde_json::Value> = output.lines().map(|line| serde_json::from_str(line).unwrap()).collect();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["fields"]["event"], "safe_event");
    assert_eq!(rows[0]["fields"]["conn"], 7);
    assert!(!output.contains("sensitive-test-marker"));
}

#[test]
fn text_filter_keeps_warning_and_suppresses_debug() {
    let bytes = Buffer::default();
    let (writer, worker) = NonBlockingBuilder::default().finish(bytes.clone());
    let subscriber = subscriber(writer, "text", "warn").unwrap();
    tracing::dispatcher::with_default(&subscriber, || {
        tracing::debug!(target: "ws_trade_server::test", event = "hidden_debug");
        tracing::warn!(target: "ws_trade_server::test", event = "visible_warning");
    });
    drop(worker);
    let output = String::from_utf8(bytes.0.lock().unwrap().clone()).unwrap();
    assert!(output.contains("visible_warning"));
    assert!(!output.contains("hidden_debug"));
}

#[tokio::test]
async fn slow_output_drops_logs_without_blocking_the_producer() {
    struct Slow(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>);
    impl io::Write for Slow {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let _ = self.0.send(());
            let _ = self.1.recv();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let (entered, started) = std::sync::mpsc::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let (writer, worker) =
        NonBlockingBuilder::default().buffered_lines_limit(2).lossy(true).finish(Slow(entered, wait));
    let dropped = writer.error_counter();
    let subscriber = subscriber(writer, "json", "info").unwrap();
    tracing::dispatcher::with_default(&subscriber, || {
        tracing::info!(target: "ws_trade_server::test", event = "first");
        started.recv_timeout(std::time::Duration::from_secs(2)).unwrap();
        for _ in 0..20 {
            tracing::info!(target: "ws_trade_server::test", event = "overflow");
        }
    });
    assert!(dropped.dropped_lines() > 0);
    let hub = Hub::spawn();
    Reporter::start(hub.clone(), dropped.clone()).finish().await;
    assert_eq!(hub.metrics().await.unwrap().logs_dropped_total, dropped.dropped_lines() as u64);
    drop(release);
    drop(worker);
}

#[tokio::test]
async fn summaries_report_only_new_failures() {
    let bytes = Buffer::default();
    let (writer, worker) = NonBlockingBuilder::default().finish(bytes.clone());
    let subscriber = subscriber(writer, "json", "info").unwrap();
    let hub = Hub::spawn();
    let before = hub.metrics().await.unwrap();
    hub.bridge.decode_failures.store(3, std::sync::atomic::Ordering::Relaxed);
    hub.bridge.ring_overruns.store(2, std::sync::atomic::Ordering::Relaxed);
    let after = hub.metrics().await.unwrap();
    tracing::dispatcher::with_default(&subscriber, || {
        reporter::summary(after, Some(before));
        reporter::summary(after, Some(after));
    });
    drop(worker);
    let output = String::from_utf8(bytes.0.lock().unwrap().clone()).unwrap();
    let rows: Vec<serde_json::Value> = output.lines().map(|line| serde_json::from_str(line).unwrap()).collect();
    let warnings: Vec<_> = rows.iter().filter(|row| row["fields"]["event"] == "server_anomalies").collect();
    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0]["fields"]["decode_failures"], 3);
    assert_eq!(warnings[0]["fields"]["ring_overruns"], 2);
}
