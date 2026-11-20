use super::batch;
use crate::{auth, hub::Outgoing, stomp};
use futures_util::SinkExt;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, MissedTickBehavior, interval_at};
use tokio_tungstenite::tungstenite::Message;

pub(super) async fn run<W>(
    mut writer: W,
    mut outgoing: mpsc::Receiver<Outgoing>,
    connection_id: &str,
    heartbeat_ms: u64,
    mut shutdown: watch::Receiver<bool>,
    mut session: Option<auth::Session>,
) -> &'static str
where
    W: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let heart_beat = format!("{heartbeat_ms},{heartbeat_ms}");
    if writer.send(Message::text(stomp::connected(&heart_beat))).await.is_err() {
        return "write_error";
    }
    let period = Duration::from_millis(heartbeat_ms);
    let mut beat = interval_at(Instant::now() + period, period);
    beat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // message-id의 seq는 이 연결로 실제 나가는 순서대로 여기서만 매긴다.
    let mut seq: u64 = 0;
    loop {
        let first = tokio::select! {
            reason = auth::invalidated(&mut session) => {
                let _ = writer.send(Message::text(stomp::error(reason))).await;
                let _ = writer.close().await;
                return reason;
            }
            _ = shutdown.changed() => {
                let _ = writer.send(Message::text(stomp::error("server-shutdown"))).await;
                let _ = writer.close().await;
                return "server_shutdown";
            }
            first = outgoing.recv() => match first {
                Some(first) => first,
                None => return "hub_closed",
            },
            _ = beat.tick() => {
                if writer.send(Message::text("\n")).await.is_err() {
                    return "write_error";
                }
                continue;
            }
        };
        if let Err(reason) = batch::write(&mut writer, first, &mut outgoing, connection_id, &mut seq).await {
            return reason;
        }
    }
}
