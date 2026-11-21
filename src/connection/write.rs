use super::batch;
use crate::{auth, hub::Outgoing, stomp};
use futures_util::SinkExt;
use std::future::Future;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, MissedTickBehavior, interval_at};
use tokio_tungstenite::tungstenite::Message;

pub(super) async fn run<W>(
    mut writer: W,
    mut outgoing: mpsc::Receiver<Outgoing>,
    connection_id: &str,
    heartbeat_ms: u64,
    write_timeout: Option<Duration>,
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
                match bounded(write_timeout, writer.send(Message::text("\n"))).await {
                    Some(Ok(())) => continue,
                    Some(Err(_)) => return "write_error",
                    None => return "write_timeout",
                }
            }
        };
        match bounded(write_timeout, batch::write(&mut writer, first, &mut outgoing, connection_id, &mut seq)).await {
            Some(Ok(())) => {}
            Some(Err(reason)) => return reason,
            None => return "write_timeout",
        }
    }
}

// 송신 큐가 차지 않는 저빈도 연결도 소켓이 막히면 끊기도록 쓰기 한 번에 기한을 건다. 기한이 없으면 끝까지 기다린다.
async fn bounded<T>(limit: Option<Duration>, write: impl Future<Output = T>) -> Option<T> {
    match limit {
        Some(limit) => tokio::time::timeout(limit, write).await.ok(),
        None => Some(write.await),
    }
}
