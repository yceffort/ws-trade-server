use super::{idle, read, write};
use crate::{Config, auth, hub::Hub};
use futures_util::{Sink, Stream};
use std::time::Duration;
use tokio::sync::{oneshot, watch};
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::{Error, Message};

pub(super) async fn run<R, W>(
    (reader, writer): (R, W),
    hub: &Hub,
    config: &Config,
    conn: u64,
    connection_id: &str,
    session: Option<auth::Session>,
    shutdown: watch::Receiver<bool>,
) -> &'static str
where
    R: Stream<Item = Result<Message, Error>> + Unpin,
    W: Sink<Message, Error = Error> + Unpin,
{
    let mut auth_guard = session.clone();
    let (outgoing, kicked) = hub.connect(conn, config.queue_capacity).await;
    let (activity, last_received) = watch::channel(Instant::now());
    let activity = config.idle_timeout.map(|_| activity);
    let reason = tokio::select! {
        // 오류 통지를 보낼 수 없는 느린 소켓도 인증 폐기 후 1초 안에 닫는다.
        _ = async { auth::invalidated(&mut auth_guard).await; tokio::time::sleep(Duration::from_secs(1)).await; } => "auth_invalidated",
        _ = idle::wait(last_received, config.idle_timeout) => "idle_timeout",
        reason = read::run(reader, hub, conn, activity) => reason,
        reason = write::run(writer, outgoing, connection_id, config.heartbeat_ms, shutdown, session) => reason,
        reason = rejected(kicked) => reason,
    };
    hub.disconnect(conn).await;
    tracing::debug!(event = "connection_closed", conn, reason);
    reason
}

async fn rejected(kicked: oneshot::Receiver<&'static str>) -> &'static str {
    match kicked.await {
        Ok(reason) => {
            // 정상 수신자는 ERROR를 읽을 여유를 주고, 막힌 쓰기는 이 기한에 취소한다.
            tokio::time::sleep(Duration::from_secs(1)).await;
            reason
        }
        Err(_) => "hub_closed",
    }
}

#[cfg(test)]
mod tests;
