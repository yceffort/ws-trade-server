use super::{idle, read, write};
use crate::{
    Config, auth,
    hub::{Hub, SLOW_CONSUMER},
};
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
    let principal = session.as_ref().and_then(|session| session.principal.clone());
    let (outgoing, mut kicked) = hub.connect_as(conn, config.queue_capacity, principal).await;
    let (activity, last_received) = watch::channel(Instant::now());
    let activity = config.idle_timeout.map(|_| activity);
    let mut rejection = None;
    let reason = tokio::select! {
        // 오류 통지를 보낼 수 없는 느린 소켓도 인증 폐기 후 1초 안에 닫는다.
        _ = async { auth::invalidated(&mut auth_guard).await; tokio::time::sleep(Duration::from_secs(1)).await; } => "auth_invalidated",
        _ = idle::wait(last_received, config.idle_timeout) => "idle_timeout",
        reason = read::run(reader, hub, conn, activity) => reason,
        reason = write::run(writer, outgoing, connection_id, config.heartbeat_ms, config.write_timeout, shutdown, session) => reason,
        reason = rejected(&mut kicked, &mut rejection) => reason,
    };
    // 큐가 먼저 닫혀도 이미 받은 거절 사유를 잃지 않는다. 아직 읽지 않은 통지도 확인한다.
    let reason =
        if reason == "hub_closed" { rejection.or_else(|| kicked.try_recv().ok()).unwrap_or(reason) } else { reason };
    hub.disconnect(conn).await;
    hub.counters.closed.add(reason);
    tracing::debug!(event = "connection_closed", conn, reason);
    reason
}

async fn rejected(kicked: &mut oneshot::Receiver<&'static str>, rejection: &mut Option<&'static str>) -> &'static str {
    match kicked.await {
        Ok(reason) => {
            *rejection = Some(reason);
            // 정상 수신자는 ERROR를 읽을 여유를 주고, 막힌 쓰기는 이 기한에 취소한다.
            // 느린 연결 차단은 보낼 ERROR가 없으므로 사유를 넘기기 전처럼 바로 끝낸다.
            if reason != SLOW_CONSUMER {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            reason
        }
        Err(_) => "hub_closed",
    }
}

#[cfg(test)]
mod tests;
