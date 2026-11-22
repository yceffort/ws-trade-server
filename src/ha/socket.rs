mod replay;
mod transfer;
use super::{
    Protocol,
    protocol::{Request, Resume},
};
use crate::{Config, hub::Hub};
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::{
    net::TcpStream,
    sync::watch,
    time::{Instant, timeout},
};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Message,
        handshake::server::{ErrorResponse, Request as Upgrade, Response},
        http::HeaderValue,
    },
};

type Socket = WebSocketStream<TcpStream>;
type Writer = SplitSink<Socket, Message>;
type Reader = SplitStream<Socket>;
const FRAME: usize = 48 * 1024;
// broker 한 대를 예고 없이 잃으면 controller가 fence할 때까지 commit이 멈춘다. 로컬 측정에서 8~17초였다(#94).
// 그동안 기존 연결은 source-stale 알림만 받고 유지하며, 이 시간을 넘겨도 멈춰 있으면 닫는다.
const STALE_GRACE: Duration = Duration::from_secs(30);

#[allow(clippy::result_large_err)]
pub(crate) fn negotiate(request: &Upgrade, mut response: Response) -> Result<Response, ErrorResponse> {
    if request.uri().query().is_some()
        || !request
            .headers()
            .get("sec-websocket-protocol")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.split(',').any(|p| p.trim() == "wts.ha.v1"))
    {
        return Err(tokio_tungstenite::tungstenite::http::Response::builder()
            .status(400)
            .body(None)
            .expect("유효한 상태 코드"));
    }
    response.headers_mut().insert("sec-websocket-protocol", HeaderValue::from_static("wts.ha.v1"));
    Ok(response)
}

async fn request(reader: &mut Reader) -> Result<Request, &'static str> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match tokio::time::timeout_at(deadline, reader.next()).await {
            Ok(Some(Ok(Message::Text(text)))) if text.len() <= FRAME => {
                return serde_json::from_str(&text).map_err(|_| "invalid-request");
            }
            Ok(Some(Ok(Message::Ping(_) | Message::Pong(_)))) => {}
            _ => return Err("invalid-request"),
        }
    }
}

pub(super) async fn send(writer: &mut Writer, value: Value, deadline: Instant) -> Result<usize, &'static str> {
    let text = serde_json::to_string(&value).map_err(|_| "invalid-response")?;
    if text.len() > FRAME {
        return Err("response-too-large");
    }
    let bytes = text.len();
    tokio::time::timeout_at(deadline.min(Instant::now() + Duration::from_secs(1)), writer.send(Message::text(text)))
        .await
        .map_err(|_| "slow-client")?
        .map_err(|_| "connection-closed")?;
    Ok(bytes)
}

pub(crate) async fn serve_socket(
    socket: Socket,
    hub: &Hub,
    config: &Config,
    conn: u64,
    mut shutdown: watch::Receiver<bool>,
) {
    let _connection = config.ha_protocol.as_ref().map(|protocol| protocol.counters.connection());
    let (mut writer, mut reader) = socket.split();
    let handshake = async {
        let Request::Connect { token } = request(&mut reader).await? else {
            return Err("auth-invalid");
        };
        let (session, subject) = if let Some(auth) = &config.ha_auth {
            let session = auth.authorize(&token)?;
            let subject = session.subject();
            (Authentication::Shared(session), subject)
        } else {
            let session = config.auth.as_ref().ok_or("auth-required")?.authorize(Some(&token))?;
            let subject = session.principal.clone().unwrap_or_else(|| super::digest(token.as_bytes()));
            (Authentication::Local(session), subject)
        };
        let protocol = config.ha_protocol.as_ref().ok_or("ha-disabled")?;
        send(
            &mut writer,
            json!({"type":"connected","protocol":"wts.ha.v1","heartbeat_ms":1000,"receive_deadline_ms":3000}),
            Instant::now() + Duration::from_secs(1),
        )
        .await?;
        let input = config.ha.as_ref().ok_or("ha-disabled")?;
        let resume =
            Resume::parse(request(&mut reader).await?, protocol, &input.source.log_id, &input.source.topics, &subject)?;
        if !session.allows(&resume.topics) {
            return Err("forbidden-scope");
        }
        Ok::<_, &'static str>((session, subject, resume))
    };
    let result = tokio::select! {
        result = handshake => result,
        _ = shutdown.changed() => Err("server-shutdown"),
    };
    let reason = match result {
        Ok((mut session, subject, resume)) => {
            let protocol: &Protocol = config.ha_protocol.as_ref().expect("handshake에서 확인한 설정");
            let input = config.ha.as_ref().expect("handshake에서 확인한 설정");
            tokio::select! {
                biased;
                reason = session.invalidated() => reason,
                _ = shutdown.changed() => "server-shutdown",
                reason = async {
                    match &config.ha_lifecycle {
                        Some(life) => life.close_reason(conn).await,
                        None => std::future::pending().await,
                    }
                } => reason,
                reason = monitor(|| hub.replica_health(true), STALE_GRACE) => reason,
                reason = read_heartbeats(&mut reader) => reason,
                result = transfer::run(&mut writer, hub, input, protocol, conn, &subject, resume) => result.err().unwrap_or("connection-closed"),
            }
        }
        Err(reason) => reason,
    };
    if let Some(protocol) = &config.ha_protocol {
        protocol.counters.closed(reason);
    }
    hub.unfollow_replica(conn);
    let frame = if reason == "draining" {
        json!({"type":"draining","retry_after_ms":0})
    } else {
        json!({"type":"error","code":reason})
    };
    let _ = send(&mut writer, frame, Instant::now() + Duration::from_secs(1)).await;

    let _ = timeout(Duration::from_secs(1), writer.close()).await;
    tracing::debug!(event = "ha_connection_closed", conn, reason);
}

async fn read_heartbeats(reader: &mut Reader) -> &'static str {
    let mut deadline = Instant::now() + Duration::from_secs(3);
    loop {
        match tokio::time::timeout_at(deadline, reader.next()).await {
            Ok(Some(Ok(Message::Text(text)))) if text.len() <= FRAME => {
                if !matches!(serde_json::from_str::<Request>(&text), Ok(Request::Heartbeat)) {
                    return "invalid-request";
                }
                deadline = Instant::now() + Duration::from_secs(3);
            }
            Ok(Some(Ok(Message::Ping(_) | Message::Pong(_)))) => {}
            Ok(_) => return "connection-closed",
            Err(_) => return "heartbeat-timeout",
        }
    }
}

async fn monitor(health: impl Fn() -> (bool, bool), grace: Duration) -> &'static str {
    let mut stale = None;
    loop {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let (ready, failed) = health();
        if failed {
            return "source-stale";
        }
        if ready {
            stale = None;
        } else if stale.get_or_insert_with(Instant::now).elapsed() >= grace {
            return "source-stale";
        }
    }
}

enum Authentication {
    Local(crate::auth::Session),
    Shared(super::auth::Session),
}
impl Authentication {
    fn allows(&self, topics: &[String]) -> bool {
        match self {
            Self::Local(_) => true,
            Self::Shared(session) => session.allows(topics),
        }
    }
    async fn invalidated(&mut self) -> &'static str {
        match self {
            Self::Local(session) => session.invalidated().await,
            Self::Shared(session) => session.invalidated().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[tokio::test]
    async fn a_stalled_source_keeps_the_connection_until_the_grace_ends() {
        let grace = Duration::from_millis(1000);
        let health = Cell::new((false, false));
        let started = Instant::now();
        let driver = async {
            tokio::time::sleep(Duration::from_millis(900)).await;
            health.set((true, false));
            tokio::time::sleep(Duration::from_millis(400)).await;
            health.set((false, false));
            std::future::pending::<()>().await
        };
        let reason = tokio::select! {
            reason = monitor(|| health.get(), grace) => reason,
            _ = driver => unreachable!(),
        };
        assert_eq!(reason, "source-stale");
        // 유예 직전에 회복하면 다음 멈춤의 유예가 새로 시작된다.
        let elapsed = started.elapsed();
        assert!(elapsed >= Duration::from_millis(2300) && elapsed < Duration::from_millis(3500), "{elapsed:?}");
    }

    #[tokio::test]
    async fn a_failed_replica_closes_without_waiting_for_the_grace() {
        let started = Instant::now();
        assert_eq!(monitor(|| (false, true), STALE_GRACE).await, "source-stale");
        assert!(started.elapsed() < Duration::from_secs(1));
    }
}
