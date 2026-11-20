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
    let (mut writer, mut reader) = socket.split();
    let handshake = async {
        let Request::Connect { token } = request(&mut reader).await? else {
            return Err("auth-invalid");
        };
        let session = config.auth.as_ref().ok_or("auth-required")?.authorize(Some(&token))?;
        let subject = session.principal.clone().unwrap_or_else(|| super::digest(token.as_bytes()));
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
                reason = monitor(hub) => reason,
                reason = read_heartbeats(&mut reader) => reason,
                result = transfer::run(&mut writer, hub, input, protocol, conn, &subject, resume) => result.err().unwrap_or("connection-closed"),
            }
        }
        Err(reason) => reason,
    };
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

async fn monitor(hub: &Hub) -> &'static str {
    loop {
        tokio::time::sleep(Duration::from_millis(250)).await;
        if !hub.replica_ready() {
            return "source-stale";
        }
    }
}
