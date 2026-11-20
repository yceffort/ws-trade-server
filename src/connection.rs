mod idle;
mod rejection;
use crate::hub::{Hub, Outgoing};
use crate::stomp::{self, Incoming};
use crate::{Config, auth};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
pub(crate) use rejection::Rejector;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, MissedTickBehavior, interval_at, timeout};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::{self, HeaderValue};
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{WebSocketStream, accept_hdr_async_with_config};

type Reader = SplitStream<WebSocketStream<TcpStream>>;
type Writer = SplitSink<WebSocketStream<TcpStream>, Message>;

fn reject(status: u16) -> ErrorResponse {
    http::Response::builder().status(status).body(None).expect("상태 코드는 유효하다")
}

// 콜백 시그니처가 tungstenite의 Callback 트레이트로 정해져 있어 Err 크기를 줄일 수 없다.
#[allow(clippy::result_large_err)]
fn negotiate(request: &Request, mut response: Response) -> Result<Response, ErrorResponse> {
    if request.uri().path() != "/ws" {
        return Err(reject(404));
    }
    let offered = request.headers().get("sec-websocket-protocol").and_then(|value| value.to_str().ok());
    if !offered.is_some_and(|list| list.split(',').any(|protocol| protocol.trim() == "v12.stomp")) {
        return Err(reject(400));
    }
    response.headers_mut().insert("sec-websocket-protocol", HeaderValue::from_static("v12.stomp"));
    Ok(response)
}

// tungstenite의 콜백이 정한 오류 타입을 그대로 반환한다.
#[allow(clippy::result_large_err)]
pub async fn reject_overloaded(stream: TcpStream) {
    // 거절용 태스크는 Rejector가 제한한다. 소켓 하나의 대기도 짧게 끝낸다.
    let callback = |_: &Request, _: Response| Err(reject(503));
    let _ = timeout(Duration::from_millis(250), accept_hdr_async_with_config(stream, callback, None)).await;
}

// 업그레이드와 첫 CONNECT를 이 시간 안에 끝내지 못하면 끊는다. 아무것도 보내지 않는 소켓이 쌓이는 것을 막는다.
const HANDSHAKE_DEADLINE: Duration = Duration::from_secs(10);
// 실제 클라이언트 프레임은 수백 바이트다. 기본값 64 MiB는 연결마다 버퍼를 그만큼 허용한다.
const MAX_MESSAGE_BYTES: usize = 64 << 10;
// 기본값 128 KiB는 연결마다 읽기 버퍼를 미리 잡는다. 클라이언트 프레임은 수백 바이트라 4 KiB면 충분하고, 더 큰 메시지는 버퍼가 알아서 늘어난다.
const READ_BUFFER_BYTES: usize = 4 << 10;

pub async fn serve(stream: TcpStream, hub: Hub, config: Config, conn: u64, mut shutdown: watch::Receiver<bool>) {
    let websocket_config =
        WebSocketConfig::default().max_message_size(Some(MAX_MESSAGE_BYTES)).read_buffer_size(READ_BUFFER_BYTES);
    let handshake = async {
        let socket = accept_hdr_async_with_config(stream, negotiate, Some(websocket_config)).await.ok()?;
        let (mut writer, mut reader) = socket.split();
        let frame = read_connect(&mut reader).await?;
        let connection_id = frame.header("connection-id")?.to_string();
        let session = match &config.auth {
            Some(auth) => match auth.authorize(frame.header("authorization")) {
                Ok(session) => Some(session),
                Err(code) => {
                    let _ = writer.send(Message::text(stomp::error(code))).await;
                    let _ = writer.close().await;
                    return None;
                }
            },
            None => None,
        };
        Some((writer, reader, connection_id, session))
    };
    let result = tokio::select! {
        result = timeout(HANDSHAKE_DEADLINE, handshake) => result,
        _ = shutdown.changed() => return,
    };
    let Ok(Some((writer, reader, connection_id, session))) = result else {
        return;
    };
    eprintln!("연결: {connection_id}");

    let mut auth_guard = session.clone();
    let (outgoing, kicked) = hub.connect(conn, config.queue_capacity).await;
    let (activity, last_received) = watch::channel(Instant::now());
    let activity = config.idle_timeout.map(|_| activity);
    tokio::select! {
        // 오류 통지를 보낼 수 없는 느린 소켓도 인증 폐기 후 1초 안에 닫는다.
        _ = async { auth::invalidated(&mut auth_guard).await; tokio::time::sleep(Duration::from_secs(1)).await; } => {}
        _ = idle::wait(last_received, config.idle_timeout) => {}
        _ = read_loop(reader, &hub, conn, activity) => {}
        _ = write_loop(writer, outgoing, &connection_id, config.heartbeat_ms, shutdown, session) => {}
        _ = kicked => {}
    }
    hub.disconnect(conn).await;
    eprintln!("종료: {connection_id}");
}

async fn read_connect(reader: &mut Reader) -> Option<stomp::Frame> {
    let Message::Text(text) = reader.next().await?.ok()? else {
        return None;
    };
    let Incoming::Frame(frame) = stomp::parse(&text)? else {
        return None;
    };
    if frame.command != "CONNECT" {
        return None;
    }
    Some(frame)
}

async fn read_loop(mut reader: Reader, hub: &Hub, conn: u64, activity: Option<watch::Sender<Instant>>) {
    loop {
        let Some(Ok(message)) = reader.next().await else { return };
        // 허브 전송을 기다리는 동안에도 별도 감시자가 이 수신 시각으로 마감을 판단한다.
        if let Some(activity) = &activity {
            activity.send_replace(Instant::now());
        }
        let text = match message {
            Message::Text(text) => text,
            Message::Ping(_) | Message::Pong(_) => continue,
            _ => return,
        };
        let Some(incoming) = stomp::parse(&text) else {
            return;
        };
        let Incoming::Frame(frame) = incoming else {
            continue;
        };
        match frame.command.as_str() {
            "SUBSCRIBE" => {
                let (Some(id), Some(destination)) = (frame.header("id"), frame.header("destination")) else {
                    return;
                };
                let receipt = frame.header("receipt").map(str::to_string);
                hub.subscribe(conn, id.to_string(), destination.to_string(), receipt).await;
            }
            "UNSUBSCRIBE" => {
                let Some(id) = frame.header("id") else { return };
                let receipt = frame.header("receipt").map(str::to_string);
                hub.unsubscribe(conn, id.to_string(), receipt).await;
            }
            // DISCONNECT, 두 번째 CONNECT, 모르는 명령은 모두 연결을 닫는다.
            _ => return,
        }
    }
}

async fn write_loop(
    mut writer: Writer,
    mut outgoing: mpsc::Receiver<Outgoing>,
    connection_id: &str,
    heartbeat_ms: u64,
    mut shutdown: watch::Receiver<bool>,
    mut session: Option<auth::Session>,
) {
    let heart_beat = format!("{heartbeat_ms},{heartbeat_ms}");
    if writer.send(Message::text(stomp::connected(&heart_beat))).await.is_err() {
        return;
    }
    let period = Duration::from_millis(heartbeat_ms);
    let mut beat = interval_at(Instant::now() + period, period);
    beat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // message-id의 seq는 이 연결로 실제 나가는 순서대로 여기서만 매긴다.
    let mut seq: u64 = 0;
    loop {
        let text = tokio::select! {
            reason = auth::invalidated(&mut session) => {
                let _ = writer.send(Message::text(stomp::error(reason))).await;
                let _ = writer.close().await;
                return;
            }
            _ = shutdown.changed() => {
                let _ = writer.send(Message::text(stomp::error("server-shutdown"))).await;
                let _ = writer.close().await;
                return;
            }
            outgoing = outgoing.recv() => match outgoing {
                Some(Outgoing::Error(code)) => {
                    let _ = writer.send(Message::text(stomp::error(code))).await;
                    return;
                }
                Some(Outgoing::Receipt { receipt_id, message }) => stomp::receipt(&receipt_id, message),
                Some(Outgoing::Message { subscription, body }) => {
                    seq += 1;
                    stomp::message(&format!("{connection_id}-{seq}"), &subscription, &body)
                }
                None => return,
            },
            _ = beat.tick() => "\n".to_string(),
        };
        if writer.send(Message::text(text)).await.is_err() {
            return;
        }
    }
}
