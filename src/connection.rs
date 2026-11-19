use crate::hub::{Hub, Outgoing};
use crate::stomp::{self, Incoming};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
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

// 업그레이드와 첫 CONNECT를 이 시간 안에 끝내지 못하면 끊는다. 아무것도 보내지 않는 소켓이 쌓이는 것을 막는다.
const HANDSHAKE_DEADLINE: Duration = Duration::from_secs(10);
// 실제 클라이언트 프레임은 수백 바이트다. 기본값 64 MiB는 연결마다 버퍼를 그만큼 허용한다.
const MAX_MESSAGE_BYTES: usize = 64 << 10;
// 기본값 128 KiB는 연결마다 읽기 버퍼를 미리 잡는다. 클라이언트 프레임은 수백 바이트라 4 KiB면 충분하고, 더 큰 메시지는 버퍼가 알아서 늘어난다.
const READ_BUFFER_BYTES: usize = 4 << 10;

pub async fn serve(stream: TcpStream, hub: Hub, heartbeat_ms: u64, queue_capacity: usize, conn: u64) {
    let websocket_config =
        WebSocketConfig::default().max_message_size(Some(MAX_MESSAGE_BYTES)).read_buffer_size(READ_BUFFER_BYTES);
    let handshake = async {
        let socket = accept_hdr_async_with_config(stream, negotiate, Some(websocket_config)).await.ok()?;
        let (writer, mut reader) = socket.split();
        let connection_id = read_connect(&mut reader).await?;
        Some((writer, reader, connection_id))
    };
    let Ok(Some((writer, reader, connection_id))) = timeout(HANDSHAKE_DEADLINE, handshake).await else {
        return;
    };
    eprintln!("연결: {connection_id}");

    let (outgoing, kicked) = hub.connect(conn, queue_capacity).await;
    tokio::select! {
        _ = read_loop(reader, &hub, conn) => {}
        _ = write_loop(writer, outgoing, &connection_id, heartbeat_ms) => {}
        _ = kicked => {}
    }
    hub.disconnect(conn).await;
    eprintln!("종료: {connection_id}");
}

async fn read_connect(reader: &mut Reader) -> Option<String> {
    let Message::Text(text) = reader.next().await?.ok()? else {
        return None;
    };
    let Incoming::Frame(frame) = stomp::parse(&text)? else {
        return None;
    };
    if frame.command != "CONNECT" {
        return None;
    }
    frame.header("connection-id").map(str::to_string)
}

async fn read_loop(mut reader: Reader, hub: &Hub, conn: u64) {
    while let Some(Ok(message)) = reader.next().await {
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
            outgoing = outgoing.recv() => match outgoing {
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
