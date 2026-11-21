mod batch;
mod idle;
mod read;
mod rejection;
mod session;
mod write;
use crate::Config;
use crate::hub::Hub;
use crate::stomp::{self, Incoming};
use futures_util::stream::SplitStream;
use futures_util::{SinkExt, StreamExt};
pub(crate) use rejection::Rejector;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::{self, HeaderValue};
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{WebSocketStream, accept_hdr_async_with_config};

type Reader = SplitStream<WebSocketStream<TcpStream>>;
type Writer = futures_util::stream::SplitSink<WebSocketStream<TcpStream>, Message>;

enum Handshake {
    Legacy(Writer, Reader, String, Option<crate::auth::Session>),
    #[cfg(feature = "ha")]
    Ha(WebSocketStream<TcpStream>),
}

fn reject(status: u16) -> ErrorResponse {
    http::Response::builder().status(status).body(None).expect("상태 코드는 유효하다")
}

fn unavailable() -> ErrorResponse {
    let mut response = reject(503);
    response.headers_mut().insert("retry-after", HeaderValue::from_static("1"));
    response
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
    let callback = |_: &Request, _: Response| Err(unavailable());
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
        #[cfg(feature = "ha")]
        let mut route_ha = false;
        // 수락할 때는 준비 상태였다가 upgrade 전에 drain이 시작된 연결이다. 수락 시점의 거절과 같은 사유로 센다.
        #[cfg(feature = "ha")]
        let mut not_ready = false;
        #[allow(clippy::result_large_err)]
        let callback = |request: &Request, response: Response| {
            #[cfg(feature = "ha")]
            if config.ha_lifecycle.as_ref().is_some_and(|life| !life.ready()) {
                not_ready = true;
                return Err(unavailable());
            }
            #[cfg(feature = "ha")]
            if config.ha_auth.is_some() && request.uri().path() != "/experimental/ha/ws" {
                return Err(reject(404));
            }
            #[cfg(feature = "ha")]
            if config.ha_protocol.is_some() && request.uri().path() == "/experimental/ha/ws" {
                route_ha = true;
                return crate::ha::negotiate(request, response);
            }
            negotiate(request, response)
        };
        let socket = accept_hdr_async_with_config(stream, callback, Some(websocket_config)).await;
        #[cfg(feature = "ha")]
        if not_ready {
            return Err("not_ready");
        }
        let socket = socket.map_err(|_| "handshake_failed")?;
        #[cfg(feature = "ha")]
        if route_ha {
            return Ok(Handshake::Ha(socket));
        }
        let (mut writer, mut reader) = socket.split();
        let frame = read_connect(&mut reader).await.ok_or("handshake_failed")?;
        let connection_id = frame.header("connection-id").ok_or("handshake_failed")?.to_string();
        let session = match &config.auth {
            Some(auth) => match auth.authorize(frame.header("authorization")) {
                Ok(session) => Some(session),
                Err(code) => {
                    tracing::debug!(event = "authentication_rejected", conn, reason = code);
                    let _ = writer.send(Message::text(stomp::error(code))).await;
                    let _ = writer.close().await;
                    return Err(code);
                }
            },
            None => None,
        };
        Ok(Handshake::Legacy(writer, reader, connection_id, session))
    };
    let result = tokio::select! {
        result = timeout(HANDSHAKE_DEADLINE, handshake) => result.unwrap_or(Err("handshake_timeout")),
        _ = shutdown.changed() => return,
    };
    let handshake = match result {
        Ok(handshake) => handshake,
        Err(reason) => {
            hub.counters.rejected.add(reason);
            tracing::debug!(event = "handshake_failed", conn);
            return;
        }
    };
    let (writer, reader, connection_id, session) = match handshake {
        Handshake::Legacy(writer, reader, id, session) => (writer, reader, id, session),
        #[cfg(feature = "ha")]
        Handshake::Ha(socket) => {
            crate::ha::serve_socket(socket, &hub, &config, conn, shutdown).await;
            return;
        }
    };
    tracing::debug!(event = "connection_opened", conn);

    session::run((reader, writer), &hub, &config, conn, &connection_id, session, shutdown).await;
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

#[cfg(all(test, feature = "ha"))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn upgrade_refused_after_drain_starts_counts_as_not_ready() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/ws", listener.local_addr().unwrap());
        let client = tokio::spawn(tokio_tungstenite::connect_async(url));
        // 수락은 이미 끝났고, 준비되지 않은 상태에서 upgrade를 받는다.
        let (stream, _) = listener.accept().await.unwrap();
        let hub = Hub::spawn();
        let config = Config { ha_lifecycle: Some(crate::ha::Lifecycle::default()), ..Config::default() };
        let (_shutdown, receiver) = watch::channel(false);
        serve(stream, hub.clone(), config, 1, receiver).await;
        assert!(client.await.unwrap().is_err());
        let text = crate::metrics::prometheus(&hub.metrics().await.unwrap(), &hub.counters);
        assert!(text.contains("wts_connections_rejected_total{reason=\"not_ready\"} 1\n"));
        assert!(text.contains("wts_connections_rejected_total{reason=\"handshake_failed\"} 0\n"));
    }
}
