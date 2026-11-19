use super::stats::Sample;
use crate::stomp::serialize;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::time::{Instant, MissedTickBehavior, interval_at, timeout};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
type BoxError = Box<dyn Error + Send + Sync>;

// 서버의 핸드셰이크 제한(10초)과 같다.
const SETUP_TIMEOUT: Duration = Duration::from_secs(10);
const HEARTBEAT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, PartialEq)]
pub enum Role {
    Normal,
    Probe,
    Stalled,
}

pub struct Shared {
    pub url: String,
    pub topics: Vec<String>,
    pub codes: Vec<String>,
    // 0 워밍업, 1 측정, 2 마지막 1초. 받은 MESSAGE 수를 구간별로 나눠 센다.
    pub phase: AtomicUsize,
}

#[derive(Default)]
pub struct Outcome {
    pub messages: [u64; 3],
    pub seq_gap: bool,
    pub disconnected: bool,
    pub kicked: bool,
    pub unparsed: u64,
    pub samples: Vec<Sample>,
}

#[derive(Deserialize)]
struct Key<'a> {
    code: &'a str,
    #[serde(rename = "cumulativeVolume")]
    cumulative_volume: u64,
}

impl Outcome {
    // message-id는 "{connection-id}-{seq}"이고 seq는 연결마다 1부터 빠짐없이 늘어야 한다. 읽지 못한 id도 틈으로 센다.
    fn count(&mut self, shared: &Shared, text: &str) -> bool {
        let Some(rest) = text.strip_prefix("MESSAGE\nmessage-id:") else {
            return false;
        };
        let seq =
            rest.split('\n').next().and_then(|id| id.rsplit_once('-')).and_then(|(_, seq)| seq.parse::<u64>().ok());
        let received: u64 = self.messages.iter().sum();
        if seq != Some(received + 1) {
            self.seq_gap = true;
        }
        self.messages[shared.phase.load(Relaxed)] += 1;
        true
    }
}

pub async fn run(
    shared: &Shared,
    index: usize,
    role: Role,
    ready: tokio::sync::oneshot::Sender<Result<(), String>>,
    mut stop: watch::Receiver<bool>,
) -> Option<(Role, Outcome)> {
    let mut outcome = Outcome::default();
    let setup = timeout(SETUP_TIMEOUT, setup(shared, index, &mut outcome)).await;
    let mut socket = match setup.map_err(|elapsed| elapsed.to_string()).and_then(|r| r.map_err(|e| e.to_string())) {
        Ok(socket) => socket,
        Err(error) => {
            let _ = ready.send(Err(error));
            return None;
        }
    };
    let _ = ready.send(Ok(()));
    if role == Role::Stalled {
        let _ = stop.changed().await;
        outcome.kicked = is_closed(&mut socket).await;
        return Some((role, outcome));
    }

    let mut beat = interval_at(Instant::now() + HEARTBEAT, HEARTBEAT);
    beat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            message = socket.next() => {
                // 이 시각이 "받은 시각"이다. 파싱과 기록은 그 뒤에 한다.
                let received = std::time::Instant::now();
                let Some(Ok(message)) = message else {
                    outcome.disconnected = true;
                    break;
                };
                let Message::Text(text) = message else { continue };
                if outcome.count(shared, text.as_str()) && role == Role::Probe && shared.phase.load(Relaxed) > 0 {
                    record(shared, text.as_str(), received, &mut outcome);
                }
            }
            _ = beat.tick() => {
                if socket.send(Message::text("\n")).await.is_err() {
                    outcome.disconnected = true;
                    break;
                }
            }
            _ = stop.changed() => break,
        }
    }
    Some((role, outcome))
}

fn record(shared: &Shared, text: &str, received: std::time::Instant, outcome: &mut Outcome) {
    let key =
        text.split_once("\n\n").and_then(|(_, body)| serde_json::from_str::<Key>(body.trim_end_matches('\0')).ok());
    match key {
        Some(Key { code, cumulative_volume }) => {
            // 모르는 종목은 쓴 기록과 짝이 안 맞아 통계에서 unmatched로 잡힌다.
            let symbol = shared.codes.iter().position(|known| known == code).unwrap_or(usize::MAX);
            outcome.samples.push(Sample { received, symbol, cumulative_volume });
        }
        None => outcome.unparsed += 1,
    }
}

// 아무것도 읽지 않던 소켓을 이제 읽어 본다. 쌓인 데이터를 비운 뒤에도 연결이 살아 있으면 조용해지므로 타임아웃으로 구분한다.
async fn is_closed(socket: &mut Socket) -> bool {
    loop {
        match timeout(Duration::from_millis(500), socket.next()).await {
            Err(_) => return false,
            Ok(Some(Ok(Message::Close(_)))) | Ok(None) | Ok(Some(Err(_))) => return true,
            Ok(Some(Ok(_))) => {}
        }
    }
}

async fn setup(shared: &Shared, index: usize, outcome: &mut Outcome) -> Result<Socket, BoxError> {
    let mut request = shared.url.as_str().into_client_request()?;
    request.headers_mut().insert("sec-websocket-protocol", HeaderValue::from_static("v12.stomp"));
    let (mut socket, _) = connect_async_with_config(request, None, true).await?;
    let connection_id = format!("loadgen-{index}");
    let headers = [("accept-version", "1.2"), ("heart-beat", "5000,5000"), ("connection-id", connection_id.as_str())];
    socket.send(Message::text(serialize("CONNECT", &headers, ""))).await?;
    wait_for(&mut socket, shared, outcome, "CONNECTED", 1).await?;
    for (id, topic) in shared.topics.iter().enumerate() {
        let (id, receipt) = (id.to_string(), format!("{index}-{id}"));
        socket
            .send(Message::text(serialize(
                "SUBSCRIBE",
                &[("id", &id), ("receipt", &receipt), ("destination", topic)],
                "",
            )))
            .await?;
    }
    wait_for(&mut socket, shared, outcome, "RECEIPT", shared.topics.len()).await?;
    Ok(socket)
}

async fn wait_for(
    socket: &mut Socket,
    shared: &Shared,
    outcome: &mut Outcome,
    command: &str,
    mut count: usize,
) -> Result<(), BoxError> {
    while count > 0 {
        let Message::Text(text) = socket.next().await.ok_or("연결이 닫혔다")?? else { continue };
        if text.as_str().starts_with(command) {
            count -= 1;
        } else {
            outcome.count(shared, text.as_str());
        }
    }
    Ok(())
}
