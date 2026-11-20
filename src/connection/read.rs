use super::Reader;
use crate::{
    hub::Hub,
    stomp::{self, Incoming},
};
use futures_util::StreamExt;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;

const PENDING_COMMANDS: usize = 32;

enum Command {
    Subscribe { id: String, destination: String, receipt: Option<String> },
    Unsubscribe { id: String, receipt: Option<String> },
}

pub(super) async fn run(
    reader: Reader,
    hub: &Hub,
    conn: u64,
    activity: Option<watch::Sender<Instant>>,
) -> &'static str {
    let (commands, pending) = mpsc::channel(PENDING_COMMANDS);
    tokio::select! {
        reason = receive(reader, commands, activity) => reason,
        _ = dispatch(pending, hub, conn) => "hub_closed",
    }
}

async fn dispatch(mut commands: mpsc::Receiver<Command>, hub: &Hub, conn: u64) {
    while let Some(command) = commands.recv().await {
        match command {
            Command::Subscribe { id, destination, receipt } => hub.subscribe(conn, id, destination, receipt).await,
            Command::Unsubscribe { id, receipt } => hub.unsubscribe(conn, id, receipt).await,
        }
    }
}

async fn receive(
    mut reader: Reader,
    commands: mpsc::Sender<Command>,
    activity: Option<watch::Sender<Instant>>,
) -> &'static str {
    loop {
        let message = match reader.next().await {
            Some(Ok(message)) => message,
            Some(Err(_)) => return "read_error",
            None => return "peer_closed",
        };
        // 명령 전달이 밀려도 소켓 수신과 유휴 시간 갱신은 계속한다.
        if let Some(activity) = &activity {
            activity.send_replace(Instant::now());
        }
        let text = match message {
            Message::Text(text) => text,
            Message::Ping(_) | Message::Pong(_) => continue,
            Message::Close(_) => return "peer_closed",
            _ => return "unsupported_message",
        };
        let Some(incoming) = stomp::parse(&text) else { return "invalid_frame" };
        let Incoming::Frame(frame) = incoming else { continue };
        let receipt = frame.header("receipt").map(str::to_string);
        let command = match frame.command.as_str() {
            "SUBSCRIBE" => {
                let (Some(id), Some(destination)) = (frame.header("id"), frame.header("destination")) else {
                    return "invalid_frame";
                };
                Command::Subscribe { id: id.into(), destination: destination.into(), receipt }
            }
            "UNSUBSCRIBE" => {
                let Some(id) = frame.header("id") else { return "invalid_command" };
                Command::Unsubscribe { id: id.into(), receipt }
            }
            "DISCONNECT" => return "client_disconnect",
            _ => return "unsupported_command",
        };
        if commands.try_send(command).is_err() {
            return "command_queue_full";
        }
        // 이미 읽을 프레임이 쌓여 있어도 같은 태스크의 명령 전달 경로가 실행될 틈을 준다.
        tokio::task::yield_now().await;
    }
}

#[cfg(test)]
mod tests;
