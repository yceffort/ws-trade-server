mod cache;
mod state;
use crate::limits::{Limits, Rate};
use cache::Cache;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot};

pub enum Outgoing {
    Error(&'static str),
    Receipt { receipt_id: String, message: &'static str },
    Message { subscription: Arc<str>, body: Arc<str> },
}

enum Command {
    Connect { conn: u64, queue: mpsc::Sender<Outgoing>, kick: oneshot::Sender<()> },
    Subscribe { conn: u64, id: String, destination: String, receipt: Option<String> },
    Unsubscribe { conn: u64, id: String, receipt: Option<String> },
    Disconnect { conn: u64 },
    Snapshot(oneshot::Sender<String>),
    Publish { topic: String, body: Arc<str> },
}

#[derive(Clone)]
pub struct Hub {
    commands: mpsc::Sender<Command>,
}

struct Client {
    queue: mpsc::Sender<Outgoing>,
    // 이 값을 버리면 연결 쪽 kicked 수신이 풀려서 연결이 끊긴다.
    _kick: oneshot::Sender<()>,
    subs: HashMap<Arc<str>, String>,
    rate: Rate,
    closing: bool,
}

#[derive(Default)]
struct State {
    limits: Limits,
    cache: Cache,
    clients: HashMap<u64, Client>,
    index: HashMap<String, Vec<(u64, Arc<str>)>>,
}

impl Hub {
    pub fn spawn() -> Hub {
        Self::with_limits(Limits::default())
    }

    pub fn with_limits(limits: Limits) -> Hub {
        Self::configured(limits, Vec::new())
    }

    pub fn configured(limits: Limits, topics: Vec<String>) -> Hub {
        let (commands, mut inbox) = mpsc::channel(1024);
        tokio::spawn(async move {
            let mut state = State { limits, cache: Cache::new(topics), ..State::default() };
            while let Some(command) = inbox.recv().await {
                match command {
                    Command::Connect { conn, queue, kick } => {
                        state.clients.insert(
                            conn,
                            Client { queue, _kick: kick, subs: HashMap::new(), rate: Rate::default(), closing: false },
                        );
                    }
                    Command::Subscribe { conn, id, destination, receipt } => {
                        state.subscribe(conn, id, destination, receipt)
                    }
                    Command::Unsubscribe { conn, id, receipt } => state.unsubscribe(conn, &id, receipt),
                    Command::Disconnect { conn } => state.remove(conn),
                    Command::Snapshot(reply) => {
                        let _ = reply.send(state.cache.json());
                    }
                    Command::Publish { topic, body } => state.publish(&topic, body),
                }
            }
        });
        Hub { commands }
    }

    pub async fn connect(&self, conn: u64, queue_capacity: usize) -> (mpsc::Receiver<Outgoing>, oneshot::Receiver<()>) {
        let (queue, outgoing) = mpsc::channel(queue_capacity);
        let (kick, kicked) = oneshot::channel();
        self.send(Command::Connect { conn, queue, kick }).await;
        (outgoing, kicked)
    }

    pub async fn subscribe(&self, conn: u64, id: String, destination: String, receipt: Option<String>) {
        self.send(Command::Subscribe { conn, id, destination, receipt }).await;
    }

    pub async fn unsubscribe(&self, conn: u64, id: String, receipt: Option<String>) {
        self.send(Command::Unsubscribe { conn, id, receipt }).await;
    }

    pub async fn disconnect(&self, conn: u64) {
        self.send(Command::Disconnect { conn }).await;
    }

    pub async fn snapshot(&self) -> Option<String> {
        let (reply, response) = oneshot::channel();
        self.send(Command::Snapshot(reply)).await;
        response.await.ok()
    }

    pub async fn publish(&self, topic: String, body: Arc<str>) {
        self.send(Command::Publish { topic, body }).await;
    }

    // 런타임 밖의 스레드(링 브리지)에서 부른다. 채널이 차면 자리가 날 때까지 막힌다. 실패를 무시하는 이유는 send와 같다.
    pub fn publish_blocking(&self, topic: String, body: Arc<str>) {
        let _ = self.commands.blocking_send(Command::Publish { topic, body });
    }

    async fn send(&self, command: Command) {
        // hub 태스크는 프로세스와 수명이 같다. 여기서 실패하면 이미 끝난 상태라 할 일이 없다.
        let _ = self.commands.send(command).await;
    }
}

#[cfg(test)]
mod tests;
