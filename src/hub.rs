use crate::limits::{Limits, Rate};
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
    clients: HashMap<u64, Client>,
    index: HashMap<String, Vec<(u64, Arc<str>)>>,
}

impl Hub {
    pub fn spawn() -> Hub {
        Self::with_limits(Limits::default())
    }

    pub fn with_limits(limits: Limits) -> Hub {
        let (commands, mut inbox) = mpsc::channel(1024);
        tokio::spawn(async move {
            let mut state = State { limits, ..State::default() };
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

impl State {
    fn subscribe(&mut self, conn: u64, id: String, destination: String, receipt: Option<String>) {
        let Some(client) = self.clients.get_mut(&conn) else {
            return;
        };
        if client.closing {
            return;
        }
        let error = if !client.rate.allow(self.limits.commands_per_second) {
            Some("rate-limit-exceeded")
        } else if !self.limits.topics.is_empty() && !self.limits.topics.contains(&destination) {
            Some("unknown-topic")
        } else if self.limits.subscriptions > 0
            && client.subs.len() >= self.limits.subscriptions
            && !client.subs.contains_key(id.as_str())
        {
            Some("too-many-subscriptions")
        } else {
            None
        };
        if let Some(code) = error {
            client.closing = true;
            self.deliver(conn, Outgoing::Error(code));
            return;
        }
        let id: Arc<str> = id.into();
        if let Some(old) = client.subs.insert(id.clone(), destination.clone()) {
            self.unindex(conn, &id, &old);
        }
        self.index.entry(destination).or_default().push((conn, id));
        // 색인에 올린 바로 이 자리에서 RECEIPT를 큐에 넣는다. 이후 발행분은 항상 그 뒤에 쌓인다.
        if let Some(receipt) = receipt {
            self.deliver(conn, Outgoing::Receipt { receipt_id: receipt, message: "subscribe success" });
        }
    }

    // 색인에서 빼는 것과 RECEIPT를 큐에 넣는 것이 한 명령 안에서 일어난다.
    // 이미 큐에 있던 MESSAGE는 RECEIPT보다 앞에 있고, 이후 발행분은 이 구독으로 오지 않는다.
    // 구독 중이 아닌 id도 관측이 없어 같은 RECEIPT로 답한다.
    fn unsubscribe(&mut self, conn: u64, id: &str, receipt: Option<String>) {
        let Some(client) = self.clients.get_mut(&conn) else { return };
        if client.closing {
            return;
        }
        if !client.rate.allow(self.limits.commands_per_second) {
            client.closing = true;
            self.deliver(conn, Outgoing::Error("rate-limit-exceeded"));
            return;
        }
        if let Some(topic) = client.subs.remove(id) {
            self.unindex(conn, id, &topic);
        }
        if let Some(receipt) = receipt {
            self.deliver(conn, Outgoing::Receipt { receipt_id: receipt, message: "unsubscribe success" });
        }
    }

    fn publish(&mut self, topic: &str, body: Arc<str>) {
        let Some(targets) = self.index.get(topic) else {
            return;
        };
        let mut full = Vec::new();
        for (conn, id) in targets {
            let Some(client) = self.clients.get(conn) else {
                continue;
            };
            if client.closing {
                continue;
            }
            let message = Outgoing::Message { subscription: id.clone(), body: body.clone() };
            if let Err(TrySendError::Full(_)) = client.queue.try_send(message) {
                full.push(*conn);
            }
        }
        for conn in full {
            self.drop_slow(conn);
        }
    }

    fn deliver(&mut self, conn: u64, outgoing: Outgoing) {
        let Some(client) = self.clients.get(&conn) else {
            return;
        };
        if let Err(TrySendError::Full(_)) = client.queue.try_send(outgoing) {
            self.drop_slow(conn);
        }
    }

    fn drop_slow(&mut self, conn: u64) {
        if self.clients.contains_key(&conn) {
            eprintln!("느린 클라이언트 차단: conn {conn}");
            self.remove(conn);
        }
    }

    fn remove(&mut self, conn: u64) {
        let Some(client) = self.clients.remove(&conn) else {
            return;
        };
        for (id, topic) in client.subs {
            self.unindex(conn, &id, &topic);
        }
    }

    fn unindex(&mut self, conn: u64, id: &str, topic: &str) {
        let Some(list) = self.index.get_mut(topic) else {
            return;
        };
        list.retain(|(c, i)| !(*c == conn && &**i == id));
        if list.is_empty() {
            self.index.remove(topic);
        }
    }
}

#[cfg(test)]
mod tests;
