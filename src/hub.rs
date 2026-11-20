mod cache;
mod metrics;
use crate::metrics::{BridgeCounters, Metrics};
mod state;
use crate::limits::{Limits, Rate};
use crate::source::{AppliedRecord, Input, InputError, Publication};
use cache::Cache;
use std::collections::{HashMap, HashSet};
use std::sync::{
    Arc, Mutex, PoisonError,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot};

pub enum Outgoing {
    Error(&'static str),
    Receipt { receipt_id: String, message: &'static str },
    // 연결은 그대로 두고 구독 하나만 거절한다.
    Denied { receipt_id: String, code: &'static str },
    Message { subscription: Arc<str>, body: Arc<str> },
    // 최신 우선 topic의 표지. 쓰는 쪽이 꺼낼 때 칸에 든 가장 최근 본문을 MESSAGE로 쓴다.
    Latest { subscription: Arc<str>, slot: Arc<Slot> },
}

// 최신 우선 구독 하나의 대기 칸. 칸이 비어 있다가 채워질 때만 송신 큐에 표지가 하나 들어간다.
#[derive(Default)]
pub struct Slot(Mutex<Option<Arc<str>>>);

impl Slot {
    // 잠근 채 패닉하는 코드가 없어 독이 들 일은 없지만, 연결 태스크가 패닉하지 않도록 값을 그대로 쓴다.
    pub fn take(&self) -> Option<Arc<str>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).take()
    }

    // 칸이 차 있을 때만 값을 바꾸고 true를 돌려준다. 비어 있으면 그대로 둔다.
    pub(crate) fn replace(&self, body: &Arc<str>) -> bool {
        let mut pending = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        pending.as_mut().map(|value| *value = body.clone()).is_some()
    }

    pub(crate) fn fill(&self, body: Arc<str>) {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = Some(body);
    }
}

enum Command {
    #[cfg(test)]
    Pause {
        entered: oneshot::Sender<()>,
        resume: oneshot::Receiver<()>,
    },
    Connect {
        conn: u64,
        queue: mpsc::Sender<Outgoing>,
        kick: oneshot::Sender<&'static str>,
        principal: Option<String>,
    },
    Subscribe {
        conn: u64,
        id: String,
        destination: String,
        receipt: Option<String>,
    },
    Unsubscribe {
        conn: u64,
        id: String,
        receipt: Option<String>,
    },
    Disconnect {
        conn: u64,
    },
    Stop(oneshot::Sender<()>),
    Metrics(oneshot::Sender<Metrics>),
    Snapshot(oneshot::Sender<String>),
    Publish {
        publication: Publication,
        reply: Option<oneshot::Sender<Option<Arc<AppliedRecord>>>>,
    },
}

#[derive(Clone)]
pub struct Hub {
    commands: mpsc::Sender<Command>,
    log_drops: Arc<AtomicU64>,
    pub(crate) bridge: Arc<BridgeCounters>,
}

struct Client {
    queue: mpsc::Sender<Outgoing>,
    // 이 값을 버리면 연결 쪽 kicked 수신이 풀려서 연결이 끊긴다.
    kick: Option<oneshot::Sender<&'static str>>,
    subs: HashMap<Arc<str>, String>,
    principal: Option<String>,
    rate: Rate,
    closing: bool,
}

struct Target {
    conn: u64,
    id: Arc<str>,
    // 최신 우선 topic의 구독에만 있다. 끊기 정책 topic은 할당도 잠금도 하지 않는다.
    slot: Option<Arc<Slot>>,
}

#[derive(Default)]
struct State {
    limits: Limits,
    log_drops: Arc<AtomicU64>,
    bridge: Arc<BridgeCounters>,
    published: u64,
    enqueued: u64,
    slow_disconnections: u64,
    latest_skipped: u64,
    latest_first: HashSet<String>,
    latest_first_depth: usize,
    cache: Cache,
    clients: HashMap<u64, Client>,
    index: HashMap<String, Vec<Target>>,
}

// 접두사 뒤 첫 구간이 소유자다. 소유자 구간이 비어도 공개 topic으로 새지 않게 개인 topic으로 남긴다.
fn personal_owner(destination: &str) -> Option<&str> {
    if destination == "/experimental/personal" {
        return Some("");
    }
    let rest = destination.strip_prefix("/experimental/personal/")?;
    Some(rest.split_once('/').map_or(rest, |(owner, _)| owner))
}

impl Hub {
    pub fn spawn() -> Hub {
        Self::with_limits(Limits::default())
    }

    pub fn with_limits(limits: Limits) -> Hub {
        Self::configured(limits, Vec::new())
    }

    pub fn configured(limits: Limits, topics: Vec<String>) -> Hub {
        Self::with_latest_first(limits, topics, Vec::new(), 0)
    }

    // latest_first_topics에 정확히 같은 topic만 최신 우선 정책이다. 나머지는 송신 큐가 차면 연결을 끊는다.
    // 최신 우선 topic도 큐에 latest_first_depth건 미만이 쌓여 있으면 일반 MESSAGE로 넣고, 넘을 때만 칸으로 접는다.
    pub fn with_latest_first(
        limits: Limits,
        topics: Vec<String>,
        latest_first_topics: Vec<String>,
        latest_first_depth: usize,
    ) -> Hub {
        let (commands, mut inbox) = mpsc::channel(1024);
        let bridge = Arc::new(BridgeCounters::default());
        let counters = bridge.clone();
        let log_drops = Arc::new(AtomicU64::new(0));
        let drops = log_drops.clone();
        tokio::spawn(async move {
            let mut state = State {
                limits,
                log_drops: drops,
                bridge: counters,
                cache: Cache::new(topics),
                latest_first: latest_first_topics.into_iter().collect(),
                latest_first_depth,
                ..State::default()
            };
            while let Some(command) = inbox.recv().await {
                match command {
                    #[cfg(test)]
                    Command::Pause { entered, resume } => {
                        let _ = entered.send(());
                        let _ = resume.await;
                    }
                    Command::Connect { conn, queue, kick, principal } => {
                        state.clients.insert(
                            conn,
                            Client {
                                queue,
                                kick: Some(kick),
                                subs: HashMap::new(),
                                principal,
                                rate: Rate::default(),
                                closing: false,
                            },
                        );
                    }
                    Command::Subscribe { conn, id, destination, receipt } => {
                        state.subscribe(conn, id, destination, receipt)
                    }
                    Command::Unsubscribe { conn, id, receipt } => state.unsubscribe(conn, &id, receipt),
                    Command::Disconnect { conn } => state.remove(conn),
                    Command::Stop(reply) => {
                        drop(state);
                        inbox.close();
                        let _ = reply.send(());
                        break;
                    }
                    Command::Metrics(reply) => {
                        let _ = reply.send(state.metrics(inbox.len()));
                    }
                    Command::Snapshot(reply) => {
                        let _ = reply.send(state.cache.json());
                    }
                    Command::Publish { publication, reply } => {
                        state.publish(&publication.topic, publication.body);
                        if let Some(reply) = reply {
                            let _ = reply.send(publication.record);
                        }
                    }
                }
            }
        });
        Hub { commands, bridge, log_drops }
    }

    pub async fn connect(
        &self,
        conn: u64,
        queue_capacity: usize,
    ) -> (mpsc::Receiver<Outgoing>, oneshot::Receiver<&'static str>) {
        self.connect_as(conn, queue_capacity, None).await
    }

    pub(crate) async fn connect_as(
        &self,
        conn: u64,
        queue_capacity: usize,
        principal: Option<String>,
    ) -> (mpsc::Receiver<Outgoing>, oneshot::Receiver<&'static str>) {
        let (queue, outgoing) = mpsc::channel(queue_capacity);
        let (kick, kicked) = oneshot::channel();
        self.send(Command::Connect { conn, queue, kick, principal }).await;
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

    pub(crate) async fn stop(&self) {
        let (reply, done) = oneshot::channel();
        self.send(Command::Stop(reply)).await;
        let _ = done.await;
    }

    pub(crate) fn record_log_drops(&self, dropped: u64) {
        self.log_drops.store(dropped, Ordering::Relaxed);
    }

    pub async fn metrics(&self) -> Option<Metrics> {
        let (reply, response) = oneshot::channel();
        self.send(Command::Metrics(reply)).await;
        response.await.ok()
    }

    pub async fn snapshot(&self) -> Option<String> {
        let (reply, response) = oneshot::channel();
        self.send(Command::Snapshot(reply)).await;
        response.await.ok()
    }

    pub async fn publish(&self, topic: String, body: Arc<str>) {
        self.send(Command::Publish { publication: Publication::local(topic, body), reply: None }).await;
    }

    // 런타임 밖의 스레드(링 브리지)에서 부른다. 채널이 차면 자리가 날 때까지 막힌다. 실패를 무시하는 이유는 send와 같다.
    pub fn publish_blocking(&self, topic: String, body: Arc<str>) -> bool {
        self.enqueue_input_blocking(Publication::local(topic, body)).is_ok()
    }

    pub fn input(&self) -> Input {
        Input { hub: self.clone() }
    }

    pub(crate) async fn publish_input(
        &self,
        publication: &Publication,
    ) -> Result<Option<Arc<AppliedRecord>>, InputError> {
        // 대기 중 취소되면 입력을 소비하지 않으며, 전송 이후 취소는 적용 취소를 뜻하지 않는다.
        let permit = self.commands.reserve().await.map_err(|_| InputError::Closed)?;
        let (reply, applied) = oneshot::channel();
        permit.send(Command::Publish { publication: publication.clone(), reply: Some(reply) });
        applied.await.map_err(|_| InputError::CompletionLost)
    }

    pub(crate) fn enqueue_input_blocking(&self, publication: Publication) -> Result<(), InputError> {
        self.commands.blocking_send(Command::Publish { publication, reply: None }).map_err(|_| InputError::Closed)
    }

    async fn send(&self, command: Command) {
        // 종료된 hub에는 새 명령을 전달하지 않는다.
        let _ = self.commands.send(command).await;
    }
}

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;
