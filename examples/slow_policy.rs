// 느린 수신자 정책을 같은 조건에서 잰다. 지금 방식(큐가 차면 끊기), (가) 덮어쓰는 고리, (나) 구독별 최신 한 칸.
// 사용: cargo run --release --example slow_policy
use futures_util::FutureExt;
use std::collections::{HashMap, VecDeque};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::{JoinHandle, JoinSet};
use ws_trade_server::hub::{Hub, Outgoing};

// gateway의 송신 큐 기본 용량(Config::queue_capacity)과 같다.
const CAPACITY: usize = 1024;
// 이 접두사의 topic만 최신 우선 정책을 쓴다. 나머지 topic은 지금처럼 큐가 차면 끊는다.
const LATEST_FIRST: &str = "/latest/";
const HOT: &str = "/latest/hot";
const QUIET: &str = "/latest/quiet";

enum Push {
    Queued,
    Dropped,
    Full,
}

// hub가 쥐는 송신단. Full을 돌려주면 hub가 그 연결을 끊는다.
trait Queue: Send + Sized + 'static {
    // 구독마다 두는 상태. 같은 id로 다시 구독할 때도 새로 만든다.
    type Sub: Default + Send + 'static;
    type Rx: Pop;
    fn open(capacity: usize) -> (Self, Self::Rx);
    // latest는 최신 우선 topic의 MESSAGE에만 넘긴다. None으로 넣은 항목은 버리지 않는다.
    fn push(&self, item: Outgoing, latest: Option<&Self::Sub>) -> Push;
}

// 연결 태스크가 쥐는 수신단.
trait Pop: Send + 'static {
    fn pop(&mut self) -> impl Future<Output = Option<Outgoing>> + Send;
}

// 지금 방식. 본체 hub와 같은 mpsc 큐이고 차면 끊는다.
type Current = mpsc::Sender<Outgoing>;

impl Queue for Current {
    type Sub = ();
    type Rx = mpsc::Receiver<Outgoing>;

    fn open(capacity: usize) -> (Self, Self::Rx) {
        mpsc::channel(capacity)
    }

    fn push(&self, item: Outgoing, _: Option<&()>) -> Push {
        match self.try_send(item) {
            Err(TrySendError::Full(_)) => Push::Full,
            _ => Push::Queued,
        }
    }
}

impl Pop for mpsc::Receiver<Outgoing> {
    async fn pop(&mut self) -> Option<Outgoing> {
        self.recv().await
    }
}

// (가) 덮어쓰는 고리. 가득 차면 가장 오래된, 버려도 되는 MESSAGE 하나를 빼고 새 것을 넣는다.
struct Ring {
    items: Mutex<VecDeque<(Outgoing, bool)>>,
    capacity: usize,
    ready: Notify,
    closed: AtomicBool,
}

struct RingTx(Arc<Ring>);
struct RingRx(Arc<Ring>);

impl Queue for RingTx {
    type Sub = ();
    type Rx = RingRx;

    fn open(capacity: usize) -> (Self, RingRx) {
        let ring = Ring { items: Mutex::default(), capacity, ready: Notify::new(), closed: AtomicBool::new(false) };
        let ring = Arc::new(ring);
        (RingTx(ring.clone()), RingRx(ring))
    }

    fn push(&self, item: Outgoing, latest: Option<&()>) -> Push {
        let mut items = self.0.items.lock().unwrap();
        let mut pushed = Push::Queued;
        if items.len() == self.0.capacity {
            // 버릴 수 없는 항목만 남았으면 지금처럼 끊는다.
            let Some(oldest) = items.iter().position(|(_, droppable)| *droppable) else { return Push::Full };
            items.remove(oldest);
            pushed = Push::Dropped;
        }
        items.push_back((item, latest.is_some()));
        drop(items);
        self.0.ready.notify_one();
        pushed
    }
}

// hub가 연결을 끊으면서 송신단을 버리면 수신단은 남은 것을 비운 뒤 None을 받는다(mpsc와 같다).
impl Drop for RingTx {
    fn drop(&mut self) {
        self.0.closed.store(true, Ordering::Release);
        self.0.ready.notify_one();
    }
}

impl Pop for RingRx {
    async fn pop(&mut self) -> Option<Outgoing> {
        loop {
            // 닫힘을 먼저 읽어야 닫히기 직전에 들어온 항목을 놓치지 않는다.
            let closed = self.0.closed.load(Ordering::Acquire);
            let front = self.0.items.lock().unwrap().pop_front();
            if let Some((item, _)) = front {
                return Some(item);
            }
            if closed {
                return None;
            }
            self.0.ready.notified().await;
        }
    }
}

// (나) 구독별 최신 한 칸. 칸이 비어 있다가 채워질 때만 큐에 표지를 넣고, 이미 차 있으면 값만 바꾼다.
type Slot = Arc<Mutex<Option<Arc<str>>>>;

enum Queued {
    Now(Outgoing),
    Latest(Arc<str>, Slot),
}

struct SlotTx(mpsc::Sender<Queued>);
struct SlotRx(mpsc::Receiver<Queued>);

impl Queue for SlotTx {
    type Sub = Slot;
    type Rx = SlotRx;

    fn open(capacity: usize) -> (Self, SlotRx) {
        let (queue, outgoing) = mpsc::channel(capacity);
        (SlotTx(queue), SlotRx(outgoing))
    }

    fn push(&self, item: Outgoing, latest: Option<&Slot>) -> Push {
        let queued = match (item, latest) {
            (Outgoing::Message { subscription, body }, Some(slot)) => {
                if slot.lock().unwrap().replace(body).is_some() {
                    return Push::Dropped;
                }
                Queued::Latest(subscription, slot.clone())
            }
            (item, _) => Queued::Now(item),
        };
        match self.0.try_send(queued) {
            Err(TrySendError::Full(_)) => Push::Full,
            _ => Push::Queued,
        }
    }
}

impl Pop for SlotRx {
    async fn pop(&mut self) -> Option<Outgoing> {
        Some(match self.0.recv().await? {
            Queued::Now(item) => item,
            Queued::Latest(subscription, slot) => {
                let body = slot.lock().unwrap().take().expect("칸이 채워질 때마다 표지가 하나씩 들어간다");
                Outgoing::Message { subscription, body }
            }
        })
    }
}

// (나)에 깊이 K를 더한 것(본체가 쓰는 방식). 칸이 비어 있고 큐에 K건 미만이 쌓여 있으면 지금 방식처럼 넣는다.
// 칸이 차 있으면 깊이와 상관없이 값만 바꿔야 표지보다 오래된 MESSAGE가 뒤에 나가지 않는다.
struct DepthTx<const K: usize>(SlotTx);

impl<const K: usize> Queue for DepthTx<K> {
    type Sub = Slot;
    type Rx = SlotRx;

    fn open(capacity: usize) -> (Self, SlotRx) {
        let (queue, outgoing) = SlotTx::open(capacity);
        (DepthTx(queue), outgoing)
    }

    fn push(&self, item: Outgoing, latest: Option<&Slot>) -> Push {
        let pending = latest.is_some_and(|slot| slot.lock().unwrap().is_some());
        let queued = self.0.0.max_capacity() - self.0.0.capacity();
        self.0.push(item, latest.filter(|_| pending || queued >= K))
    }
}

enum Cmd<Q> {
    Connect(u64, Q),
    Subscribe { conn: u64, id: Arc<str>, topic: String, receipt: Option<String> },
    Unsubscribe { conn: u64, id: Arc<str>, receipt: Option<String> },
    Publish(String, Arc<str>),
    Counts(oneshot::Sender<(u64, u64)>),
}

fn subscribe<Q>(conn: u64, id: &str, topic: &str) -> Cmd<Q> {
    Cmd::Subscribe { conn, id: id.into(), topic: topic.into(), receipt: None }
}

// 본체 hub의 색인과 팬아웃만 옮긴 축소판. 세 방식은 송신 큐(Q)만 다르다.
struct Mini<Q>(mpsc::Sender<Cmd<Q>>);

struct Client<Q> {
    queue: Q,
    subs: HashMap<Arc<str>, String>,
}

// topic마다 (연결, 구독 id, 구독 상태).
type Targets<S> = Vec<(u64, Arc<str>, S)>;

struct State<Q: Queue> {
    clients: HashMap<u64, Client<Q>>,
    index: HashMap<String, Targets<Q::Sub>>,
    dropped: u64,
    kicked: u64,
}

impl<Q: Queue> Mini<Q> {
    fn spawn() -> Self {
        let (commands, mut inbox) = mpsc::channel(1024);
        tokio::spawn(async move {
            let mut state = State::<Q> { clients: HashMap::new(), index: HashMap::new(), dropped: 0, kicked: 0 };
            while let Some(command) = inbox.recv().await {
                match command {
                    Cmd::Connect(conn, queue) => {
                        state.clients.insert(conn, Client { queue, subs: HashMap::new() });
                    }
                    Cmd::Subscribe { conn, id, topic, receipt } => state.subscribe(conn, id, topic, receipt),
                    Cmd::Unsubscribe { conn, id, receipt } => state.unsubscribe(conn, &id, receipt),
                    Cmd::Publish(topic, body) => state.publish(&topic, body),
                    Cmd::Counts(reply) => {
                        let _ = reply.send((state.dropped, state.kicked));
                    }
                }
            }
        });
        Mini(commands)
    }

    async fn send(&self, command: Cmd<Q>) {
        let _ = self.0.send(command).await;
    }

    fn send_blocking(&self, command: Cmd<Q>) {
        let _ = self.0.blocking_send(command);
    }

    // 앞서 보낸 명령을 hub가 다 처리한 뒤에 돌아온다. (버린 MESSAGE 수, 끊은 연결 수)
    async fn counts(&self) -> (u64, u64) {
        let (reply, counts) = oneshot::channel();
        self.send(Cmd::Counts(reply)).await;
        counts.await.expect("hub가 돌고 있다")
    }
}

impl<Q: Queue> State<Q> {
    fn subscribe(&mut self, conn: u64, id: Arc<str>, topic: String, receipt: Option<String>) {
        let Some(client) = self.clients.get_mut(&conn) else { return };
        if let Some(old) = client.subs.insert(id.clone(), topic.clone()) {
            self.unindex(conn, &id, &old);
        }
        self.index.entry(topic).or_default().push((conn, id, Q::Sub::default()));
        if let Some(receipt_id) = receipt {
            self.control(conn, Outgoing::Receipt { receipt_id, message: "subscribe success" });
        }
    }

    fn unsubscribe(&mut self, conn: u64, id: &str, receipt: Option<String>) {
        let Some(client) = self.clients.get_mut(&conn) else { return };
        if let Some(topic) = client.subs.remove(id) {
            self.unindex(conn, id, &topic);
        }
        if let Some(receipt_id) = receipt {
            self.control(conn, Outgoing::Receipt { receipt_id, message: "unsubscribe success" });
        }
    }

    fn publish(&mut self, topic: &str, body: Arc<str>) {
        let latest = topic.starts_with(LATEST_FIRST);
        let Some(targets) = self.index.get(topic) else { return };
        let mut full = Vec::new();
        for (conn, id, sub) in targets {
            let Some(client) = self.clients.get(conn) else { continue };
            let message = Outgoing::Message { subscription: id.clone(), body: body.clone() };
            match client.queue.push(message, latest.then_some(sub)) {
                Push::Queued => {}
                Push::Dropped => self.dropped += 1,
                Push::Full => full.push(*conn),
            }
        }
        for conn in full {
            self.kick(conn);
        }
    }

    fn control(&mut self, conn: u64, item: Outgoing) {
        let Some(client) = self.clients.get(&conn) else { return };
        if let Push::Full = client.queue.push(item, None) {
            self.kick(conn);
        }
    }

    fn kick(&mut self, conn: u64) {
        let Some(client) = self.clients.remove(&conn) else { return };
        self.kicked += 1;
        for (id, topic) in client.subs {
            self.unindex(conn, &id, &topic);
        }
    }

    fn unindex(&mut self, conn: u64, id: &str, topic: &str) {
        let Some(list) = self.index.get_mut(topic) else { return };
        list.retain(|(c, i, _)| !(*c == conn && &**i == id));
        if list.is_empty() {
            self.index.remove(topic);
        }
    }
}

// n번째 발행분의 본문. 길이는 실제 체결 본문과 비슷하게 248바이트로 맞춘다.
fn body(n: u64) -> Arc<str> {
    format!("{n:>248}").into()
}

fn number(body: &str) -> u64 {
    body.trim_start().parse().expect("본문은 발행 번호다")
}

fn spin_until(deadline: Instant) {
    while Instant::now() < deadline {
        std::hint::spin_loop();
    }
}

// 프로세스 CPU 시간(초). unsafe 없이 getrusage를 부를 수 없어 ps로 읽는다. macOS에서 해상도는 10ms다.
fn cpu_seconds() -> f64 {
    let pid = std::process::id().to_string();
    let out = Command::new("ps").args(["-o", "cputime=", "-p", &pid]).output().expect("ps를 실행한다");
    let text = String::from_utf8(out.stdout).expect("ps 출력은 UTF-8이다");
    text.trim().split(':').fold(0.0, |total, part| total * 60.0 + part.parse::<f64>().expect("cputime 형식"))
}

fn percentile(sorted: &[u64], q: f64) -> u64 {
    sorted[((sorted.len() as f64 * q).ceil() as usize).max(1) - 1]
}

// 세는 구독자가 표본 하나를 다 받으면 깨운다.
struct Tally {
    expected: usize,
    seen: AtomicUsize,
    done: Notify,
}

fn tally(expected: usize) -> Arc<Tally> {
    Arc::new(Tally { expected, seen: AtomicUsize::new(0), done: Notify::new() })
}

async fn count(mut rx: impl Pop, tally: Arc<Tally>) {
    while let Some(item) = rx.pop().await {
        if matches!(item, Outgoing::Message { .. }) && tally.seen.fetch_add(1, Ordering::Relaxed) + 1 == tally.expected
        {
            tally.done.notify_one();
        }
    }
}

const WARMUP: u64 = 20;

struct Latency {
    p50_us: f64,
    p99_us: f64,
    cpu_us: f64,
}

// 발행부터 세는 구독자 전원이 본문을 손에 쥘 때까지가 표본 하나다.
async fn measure(tally: &Tally, samples: u64, publish: impl AsyncFn(u64)) -> Latency {
    let mut nanos = Vec::with_capacity((WARMUP + samples) as usize);
    let mut cpu = 0.0;
    for n in 0..WARMUP + samples {
        if n == WARMUP {
            cpu = cpu_seconds();
        }
        let start = Instant::now();
        publish(n).await;
        tally.done.notified().await;
        nanos.push(start.elapsed().as_nanos() as u64);
        tally.seen.store(0, Ordering::Relaxed);
    }
    let cpu = cpu_seconds() - cpu;
    let mut nanos = nanos.split_off(WARMUP as usize);
    nanos.sort_unstable();
    let us = |q| percentile(&nanos, q) as f64 / 1e3;
    Latency { p50_us: us(0.5), p99_us: us(0.99), cpu_us: cpu * 1e6 / samples as f64 }
}

#[derive(Default)]
struct Unstalled {
    first: Option<u64>,
    hot: u64,
    frames: usize,
    body_bytes: usize,
    quiet: bool,
    latest_after: Option<Duration>,
    kicked: u64,
}

// 문이 열리기 전에는 꺼내지 않는다. 열리면 hot의 마지막 발행분(last)을 받을 때까지, 끊겼으면 큐가 빌 때까지 꺼낸다.
async fn unstall(mut rx: impl Pop, gate: oneshot::Receiver<()>, last: u64) -> Unstalled {
    let _ = gate.await;
    let opened = Instant::now();
    let mut seen = Unstalled::default();
    while let Some(Outgoing::Message { subscription, body }) = rx.pop().await {
        seen.frames += 1;
        seen.body_bytes += body.len();
        if &*subscription == "quiet" {
            seen.quiet = true;
            continue;
        }
        let n = number(&body);
        seen.first.get_or_insert(n);
        seen.hot += 1;
        if n == last {
            seen.latest_after = Some(opened.elapsed());
            break;
        }
    }
    seen
}

// 연결마다 태스크 하나가 자기 큐에서 꺼낸다. stall이면 0번 연결은 hot과 quiet을 구독한 채 측정이 끝날 때까지 꺼내지 않는다.
async fn fanout<Q: Queue>(connections: u64, samples: u64, stall: bool) -> (Latency, Option<Unstalled>) {
    let hub = Mini::<Q>::spawn();
    let tally = tally(connections as usize - usize::from(stall));
    let mut counters = JoinSet::new();
    let mut stalled = None;
    for conn in 0..connections {
        let (queue, rx) = Q::open(CAPACITY);
        hub.send(Cmd::Connect(conn, queue)).await;
        hub.send(subscribe(conn, "hot", HOT)).await;
        if stall && conn == 0 {
            hub.send(subscribe(conn, "quiet", QUIET)).await;
            let (open, gate) = oneshot::channel();
            stalled = Some((open, tokio::spawn(unstall(rx, gate, WARMUP + samples - 1))));
        } else {
            counters.spawn(count(rx, tally.clone()));
        }
    }
    // quiet은 측정 전에 한 번만 발행한다. 그 뒤로 hot만 큐를 채운다.
    hub.send(Cmd::Publish(QUIET.into(), body(0))).await;
    hub.counts().await;
    let latency = measure(&tally, samples, async |n| hub.send(Cmd::Publish(HOT.into(), body(n))).await).await;
    let mut unstalled = None;
    if let Some((open, task)) = stalled {
        let _ = open.send(());
        let mut seen = task.await.expect("멈춘 연결 태스크");
        seen.kicked = hub.counts().await.1;
        unstalled = Some(seen);
    }
    drop(hub);
    counters.join_all().await;
    (latency, unstalled)
}

// 같은 측정을 본체 Hub로 한다. 축소판이 본체와 같은 비용인지 견주는 기준이다.
async fn fanout_hub(connections: u64, samples: u64) -> Latency {
    let hub = Hub::spawn();
    let tally = tally(connections as usize);
    let mut counters = JoinSet::new();
    for conn in 0..connections {
        let (rx, _kicked) = hub.connect(conn, CAPACITY).await;
        hub.subscribe(conn, "hot".into(), HOT.into(), None).await;
        counters.spawn(count(rx, tally.clone()));
    }
    hub.metrics().await;
    let latency = measure(&tally, samples, async |n| hub.publish(HOT.into(), body(n)).await).await;
    drop(hub);
    counters.join_all().await;
    latency
}

const SYMBOLS: [&str; 3] = ["/latest/a", "/latest/b", "/latest/c"];

// 한 연결이 세 종목을 구독한다. MESSAGE를 꺼낼 때마다 소켓 쓰기를 흉내 내며 service 동안 워커를 붙든다.
async fn lane<Q: Queue>(service: Duration) -> (Mini<Q>, JoinHandle<Vec<(u64, Instant)>>) {
    let hub = Mini::<Q>::spawn();
    let (queue, mut rx) = Q::open(CAPACITY);
    hub.send(Cmd::Connect(0, queue)).await;
    for topic in SYMBOLS {
        hub.send(subscribe(0, topic, topic)).await;
    }
    hub.counts().await;
    let task = tokio::spawn(async move {
        let (mut got, mut free_at) = (Vec::new(), Instant::now());
        loop {
            // 기다리지 않고 꺼냈으면 앞 프레임이 끝났어야 할 시각부터 잰다. 스레드가 운영체제에 잠깐 밀려도 처리 속도는 service로 유지된다.
            let (item, queued) = match rx.pop().now_or_never() {
                Some(item) => (item, true),
                None => (rx.pop().await, false),
            };
            let Some(item) = item else { break };
            if let Outgoing::Message { body, .. } = item {
                let now = Instant::now();
                got.push((number(&body), now));
                free_at = if queued { free_at } else { now } + service;
                spin_until(free_at);
            }
        }
        got
    });
    (hub, task)
}

// 종목마다 초당 rate건을 지수 분포 간격으로 발행하고(loadgen과 같다) 발행 시각을 돌려준다.
// 링 브리지처럼 런타임 밖의 스레드에서 부른다.
fn publish_poisson(rate: f64, seconds: f64, mut publish: impl FnMut(&str, Arc<str>)) -> Vec<Instant> {
    let interval = 1.0 / (rate * SYMBOLS.len() as f64);
    let start = Instant::now();
    let (mut at, mut sent) = (0.0, Vec::new());
    while at < seconds {
        at += -(1.0 - rand::random::<f64>()).ln() * interval;
        spin_until(start + Duration::from_secs_f64(at));
        publish(SYMBOLS[rand::random_range(0..SYMBOLS.len())], body(sent.len() as u64));
        sent.push(Instant::now());
    }
    sent
}

// 처리 시간은 평균 발행 간격의 ratio배다. 모든 방식에 같은 발행분을 같은 시각에 넣는다.
async fn lagging(rate: f64, ratio: f64, seconds: f64) {
    let service = Duration::from_secs_f64(ratio / (rate * SYMBOLS.len() as f64));
    let (now, now_task) = lane::<Current>(service).await;
    let (ring, ring_task) = lane::<RingTx>(service).await;
    let (slot, slot_task) = lane::<SlotTx>(service).await;
    let (k8, k8_task) = lane::<DepthTx<8>>(service).await;
    let (k64, k64_task) = lane::<DepthTx<64>>(service).await;
    let (now, ring, slot, k8, k64, sent) = tokio::task::spawn_blocking(move || {
        let sent = publish_poisson(rate, seconds, |topic, body| {
            now.send_blocking(Cmd::Publish(topic.into(), body.clone()));
            ring.send_blocking(Cmd::Publish(topic.into(), body.clone()));
            slot.send_blocking(Cmd::Publish(topic.into(), body.clone()));
            k8.send_blocking(Cmd::Publish(topic.into(), body.clone()));
            k64.send_blocking(Cmd::Publish(topic.into(), body));
        });
        (now, ring, slot, k8, k64, sent)
    })
    .await
    .expect("발행 스레드");
    let kicked =
        [now.counts().await.1, ring.counts().await.1, slot.counts().await.1, k8.counts().await.1, k64.counts().await.1];
    drop((now, ring, slot, k8, k64));
    let tasks = [
        ("지금(끊기)", now_task),
        ("(가) 고리", ring_task),
        ("(나) 한 칸", slot_task),
        ("(나) K=8", k8_task),
        ("(나) K=64", k64_task),
    ];
    for ((name, task), kicked) in tasks.into_iter().zip(kicked) {
        let got = task.await.expect("수신 태스크");
        let mut ages: Vec<u64> =
            got.iter().map(|(n, t)| t.duration_since(sent[*n as usize]).as_micros() as u64).collect();
        ages.sort_unstable();
        let ms = |q| if ages.is_empty() { f64::NAN } else { percentile(&ages, q) as f64 / 1e3 };
        println!(
            "{rate:>5} | {ratio:>4.1} | {name:<10} | {:>5} | {:>5} | {:>5.1} | {:>9.2} | {:>9.2} | {}",
            sent.len(),
            got.len(),
            100.0 * (1.0 - got.len() as f64 / sent.len() as f64),
            ms(0.5),
            ms(0.99),
            if kicked > 0 { "끊김" } else { "-" },
        );
    }
}

// 쓰는 쪽을 일부러 늦추지 않는다. 연결마다 세 종목을 모두 구독하고, 받은 MESSAGE 수만 센다.
async fn keep_up<Q: Queue>(name: &str, connections: u64, rate: f64, seconds: f64) {
    let hub = Mini::<Q>::spawn();
    let mut readers = JoinSet::new();
    for conn in 0..connections {
        let (queue, mut rx) = Q::open(CAPACITY);
        hub.send(Cmd::Connect(conn, queue)).await;
        for topic in SYMBOLS {
            hub.send(subscribe(conn, topic, topic)).await;
        }
        readers.spawn(async move {
            let mut got = 0;
            while let Some(item) = rx.pop().await {
                got += u64::from(matches!(item, Outgoing::Message { .. }));
            }
            got
        });
    }
    hub.counts().await;
    let (cpu, start) = (cpu_seconds(), Instant::now());
    let (hub, sent) = tokio::task::spawn_blocking(move || {
        let sent = publish_poisson(rate, seconds, |topic, body| hub.send_blocking(Cmd::Publish(topic.into(), body)));
        (hub, sent.len() as u64)
    })
    .await
    .expect("발행 스레드");
    let kicked = hub.counts().await.1;
    let cores = (cpu_seconds() - cpu) / start.elapsed().as_secs_f64();
    drop(hub);
    let got: u64 = readers.join_all().await.into_iter().sum();
    let expected = sent * connections;
    println!(
        "{connections:>6} | {rate:>5} | {name:<10} | {sent:>5} | {expected:>9} | {got:>9} | {:>6.2} | {kicked} | {cores:>4.1}",
        100.0 * (1.0 - got as f64 / expected as f64),
    );
}

const IDS: [&str; 3] = ["a", "b", "c"];
const MIXED: [&str; 3] = ["/latest/x", "/latest/y", "/strict"];

enum Step {
    Sub(usize, usize, Option<String>),
    Unsub(usize, Option<String>),
    Publish(usize, u64),
}

impl Step {
    fn random(n: u64) -> Step {
        let receipt = rand::random_bool(0.5).then(|| format!("r{n}"));
        match rand::random_range(0..100) {
            0..4 => Step::Sub(rand::random_range(0..3), rand::random_range(0..3), receipt),
            4..6 => Step::Unsub(rand::random_range(0..3), receipt),
            6 => Step::Publish(2, n),
            // y는 드물게 발행해서, x가 쏟아지는 동안 y의 마지막 건이 남는지 본다.
            7..10 => Step::Publish(1, n),
            _ => Step::Publish(0, n),
        }
    }

    fn command<Q>(&self) -> Cmd<Q> {
        match self {
            Step::Sub(id, topic, receipt) => {
                Cmd::Subscribe { conn: 0, id: IDS[*id].into(), topic: MIXED[*topic].into(), receipt: receipt.clone() }
            }
            Step::Unsub(id, receipt) => Cmd::Unsubscribe { conn: 0, id: IDS[*id].into(), receipt: receipt.clone() },
            Step::Publish(topic, n) => Cmd::Publish(MIXED[*topic].into(), body(*n)),
        }
    }
}

enum Seen {
    Receipt(String),
    Message(Arc<str>, u64),
}

struct Instance<'a> {
    topic: usize,
    begin: Option<&'a str>,
    end: Option<&'a str>,
    live: bool,
    sent: Vec<u64>,
    got: Vec<(usize, u64)>,
}

// 명령 순서대로 구독을 다시 세우고, 받은 프레임이 그 구독의 규칙을 지키는지 본다.
// 끝까지 살아 있는 최신 우선 구독 중 마지막 발행분을 받지 못한 수와 그런 구독의 수를 돌려준다.
fn verify(log: &[Step], seen: &[Seen], kicked: bool) -> (u64, u64) {
    let mut instances: Vec<Instance> = Vec::new();
    let (mut active, mut owner, mut receipts) = ([None; 3], HashMap::new(), Vec::new());
    for step in log {
        match step {
            Step::Sub(id, _, receipt) | Step::Unsub(id, receipt) => {
                // 같은 id로 다시 구독해도 이전 구독은 끝난다.
                if let Some(i) = active[*id].take() {
                    let ended: &mut Instance = &mut instances[i];
                    (ended.end, ended.live) = (receipt.as_deref(), false);
                }
                receipts.extend(receipt.as_deref());
                if let Step::Sub(_, topic, _) = step {
                    active[*id] = Some(instances.len());
                    let begin = receipt.as_deref();
                    instances.push(Instance { topic: *topic, begin, end: None, live: true, sent: vec![], got: vec![] });
                }
            }
            Step::Publish(topic, n) => {
                for (id, i) in active.iter().enumerate() {
                    if let Some(i) = *i
                        && instances[i].topic == *topic
                    {
                        instances[i].sent.push(*n);
                        owner.insert((id, *n), i);
                    }
                }
            }
        }
    }
    let (mut at, mut delivered) = (HashMap::new(), Vec::new());
    for (p, frame) in seen.iter().enumerate() {
        match frame {
            Seen::Receipt(r) => {
                at.insert(r.as_str(), p);
                delivered.push(r.as_str());
            }
            Seen::Message(id, n) => {
                let id = IDS.iter().position(|known| *known == &**id).expect("아는 구독 id");
                let i = *owner.get(&(id, *n)).expect("받을 구독이 없는 MESSAGE가 나갔다");
                let got = &mut instances[i].got;
                assert!(got.last().is_none_or(|(_, m)| m < n), "한 구독 안에서 순서가 바뀌거나 겹쳤다");
                got.push((p, *n));
            }
        }
    }
    // 끊긴 연결은 그 앞까지만 받는다.
    assert!(receipts.starts_with(&delivered) && (kicked || receipts.len() == delivered.len()), "불변식 3: RECEIPT");
    let (mut stale, mut checked) = (0, 0);
    for instance in &instances {
        if !kicked && !MIXED[instance.topic].starts_with(LATEST_FIRST) {
            assert_eq!(instance.got.len(), instance.sent.len(), "불변식 3: 끊기 정책 topic의 MESSAGE가 빠졌다");
        } else if !kicked && instance.live && !instance.sent.is_empty() {
            checked += 1;
            stale += u64::from(instance.sent.last() != instance.got.last().map(|(_, n)| n));
        }
        let (Some(&(first, _)), Some(&(last, _))) = (instance.got.first(), instance.got.last()) else { continue };
        assert!(instance.begin.is_none_or(|r| at.get(r).is_some_and(|&p| p < first)), "불변식 1");
        assert!(instance.end.and_then(|u| at.get(u)).is_none_or(|&p| last < p), "불변식 2");
    }
    (stale, checked)
}

// 구독, 해지, 발행을 무작위로 섞어 보내고 수신단은 가끔 멈칫해 큐를 채운다.
async fn check<Q: Queue>(name: &str, rounds: u32, steps: u64) {
    let (mut kicked_rounds, mut frames, mut dropped, mut stale, mut checked) = (0, 0, 0, 0, 0);
    for _ in 0..rounds {
        let hub = Mini::<Q>::spawn();
        let (queue, mut rx) = Q::open(64);
        hub.send(Cmd::Connect(0, queue)).await;
        let consumer = tokio::spawn(async move {
            let mut seen = Vec::new();
            while let Some(item) = rx.pop().await {
                match item {
                    Outgoing::Receipt { receipt_id, .. } if receipt_id == "end" => break,
                    Outgoing::Receipt { receipt_id, .. } => seen.push(Seen::Receipt(receipt_id)),
                    Outgoing::Message { subscription, body } => seen.push(Seen::Message(subscription, number(&body))),
                    Outgoing::Error(_) | Outgoing::Denied { .. } | Outgoing::Latest { .. } => {
                        unreachable!("축소판 hub는 ERROR도 구독 거절도 본체의 표지도 만들지 않는다")
                    }
                }
                if rand::random_ratio(1, 50) {
                    spin_until(Instant::now() + Duration::from_micros(rand::random_range(0..300)));
                }
            }
            seen
        });
        // 끝에는 x만 몰아 보내서, 드물게 오는 y의 마지막 건이 밀려나는지 본다.
        let tail = (steps..steps + 300).map(|n| Step::Publish(0, n));
        let log: Vec<Step> = (0..steps).map(Step::random).chain(tail).collect();
        for step in &log {
            hub.send(step.command()).await;
        }
        hub.send(Cmd::Unsubscribe { conn: 0, id: "end".into(), receipt: Some("end".into()) }).await;
        let seen = consumer.await.expect("검사 수신 태스크");
        let (round_dropped, round_kicked) = hub.counts().await;
        let (round_stale, round_checked) = verify(&log, &seen, round_kicked > 0);
        kicked_rounds += u64::from(round_kicked > 0);
        frames += seen.len();
        dropped += round_dropped;
        stale += round_stale;
        checked += round_checked;
    }
    println!(
        "{name:<10} | {rounds} | {kicked_rounds:>3} | {frames:>7} | {dropped:>7} | {stale:>3}/{checked:<4} | 위반 없음"
    );
}

fn latency_row(connections: u64, name: &str, latency: &Latency) {
    let Latency { p50_us, p99_us, cpu_us } = latency;
    println!("{connections:>6} | {name:<10} | {p50_us:>8.1} | {p99_us:>8.1} | {cpu_us:>8.1}");
}

fn stalled_row(connections: u64, name: &str, (latency, seen): (Latency, Option<Unstalled>), published: u64) {
    let seen = seen.expect("멈춘 연결 결과");
    println!(
        "{connections:>6} | {name:<10} | {:>8.1} | {:>8.1} | {} | {:>5} | {:>4}/{published} | {:>9} | {:>5} | {:>6} | {}",
        latency.p50_us,
        latency.p99_us,
        if seen.kicked > 0 { "끊김" } else { "유지" },
        seen.first.map_or("-".into(), |n| n.to_string()),
        seen.hot,
        seen.latest_after.map_or("못 받음".into(), |t| format!("{}us", t.as_micros())),
        if seen.quiet { "받음" } else { "잃음" },
        seen.frames,
        seen.body_bytes,
    );
}

#[tokio::main]
async fn main() {
    let workers = tokio::runtime::Handle::current().metrics().num_workers();
    println!("런타임 워커 {workers}개, 큐 용량 {CAPACITY}, 본문 {}바이트", body(0).len());
    println!(
        "큐 항목 크기: Outgoing {}B, 고리 항목 {}B, 한 칸 방식의 표지 항목 {}B와 구독마다 칸 {}B",
        size_of::<Outgoing>(),
        size_of::<(Outgoing, bool)>(),
        size_of::<Queued>(),
        2 * size_of::<usize>() + size_of::<Mutex<Option<Arc<str>>>>(),
    );
    let sizes = [(1_000, 5_000), (10_000, 2_000)];

    println!("\n정상 경로: 구독자 100%, 발행부터 전원이 받기까지");
    println!("  연결 | 방식       |   p50 us |   p99 us | CPU us/건");
    for (connections, samples) in sizes {
        latency_row(connections, "지금(끊기)", &fanout::<Current>(connections, samples, false).await.0);
        latency_row(connections, "(가) 고리", &fanout::<RingTx>(connections, samples, false).await.0);
        latency_row(connections, "(나) 한 칸", &fanout::<SlotTx>(connections, samples, false).await.0);
        latency_row(connections, "(나) K=64", &fanout::<DepthTx<64>>(connections, samples, false).await.0);
        latency_row(connections, "본체 hub", &fanout_hub(connections, samples).await);
    }

    println!("\n멈춘 수신자 1개: 나머지 연결의 지연, 멈춘 연결이 풀린 뒤 받은 것");
    println!(
        "  연결 | 방식       |   p50 us |   p99 us | 상태 | 첫 hot | hot 받음/발행 | 최신 hot까지 | quiet | 프레임 | 본문 바이트"
    );
    for (connections, samples) in sizes {
        let published = WARMUP + samples;
        stalled_row(connections, "지금(끊기)", fanout::<Current>(connections, samples, true).await, published);
        stalled_row(connections, "(가) 고리", fanout::<RingTx>(connections, samples, true).await, published);
        stalled_row(connections, "(나) 한 칸", fanout::<SlotTx>(connections, samples, true).await, published);
        stalled_row(connections, "(나) K=8", fanout::<DepthTx<8>>(connections, samples, true).await, published);
        stalled_row(connections, "(나) K=64", fanout::<DepthTx<64>>(connections, samples, true).await, published);
    }

    println!("\n밀린 수신자: 종목 3개, 연결 1개, 처리 시간 = 평균 발행 간격 x 비율, 조건마다 8초");
    println!("종목당 초당 | 비율 | 방식       |  발행 |  받음 | 유실 % | 지연 p50 ms | p99 ms | 상태");
    for rate in [5.0, 40.0, 80.0, 120.0] {
        for ratio in [0.2, 0.5, 0.9, 1.1, 2.0] {
            lagging(rate, ratio, 8.0).await;
        }
    }

    println!("\n늦추는 수신자 없음: 연결마다 종목 3개, 조건마다 3초, CPU는 발행하는 동안 프로세스가 쓴 코어 수");
    println!("  연결 | 종목당 초당 | 방식       |  발행 | 받아야 할 건수 |   받음 | 유실 % | 끊긴 연결 | CPU");
    for rate in [5.0, 40.0, 80.0, 120.0] {
        keep_up::<Current>("지금(끊기)", 10_000, rate, 3.0).await;
        keep_up::<RingTx>("(가) 고리", 10_000, rate, 3.0).await;
        keep_up::<SlotTx>("(나) 한 칸", 10_000, rate, 3.0).await;
    }

    // 지금 방식의 순서는 src/hub/tests.rs가 검사한다. 용량 64에서는 첫 멈칫에 끊겨 여기서 볼 것이 없다.
    println!("\n불변식 검사: 큐 용량 64, 라운드마다 명령 4,000개와 x 발행 300개");
    println!("방식       | 라운드 | 끊긴 라운드 | 받은 프레임 | 버린 MESSAGE | 최신분 못 받은 구독 | 결과");
    check::<RingTx>("(가) 고리", 50, 4_000).await;
    check::<SlotTx>("(나) 한 칸", 50, 4_000).await;
    check::<DepthTx<8>>("(나) K=8", 50, 4_000).await;
}
