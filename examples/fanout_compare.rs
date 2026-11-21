// hub의 색인 방식과 색인 없이 공유 채널(broadcast)로 뿌리고 받는 쪽이 거르는 방식을 같은 조건에서 잰다.
// 사용: cargo run --release --example fanout_compare
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::oneshot::error::TryRecvError;
use tokio::sync::{Notify, broadcast};
use ws_trade_server::hub::{Hub, Outgoing};

const HOT: &str = "/hot";
const COLD: &str = "/cold";

// 한 표본의 도착 집계. 구독자가 받은 수와 깨어나 처리한 연결 수를 따로 센다.
struct Tally {
    subscribers: usize,
    wakeups: usize,
    delivered: AtomicUsize,
    handled: AtomicUsize,
    delivered_at: Mutex<Option<Instant>>,
    delivered_done: Notify,
    handled_done: Notify,
}

impl Tally {
    fn new(subscribers: usize, wakeups: usize) -> Arc<Self> {
        Arc::new(Self {
            subscribers,
            wakeups,
            delivered: AtomicUsize::new(0),
            handled: AtomicUsize::new(0),
            delivered_at: Mutex::new(None),
            delivered_done: Notify::new(),
            handled_done: Notify::new(),
        })
    }

    fn received(&self) {
        if self.delivered.fetch_add(1, Relaxed) + 1 == self.subscribers {
            let received_at = Instant::now();
            *self.delivered_at.lock().expect("수신 시각 잠금") = Some(received_at);
            self.delivered_done.notify_one();
        }
        self.skipped();
    }

    fn skipped(&self) {
        if self.handled.fetch_add(1, Relaxed) + 1 == self.wakeups {
            self.handled_done.notify_one();
        }
    }
}

// ps 출력의 반올림을 피하려고 이 시계 호출에만 unsafe를 허용한다.
#[allow(unsafe_code)]
fn cpu_seconds() -> f64 {
    let mut time = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    // SAFETY: time은 초기화된 timespec이며 호출 동안 유효한 쓰기 가능한 포인터를 넘긴다.
    let result = unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut time) };
    assert_eq!(result, 0, "프로세스 CPU 시계 조회 실패: {}", std::io::Error::last_os_error());
    time.tv_sec as f64 + time.tv_nsec as f64 / 1e9
}

struct Stats {
    median_us: f64,
    p99_us: f64,
    cpu_us: f64,
}

// 발행부터 구독자 전원이 받을 때까지가 한 표본이다. 구독 안 한 연결이 다 처리할 때까지 기다린 뒤 다음 표본으로 넘어간다.
async fn measure(tally: &Tally, samples: usize, publish: impl AsyncFn()) -> Stats {
    let sample = async || {
        let start = Instant::now();
        publish().await;
        tally.delivered_done.notified().await;
        // 측정 태스크가 다시 실행될 때까지의 대기를 전달 지연에 넣지 않는다.
        let elapsed =
            tally.delivered_at.lock().expect("수신 시각 잠금").take().expect("마지막 구독자의 수신 시각") - start;
        tally.handled_done.notified().await;
        tally.delivered.store(0, Relaxed);
        tally.handled.store(0, Relaxed);
        elapsed.as_nanos() as u64
    };
    for _ in 0..20 {
        sample().await;
    }
    let cpu_start = cpu_seconds();
    let mut nanos = Vec::with_capacity(samples);
    for _ in 0..samples {
        nanos.push(sample().await);
    }
    let cpu = cpu_seconds() - cpu_start;
    nanos.sort_unstable();
    let at = |q: f64| nanos[((nanos.len() as f64 * q).ceil() as usize).max(1) - 1] as f64 / 1e3;
    Stats { median_us: at(0.5), p99_us: at(0.99), cpu_us: cpu * 1e6 / samples as f64 }
}

// 구독자는 hot, 나머지는 cold를 구독한다. 연결마다 태스크가 하나고 자기 큐에서 받는다.
async fn run_hub(connections: usize, subscribers: usize, samples: usize) -> Stats {
    let hub = Hub::spawn();
    let tally = Tally::new(subscribers, subscribers);
    for conn in 0..connections as u64 {
        let (mut queue, _kicked) = hub.connect(conn, 1024).await;
        let topic = if (conn as usize) < subscribers { HOT } else { COLD };
        hub.subscribe(conn, "0".into(), topic.into(), None).await;
        let tally = tally.clone();
        tokio::spawn(async move {
            while let Some(Outgoing::Message { .. }) = queue.recv().await {
                tally.received();
            }
        });
    }
    let body: Arc<str> = Arc::from("x".repeat(248));
    // 구독 명령을 hub가 다 처리했는지 확인하고 시작한다.
    hub.metrics().await;
    measure(&tally, samples, async || hub.publish(HOT.into(), body.clone()).await).await
}

async fn run_broadcast(connections: usize, subscribers: usize, samples: usize) -> Stats {
    // 표본이 끝났다는 것은 구독자 전원이 받았다는 뜻이다. 하나라도 빠지면 집계가 끝나지 않고 멈추므로 Lagged나 끊김을 따로 세지 않는다.
    let (shared, _) = broadcast::channel::<(Arc<str>, Arc<str>)>(16);
    let tally = Tally::new(subscribers, connections);
    for conn in 0..connections {
        let mut rx = shared.subscribe();
        let mine: HashSet<Arc<str>> = [Arc::from(if conn < subscribers { HOT } else { COLD })].into();
        let tally = tally.clone();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok((topic, _body)) if mine.contains(&*topic) => tally.received(),
                    Ok(_) => tally.skipped(),
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        unreachable!("표본마다 한 건만 흘려 용량 16을 넘지 않는다")
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }
    let (topic, body): (Arc<str>, Arc<str>) = (HOT.into(), Arc::from("x".repeat(248)));
    measure(&tally, samples, async || _ = shared.send((topic.clone(), body.clone()))).await
}

// 받는 쪽 하나가 멈췄을 때 두 방식이 무엇을 하는지 본다. 용량 4에 10건을 흘린다.
async fn stalled_receiver_demo() {
    let (shared, mut stalled) = broadcast::channel::<u32>(4);
    for n in 0..10 {
        shared.send(n).expect("수신자가 있다");
    }
    let mut kept = Vec::new();
    let lagged = stalled.recv().await;
    while let Ok(n) = stalled.try_recv() {
        kept.push(n);
    }
    println!("broadcast: 첫 recv는 {lagged:?}, 이어서 받은 것 {kept:?}, 채널은 열려 있다");

    let hub = Hub::spawn();
    let (mut stalled, mut kicked) = hub.connect(1, 4).await;
    let (mut healthy, _) = hub.connect(2, 1024).await;
    for conn in [1, 2] {
        hub.subscribe(conn, "0".into(), HOT.into(), None).await;
    }
    for n in 0..10 {
        hub.publish(HOT.into(), Arc::from(n.to_string())).await;
    }
    let metrics = hub.metrics().await.expect("hub가 살아 있다");
    let mut queued = 0;
    while let Ok(Outgoing::Message { .. }) = stalled.try_recv() {
        queued += 1;
    }
    let mut healthy_count = 0;
    while let Ok(Outgoing::Message { .. }) = healthy.try_recv() {
        healthy_count += 1;
    }
    // 끊을 때 hub가 kick 송신단을 버리므로 try_recv는 값 없이 Err(Closed)로 끝난다.
    println!(
        "hub: 큐에 남은 것 {queued}건, 끊김 신호 {}, 끊긴 연결 {}개, 멀쩡한 연결은 {healthy_count}건 받음",
        if matches!(kicked.try_recv(), Err(TryRecvError::Closed)) { "도착" } else { "없음" },
        metrics.slow_disconnections_total,
    );
}

#[tokio::main]
async fn main() {
    let reverse = std::env::args().any(|arg| arg == "--reverse");
    let order = if reverse { ["broadcast", "hub"] } else { ["hub", "broadcast"] };
    println!("런타임 워커 {}개", tokio::runtime::Handle::current().metrics().num_workers());
    println!("CPU 시계: CLOCK_PROCESS_CPUTIME_ID");
    println!("조건별 실행 순서: {} 다음 {}", order[0], order[1]);
    println!("연결 | 구독자 | 방식      | 중앙값 us | p99 us | CPU us/건");
    for connections in [100, 1_000, 10_000] {
        let samples = 5_000_000 / connections;
        for percent in [100, 10, 1] {
            let subscribers = (connections * percent / 100).max(1);
            for name in order {
                let r = if name == "hub" {
                    run_hub(connections, subscribers, samples).await
                } else {
                    run_broadcast(connections, subscribers, samples).await
                };
                println!(
                    "{connections:>5} | {subscribers:>6} | {name:<9} | {:>9.1} | {:>6.1} | {:>9.1}",
                    r.median_us, r.p99_us, r.cpu_us
                );
            }
        }
    }
    stalled_receiver_demo().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_cpu_clock_detects_short_work() {
        let start = cpu_seconds();
        let mut sum = 0u64;
        for n in 0..100_000 {
            sum = std::hint::black_box(sum.wrapping_add(std::hint::black_box(n)));
        }
        std::hint::black_box(sum);
        assert!(cpu_seconds() > start);
    }

    #[tokio::test]
    async fn completion_time_precedes_observer_and_resets_each_sample() {
        let tally = Tally::new(2, 3);
        for _ in 0..2 {
            let start = Instant::now();
            tally.received();
            assert!(tally.delivered_at.lock().unwrap().is_none());
            tally.received();
            let observer_resumed = Instant::now();
            tally.delivered_done.notified().await;
            let received_at = tally.delivered_at.lock().unwrap().take().unwrap();
            assert!((start..=observer_resumed).contains(&received_at));
            assert_eq!(tally.handled.load(Relaxed), 2);
            tally.skipped();
            tally.handled_done.notified().await;
            tally.delivered.store(0, Relaxed);
            tally.handled.store(0, Relaxed);
        }
    }
}
