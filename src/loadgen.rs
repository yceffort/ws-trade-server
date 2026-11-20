mod client;
mod stats;

use crate::default_ring_path;
use crate::feed::{Generator, Symbol, default_symbols};
use crate::ring::Writer;
use client::{Outcome, Role, Shared, run as run_client};
use prost::Message;
use serde_json::{Value, json};
use stats::{Stamp, delivery_report, latency_report};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed};
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::{oneshot, watch};

const BATCH: usize = 200;
const RING_CAPACITY: u64 = 1 << 20;
const TRAILING: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub struct Args {
    pub url: String,
    pub ring: PathBuf,
    pub connections: usize,
    pub symbols: Vec<Symbol>,
    pub rate: f64,
    pub stalled: usize,
    pub probes: usize,
    pub warmup: u64,
    pub seconds: u64,
}

impl Default for Args {
    fn default() -> Args {
        Args {
            url: "ws://127.0.0.1:8080/ws".to_string(),
            ring: default_ring_path(),
            connections: 100,
            symbols: default_symbols(),
            rate: 5.0,
            stalled: 0,
            probes: 50,
            warmup: 3,
            seconds: 20,
        }
    }
}

// 발행 시각까지 기다린다. 멈추라는 신호가 오면 false다. 신호는 50ms 안에 보고, 일정이 밀려 기다릴 것이 없을 때도 본다.
fn wait_until(due: Instant, stop: &AtomicBool) -> bool {
    loop {
        if stop.load(Relaxed) {
            return false;
        }
        let Some(left) = due.checked_duration_since(Instant::now()) else {
            return true;
        };
        thread::sleep(left.min(Duration::from_millis(50)));
    }
}

// 종목마다 평균 초당 rate건, 간격은 지수분포다(feed::run과 같다). 쓰기 직전에 시각을 찍어 둔다.
fn write_trades(mut writer: Writer, args: &Args, stop: &AtomicBool) -> std::io::Result<Vec<Stamp>> {
    let wait = || Duration::from_secs_f64(-(1.0 - rand::random::<f64>()).ln() / args.rate);
    let mut schedule: Vec<(Instant, Generator)> =
        args.symbols.iter().map(|symbol| (Instant::now() + wait(), Generator::new(symbol.clone()))).collect();
    let mut stamps = Vec::with_capacity(
        (args.rate * args.symbols.len() as f64 * (args.warmup + args.seconds + 2) as f64 * 1.2) as usize,
    );
    for seq in 1.. {
        let next = (0..schedule.len()).min_by_key(|&i| schedule[i].0).expect("종목은 하나 이상이다");
        if !wait_until(schedule[next].0, stop) {
            return Ok(stamps);
        }
        schedule[next].0 += wait();
        let trade = schedule[next].1.next_trade(seq);
        let payload = trade.encode_to_vec();
        let written = Instant::now();
        writer.write(&payload)?;
        stamps.push(Stamp { symbol: next, cumulative_volume: trade.cumulative_volume, written });
    }
    unreachable!("seq 범위가 끝나지 않는다")
}

// 상태 표시용 한 줄. bench/run.sh가 이 줄로 측정 구간을 안다.
fn phase(name: &str) {
    eprintln!("phase={name}");
}

fn role_of(index: usize, args: &Args) -> Role {
    if index < args.stalled {
        return Role::Stalled;
    }
    // 탐침을 연결 순서 전체에 고르게 흩뿌린다. 한쪽에 몰리면 hub가 팬아웃하는 순서 차이가 지연에 섞인다.
    let (j, m) = (index - args.stalled, args.connections - args.stalled);
    if (j + 1) * args.probes / m > j * args.probes / m { Role::Probe } else { Role::Normal }
}

pub async fn run(args: Args) -> Result<Value, String> {
    let writer = Writer::open(&args.ring, RING_CAPACITY).map_err(|e| format!("링 열기 실패: {e}"))?;
    let shared = Arc::new(Shared {
        url: args.url.clone(),
        topics: args.symbols.iter().map(Symbol::topic).collect(),
        codes: args.symbols.iter().map(|symbol| symbol.code.clone()).collect(),
        phase: AtomicUsize::new(0),
    });
    let (stop_tx, stop_rx) = watch::channel(false);

    let connect_started = Instant::now();
    let (mut tasks, mut failed, mut first_error) = (Vec::new(), 0, None);
    let indexes: Vec<usize> = (0..args.connections).collect();
    for batch in indexes.chunks(BATCH) {
        let mut readiness = Vec::new();
        for &index in batch {
            let (ready, ready_rx) = oneshot::channel();
            let (shared, stop) = (shared.clone(), stop_rx.clone());
            let role = role_of(index, &args);
            tasks.push(tokio::spawn(async move { run_client(&shared, index, role, ready, stop).await }));
            readiness.push(ready_rx);
        }
        for ready in readiness {
            if let Ok(Err(error)) = ready.await {
                failed += 1;
                first_error.get_or_insert(error);
            }
        }
    }
    let connect_millis = connect_started.elapsed().as_millis();
    if let Some(error) = &first_error {
        eprintln!("연결 실패 {failed}건, 첫 오류: {error}");
    }

    let stop_writer = Arc::new(AtomicBool::new(false));
    let writing = {
        let (stop, args) = (stop_writer.clone(), args.clone());
        thread::spawn(move || write_trades(writer, &args, &stop))
    };
    phase("warmup");
    tokio::time::sleep(Duration::from_secs(args.warmup)).await;
    // phase를 먼저 바꾸고 시각을 잰다. 구간 안에 쓴 체결은 모두 그 뒤에 도착하므로 탐침이 빠짐없이 기록한다.
    shared.phase.store(1, Relaxed);
    let start = Instant::now();
    phase("measure");
    tokio::time::sleep(Duration::from_secs(args.seconds)).await;
    let stop = Instant::now();
    shared.phase.store(2, Relaxed);
    stop_writer.store(true, Relaxed);
    phase("trailing");
    tokio::time::sleep(TRAILING).await;
    let _ = stop_tx.send(true);

    let stamps = writing.join().expect("쓰기 스레드는 패닉하지 않는다").map_err(|e| format!("링 쓰기 실패: {e}"))?;
    let mut outcomes: Vec<(Role, Outcome)> = Vec::new();
    for task in tasks {
        outcomes.extend(task.await.expect("클라이언트 태스크는 패닉하지 않는다"));
    }
    Ok(report(&args, &stamps, &outcomes, (start, stop), (failed, connect_millis)))
}

fn report(
    args: &Args,
    stamps: &[Stamp],
    outcomes: &[(Role, Outcome)],
    window: (Instant, Instant),
    (failed, connect_millis): (usize, u128),
) -> Value {
    let ready = |role| outcomes.iter().filter(|(r, _)| *r == role).count();
    let (normal, probes, stalled) = (ready(Role::Normal), ready(Role::Probe), ready(Role::Stalled));
    let sum = |phase: usize| outcomes.iter().map(|(_, o)| o.messages[phase]).sum::<u64>();
    let (warmup, measured, trailing) = (sum(0), sum(1), sum(2));
    let written_measured = stamps.iter().filter(|s| s.written >= window.0 && s.written < window.1).count();
    let per_sec = |n: u64| n as f64 / args.seconds as f64;
    let probe_outcomes = || outcomes.iter().filter(|(role, _)| *role == Role::Probe).map(|(_, o)| o);
    let samples = || probe_outcomes().flat_map(|o| o.samples.iter());
    let unparsed = probe_outcomes().map(|o| o.unparsed).sum();
    json!({
        "config": {
            "url": args.url, "ring": args.ring, "connections": args.connections,
            "symbols": args.symbols.iter().map(Symbol::topic).collect::<Vec<_>>(), "rate": args.rate,
            "stalled": args.stalled, "probes": args.probes, "warmup": args.warmup, "seconds": args.seconds,
        },
        "connect": {"ready": normal + probes + stalled, "failed": failed, "millis": connect_millis},
        "clients": {"normal": normal, "probes": probes, "stalled": stalled},
        "written": {"total": stamps.len(), "measured": written_measured, "per_sec": per_sec(written_measured as u64)},
        "messages": {"total": warmup + measured + trailing, "measured": measured, "per_sec": per_sec(measured)},
        // 본문을 읽어 쓴 기록과 짝짓는 것은 탐침뿐이라 전달 비율은 탐침으로 낸다.
        "delivery": delivery_report(stamps, samples(), probes, window),
        "seq_gap_clients": outcomes.iter().filter(|(_, o)| o.seq_gap).count(),
        "unexpected_disconnects": outcomes.iter().filter(|(_, o)| o.disconnected).count(),
        "stalled_kicked": outcomes.iter().filter(|(_, o)| o.kicked).count(),
        "latency_us": latency_report(stamps, samples(), unparsed, window),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // 발행률이 처리량보다 높으면 다음 발행 시각이 계속 과거다. 그때도 멈추라는 신호를 봐야 실험이 끝난다.
    #[test]
    fn stop_is_seen_when_the_schedule_is_behind() {
        let due = Instant::now();
        thread::sleep(Duration::from_millis(1));
        assert!(!wait_until(due, &AtomicBool::new(true)));
    }
}
