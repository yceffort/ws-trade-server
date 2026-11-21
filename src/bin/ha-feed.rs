use std::io::Read;
use std::process::ExitCode;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use ws_trade_server::ha::{Bootstrap, Error, Kafka, Lease, LeaseConfig, Publisher, SourceConfig, Synthetic};

struct Args {
    command: String,
    kafka: String,
    source: String,
    lease: Option<String>,
    batches: Option<u64>,
    market_open: bool,
}

fn args() -> Option<Args> {
    let mut args = std::env::args().skip(1);
    let command = args.next()?;
    if !matches!(command.as_str(), "init" | "run" | "audit") {
        return None;
    }
    let mut parsed =
        Args { command, kafka: String::new(), source: String::new(), lease: None, batches: None, market_open: true };
    while let Some(flag) = args.next() {
        let value = args.next()?;
        match flag.as_str() {
            "--kafka" => parsed.kafka = value,
            "--source" => parsed.source = value,
            "--lease" => parsed.lease = Some(value),
            "--batches" => parsed.batches = Some(value.parse::<u64>().ok().filter(|n| *n > 0)?),
            "--market-open" => parsed.market_open = value.parse().ok()?,
            _ => return None,
        }
    }
    (!parsed.kafka.is_empty() && !parsed.source.is_empty() && (parsed.command != "run" || parsed.lease.is_some()))
        .then_some(parsed)
}

fn read<T: serde::de::DeserializeOwned>(path: &str) -> Result<T, Error> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?.take(65537).read_to_end(&mut bytes)?;
    if bytes.len() > 65536 {
        return Err(Error::TooLarge);
    }
    serde_json::from_slice(&bytes).map_err(|_| Error::InvalidConfig)
}

fn now_ms() -> Result<u64, Error> {
    u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| Error::InvalidState)?.as_millis())
        .map_err(|_| Error::Exhausted)
}

fn run(args: Args, stopped: Arc<AtomicBool>) -> Result<(), Error> {
    let kafka: Kafka = read(&args.kafka)?;
    let source: SourceConfig = read(&args.source)?;
    if args.command == "audit" {
        let result = ws_trade_server::ha::audit(&kafka, &source)?;
        println!("{}", serde_json::to_string(&result).map_err(|_| Error::InvalidState)?);
        return Ok(());
    }
    if args.command == "init" {
        let now = now_ms()?;
        let generator = Synthetic::new(&source, 1, now)?;
        Publisher::open(&kafka, source, Bootstrap::Initialize(Some(generator)), now, &mut || Ok(()))?;
        tracing::info!(target: "ws_trade_server::ha_feed", event = "ha_source_initialized");
        return Ok(());
    }
    let mut config: LeaseConfig = read(args.lease.as_deref().ok_or(Error::InvalidConfig)?)?;
    let prefix = std::env::var("POD_UID").unwrap_or(config.identity);
    config.identity = format!("{prefix}-{:016x}", rand::random::<u64>());
    let mut lease = Lease::new(config)?;
    while !lease.acquire()? {
        if stopped.load(Ordering::Relaxed) {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    let lease = lease.into_guard()?;
    let mut guard = || {
        if stopped.load(Ordering::Relaxed) {
            return Err(Error::LeaseLost);
        }
        lease.check()
    };
    let mut publisher = Publisher::open(&kafka, source, Bootstrap::Existing, now_ms()?, &mut guard)?;
    tracing::info!(target: "ws_trade_server::ha_feed", event = "ha_producer_active");
    let mut due = Instant::now();
    let mut batches = 0;
    while !stopped.load(Ordering::Relaxed) {
        publisher.synthetic(if args.market_open { 10 } else { 0 }, now_ms()?, args.market_open, &mut guard)?;
        batches += 1;
        if args.batches.is_some_and(|limit| batches >= limit) {
            break;
        }
        due += Duration::from_millis(if args.market_open { 100 } else { 1000 });
        std::thread::sleep(due.saturating_duration_since(Instant::now()));
    }
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    let _logging = match ws_trade_server::logging::init() {
        Ok(logging) => logging,
        Err(_) => return ExitCode::FAILURE,
    };
    let Some(args) = args() else {
        eprintln!(
            "ha-feed init|run|audit --kafka 파일 --source 파일 [--lease 파일] [--batches 횟수] [--market-open true|false]"
        );
        return ExitCode::from(2);
    };
    let stopped = Arc::new(AtomicBool::new(false));
    let signal = stopped.clone();
    let mut worker = tokio::task::spawn_blocking(move || run(args, stopped));
    let result = tokio::select! {
        result = &mut worker => result,
        _ = shutdown() => { signal.store(true, Ordering::Relaxed); worker.await }
    };
    match result {
        Ok(Ok(())) => ExitCode::SUCCESS,
        Ok(Err(_)) if signal.load(Ordering::Relaxed) => ExitCode::SUCCESS,
        Ok(Err(error)) => {
            tracing::error!(target: "ws_trade_server::ha_feed", event = "ha_producer_stopped", reason = %error);
            ExitCode::FAILURE
        }
        Err(_) => ExitCode::FAILURE,
    }
}

async fn shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = signal(SignalKind::terminate()).expect("SIGTERM 감시 등록");
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
