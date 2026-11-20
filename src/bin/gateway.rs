use ws_trade_server::{Config, default_ring_path, start_managed};

fn parse_args() -> Option<Config> {
    let mut config = Config { ring: Some(default_ring_path()), ..Config::default() };
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let value = args.next()?;
        match flag.as_str() {
            "--max-connections" => config.limits.connections = value.parse().ok()?,
            "--max-subscriptions" => config.limits.subscriptions = value.parse().ok()?,
            "--max-commands-per-sec" => config.limits.commands_per_second = value.parse().ok()?,
            "--allowed-topics" => {
                if value.split(',').any(str::is_empty) {
                    return None;
                }
                config.limits.topics = value.split(',').map(str::to_string).collect();
            }
            "--http-addr" => config.http_addr = Some(value),
            "--snapshot-topics" => config.snapshot_topics = value.split(',').map(str::to_string).collect(),
            "--addr" => config.addr = value,
            "--ring" => config.ring = Some(value.into()),
            "--poll-us" => config.poll_us = value.parse().ok()?,
            "--idle-ms" => {
                let millis = value.parse::<u64>().ok()?;
                config.idle_timeout = (millis > 0).then(|| std::time::Duration::from_millis(millis));
            }
            _ => return None,
        }
    }
    Some(config)
}

#[tokio::main]
async fn main() {
    let Some(config) = parse_args() else {
        eprintln!(
            "사용법: gateway [--addr 127.0.0.1:8080] [--ring 경로] [--poll-us 200] [--idle-ms 0] [--max-connections 0] [--max-subscriptions 0] [--max-commands-per-sec 0] [--allowed-topics topic,...]"
        );
        std::process::exit(2);
    };
    match start_managed(config).await {
        Ok(server) => {
            eprintln!("수신 대기: {}", server.addr);
            if let Some(addr) = server.http_addr {
                eprintln!("조회 API: {addr}");
            }
            wait_for_signal().await;
            server.shutdown().await;
        }
        Err(error) => {
            eprintln!("시작 실패: {error}");
            std::process::exit(1);
        }
    }
}

async fn wait_for_signal() {
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
