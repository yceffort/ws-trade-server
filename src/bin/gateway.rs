use std::process::ExitCode;
use ws_trade_server::{Config, default_ring_path, logging, start_managed};

fn parse_args() -> Option<Config> {
    let mut config = Config { ring: Some(default_ring_path()), ..Config::default() };
    let mut args = std::env::args().skip(1);
    #[cfg(feature = "ha")]
    let (mut kafka, mut source) = (None, None);
    #[cfg(feature = "ha")]
    let mut explicit_ring = false;
    while let Some(flag) = args.next() {
        let value = args.next()?;
        match flag.as_str() {
            #[cfg(feature = "ha")]
            "--ha-kafka" => {
                kafka = Some(read_ha_config(&value)?);
                config.ring = None;
            }
            #[cfg(feature = "ha")]
            "--ha-source" => source = Some(read_ha_config(&value)?),
            #[cfg(feature = "ha")]
            "--ha-cursor-keys" => {
                config.ha_protocol = Some(ws_trade_server::ha::Protocol::new(read_ha_config(&value)?).ok()?)
            }
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
            "--auth-file" => {
                config.auth = Some(
                    ws_trade_server::auth::Auth::from_file(value.into()).map_err(|error| tracing::error!(target: "ws_trade_server::gateway", event = "auth_config_rejected", kind = ?error.kind())).ok()?,
                );
            }
            "--addr" => config.addr = value,
            "--ring" => {
                config.ring = Some(value.into());
                #[cfg(feature = "ha")]
                {
                    explicit_ring = true;
                }
            }
            "--poll-us" => config.poll_us = value.parse().ok()?,
            "--idle-ms" => {
                let millis = value.parse::<u64>().ok()?;
                config.idle_timeout = (millis > 0).then(|| std::time::Duration::from_millis(millis));
            }
            _ => return None,
        }
    }
    #[cfg(feature = "ha")]
    match (kafka, source) {
        (Some(kafka), Some(source)) => {
            if explicit_ring {
                return None;
            }
            config.ha = Some(ws_trade_server::ha::ReplicaInput { kafka, source });
        }
        (None, None) => {}
        _ => return None,
    }
    Some(config)
}

#[cfg(feature = "ha")]
fn read_ha_config<T: serde::de::DeserializeOwned>(path: &str) -> Option<T> {
    use std::io::Read;
    let mut bytes = Vec::new();
    std::fs::File::open(path).ok()?.take(65537).read_to_end(&mut bytes).ok()?;
    if bytes.len() > 65536 {
        return None;
    }
    serde_json::from_slice(&bytes).ok()
}

#[tokio::main]
async fn main() -> ExitCode {
    let logging = match logging::init() {
        Ok(logging) => logging,
        Err(error) => {
            eprintln!("로그 초기화 실패: {error}");
            return ExitCode::FAILURE;
        }
    };
    let Some(config) = parse_args() else {
        tracing::error!(target: "ws_trade_server::gateway", event = "invalid_arguments", help = "gateway 옵션은 README.md 참고");
        return ExitCode::from(2);
    };
    match start_managed(config).await {
        Ok(server) => {
            tracing::info!(target: "ws_trade_server::gateway", event = "gateway_started", addr = %server.addr, http_addr = ?server.http_addr);
            let reporter = logging.reporter(server.hub.clone());
            wait_for_signal().await;
            tracing::info!(target: "ws_trade_server::gateway", event = "shutdown_requested");
            tokio::join!(server.shutdown(), reporter.finish());
            tracing::info!(target: "ws_trade_server::gateway", event = "shutdown_complete");
            ExitCode::SUCCESS
        }
        Err(error) => {
            tracing::error!(target: "ws_trade_server::gateway", event = "gateway_start_failed", kind = ?error.kind());
            ExitCode::FAILURE
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
