mod bridge;
pub mod connection;
pub mod feed;
pub mod hub;
pub mod limits;
pub mod loadgen;
pub mod pb;
mod region;
pub mod ring;
pub mod stomp;
pub mod trade;

use hub::Hub;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use tokio::net::TcpListener;

#[derive(Clone)]
pub struct Config {
    pub addr: String,
    pub limits: limits::Limits,
    pub ring: Option<PathBuf>,
    pub poll_us: u64,
    pub heartbeat_ms: u64,
    pub idle_timeout: Option<Duration>,
    pub queue_capacity: usize,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            addr: "127.0.0.1:8080".to_string(),
            limits: limits::Limits::default(),
            ring: None,
            poll_us: 200,
            heartbeat_ms: 5000,
            idle_timeout: None,
            queue_capacity: 1024,
        }
    }
}

// feed와 gateway가 같이 쓰는 --ring 기본값.
pub fn default_ring_path() -> PathBuf {
    std::env::temp_dir().join("ws-trade-feed.ring")
}

// 실제로 바인드된 주소와 hub 핸들을 돌려준다. ring이 없으면 브리지를 띄우지 않으므로 테스트는 hub에 직접 발행한다.
pub async fn start(config: Config) -> std::io::Result<(SocketAddr, Hub)> {
    if config.limits.connections > tokio::sync::Semaphore::MAX_PERMITS {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "연결 한도가 너무 크다"));
    }
    let permits = std::sync::Arc::new(tokio::sync::Semaphore::new(config.limits.connections));
    let listener = TcpListener::bind(&config.addr).await?;
    let addr = listener.local_addr()?;
    let hub = Hub::with_limits(config.limits.clone());
    if let Some(ring) = &config.ring {
        bridge::spawn(ring.clone(), Duration::from_micros(config.poll_us), hub.clone());
    }

    let accept_hub = hub.clone();
    tokio::spawn(async move {
        let mut conn = 0;
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let permit = if config.limits.connections == 0 {
                        None
                    } else {
                        match permits.clone().try_acquire_owned() {
                            Ok(permit) => Some(permit),
                            Err(_) => {
                                connection::reject_overloaded(stream).await;
                                continue;
                            }
                        }
                    };
                    conn += 1;
                    // 작은 프레임을 하나씩 flush하므로 Nagle이 켜져 있으면 상대의 지연 ACK를 기다린다. 실패해도 지연만 늘어난다.
                    let _ = stream.set_nodelay(true);
                    let (hub, config) = (accept_hub.clone(), config.clone());
                    tokio::spawn(async move {
                        let _permit = permit;
                        connection::serve(stream, hub, config, conn).await;
                    });
                }
                Err(error) => {
                    eprintln!("accept 실패: {error}");
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }
    });
    Ok((addr, hub))
}
