pub mod auth;
mod bridge;
pub mod connection;
pub mod corrections;
pub mod feed;
#[cfg(feature = "ha")]
pub mod ha;
mod http;
pub mod hub;
pub mod limits;
pub mod loadgen;
pub mod logging;
pub mod metrics;
pub mod pb;
mod region;
pub mod ring;
pub mod source;
pub mod stomp;
pub mod trade;

use hub::Hub;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
mod server;
pub use server::{Server, start_managed};

#[derive(Clone)]
pub struct Config {
    #[cfg(feature = "ha")]
    pub ha: Option<ha::ReplicaInput>,
    #[cfg(feature = "ha")]
    pub ha_protocol: Option<ha::Protocol>,
    #[cfg(feature = "ha")]
    pub ha_lifecycle: Option<ha::Lifecycle>,
    #[cfg(feature = "ha")]
    pub ha_auth: Option<ha::SharedAuth>,
    pub auth: Option<auth::Auth>,
    pub addr: String,
    pub http_addr: Option<String>,
    pub snapshot_topics: Vec<String>,
    pub latest_first_topics: Vec<String>,
    pub latest_first_depth: usize,
    pub limits: limits::Limits,
    pub ring: Option<PathBuf>,
    pub poll_us: u64,
    pub heartbeat_ms: u64,
    pub idle_timeout: Option<Duration>,
    pub shutdown_timeout: Duration,
    pub queue_capacity: usize,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            #[cfg(feature = "ha")]
            ha: None,
            #[cfg(feature = "ha")]
            ha_protocol: None,
            #[cfg(feature = "ha")]
            ha_lifecycle: None,
            #[cfg(feature = "ha")]
            ha_auth: None,
            auth: None,
            addr: "127.0.0.1:8080".to_string(),
            http_addr: None,
            snapshot_topics: Vec::new(),
            latest_first_topics: Vec::new(),
            latest_first_depth: 64,
            limits: limits::Limits::default(),
            ring: None,
            poll_us: 200,
            heartbeat_ms: 5000,
            idle_timeout: None,
            shutdown_timeout: Duration::from_secs(3),
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
    let mut server = start_managed(config).await?;
    // 기존 호출자는 종료 핸들을 소유하지 않는다. 명시적인 종료 요청만 감시한다.
    server.detach();
    Ok((server.addr, server.hub.clone()))
}
