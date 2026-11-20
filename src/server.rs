use crate::{Config, bridge, connection, http, hub::Hub};
use std::{io, net::SocketAddr, sync::Arc};
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};

pub struct Server {
    pub addr: SocketAddr,
    pub http_addr: Option<SocketAddr>,
    pub hub: Hub,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl Server {
    pub async fn shutdown(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }

    pub(crate) fn detach(&mut self) {
        self.stop.take();
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

pub async fn start_managed(config: Config) -> io::Result<Server> {
    if config.limits.connections > Semaphore::MAX_PERMITS || config.queue_capacity == 0 || config.heartbeat_ms == 0 {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "연결 한도, 큐 크기 또는 heartbeat 설정 오류"));
    }
    if !config.latest_first_topics.is_empty() && config.latest_first_depth >= config.queue_capacity {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "최신 우선 깊이는 송신 큐 크기보다 작아야 한다"));
    }
    let listener = TcpListener::bind(&config.addr).await?;
    let addr = listener.local_addr()?;
    let http = match &config.http_addr {
        Some(addr) => Some(http::bind(addr).await?),
        None => None,
    };
    let http_addr = http.as_ref().map(TcpListener::local_addr).transpose()?;
    let hub = Hub::with_latest_first(
        config.limits.clone(),
        config.snapshot_topics.clone(),
        config.latest_first_topics.clone(),
        config.latest_first_depth,
    );
    let bridge = config
        .ring
        .as_ref()
        .map(|path| bridge::spawn(path.clone(), std::time::Duration::from_micros(config.poll_us), hub.clone()));
    let http = http.map(|listener| http::spawn(listener, hub.clone()));
    let auth_reload = config.auth.as_ref().and_then(crate::auth::Auth::reload);
    let (stop, stopped) = oneshot::channel();
    let accept_hub = hub.clone();
    let task = tokio::spawn(async move {
        let (shutdown, receiver) = watch::channel(false);
        let permits = Arc::new(Semaphore::new(config.limits.connections));
        let mut connections = JoinSet::new();
        let mut conn = 0;
        let mut rejects = connection::Rejector::default();
        let stopped = async {
            if stopped.await.is_err() {
                std::future::pending::<()>().await;
            }
        };
        tokio::pin!(stopped);
        loop {
            tokio::select! {
                _ = &mut stopped => break,
                _ = connections.join_next(), if !connections.is_empty() => {},
                accepted = listener.accept() => {
                    let (stream, _) = match accepted {
                        Ok(accepted) => accepted,
                        Err(error) => { tracing::warn!(event = "websocket_accept_failed", kind = ?error.kind()); tokio::time::sleep(std::time::Duration::from_millis(100)).await; continue; }
                    };
                    let permit = if config.limits.connections == 0 { None } else {
                        match permits.clone().try_acquire_owned() {
                            Ok(permit) => Some(permit),
                            Err(_) => {
                                rejects.reject(stream);
                                continue;
                            }
                        }
                    };
                    conn += 1;
                    let _ = stream.set_nodelay(true);
                    let (hub, config, receiver) = (accept_hub.clone(), config.clone(), receiver.clone());
                    connections.spawn(async move {
                        let _permit = permit;
                        connection::serve(stream, hub, config, conn, receiver).await;
                    });
                }
            }
        }
        drop(listener);
        rejects.shutdown().await;
        if let Some(task) = auth_reload {
            task.abort();
            let _ = task.await;
        }
        if let Some(http) = http {
            http.abort();
            let _ = http.await;
        }
        if let Some(bridge) = &bridge {
            bridge.stop();
        }
        shutdown.send_replace(true);
        let drain = async { while connections.join_next().await.is_some() {} };
        if tokio::time::timeout(config.shutdown_timeout, drain).await.is_err() {
            connections.shutdown().await;
        }
        accept_hub.stop().await;
        if let Some(bridge) = bridge {
            bridge.join().await;
        }
    });
    Ok(Server { addr, http_addr, hub, stop: Some(stop), task: Some(task) })
}
