use crate::{Config, bridge, connection, http, hub::Hub};
use std::{io, net::SocketAddr, sync::Arc};
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};

pub struct Server {
    pub addr: SocketAddr,
    pub http_addr: Option<SocketAddr>,
    pub hub: Hub,
    #[cfg(feature = "ha")]
    pub lifecycle: Option<crate::ha::Lifecycle>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl Server {
    #[cfg(feature = "ha")]
    pub async fn wait_ready(&self, deadline: std::time::Duration) -> io::Result<()> {
        tokio::time::timeout(deadline, async {
            while self.lifecycle.as_ref().is_some_and(|life| !life.ready()) {
                if self.lifecycle.as_ref().is_some_and(|life| life.phase() == crate::ha::Phase::Stopped) {
                    return Err(io::Error::other("gateway-stopped"));
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
            Ok(())
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "gateway-not-ready"))?
    }

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
    #[cfg(feature = "ha")]
    let config = {
        let mut config = config;
        if let Some(input) = &config.ha {
            if input.kafka.timeout_ms > 10000 {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "HA broker 호출은 10초 이내여야 한다"));
            }
            if config.ha_lifecycle.is_none() {
                config.ha_lifecycle = Some(crate::ha::Lifecycle::default());
            }
        }
        if config.ha_protocol.is_some() && config.limits.connections == 0 {
            config.limits.connections = 10000;
        }
        config
    };
    #[cfg(feature = "ha")]
    if config.ha_protocol.is_some() && (config.ha.is_none() || config.auth.is_none() && config.ha_auth.is_none()) {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "HA 프로토콜에는 broker 입력과 인증이 필요하다"));
    }
    #[cfg(feature = "ha")]
    if config.ha_auth.is_some() && (config.auth.is_some() || config.ha.is_none() || config.ha_protocol.is_none()) {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "공유 인증은 HA 프로토콜에서 단독으로 설정한다"));
    }
    #[cfg(feature = "ha")]
    if config.ha.is_some() && config.ring.is_some() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "HA와 ring 입력은 함께 사용할 수 없다"));
    }
    if config.limits.connections > Semaphore::MAX_PERMITS || config.queue_capacity == 0 || config.heartbeat_ms == 0 {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "연결 한도, 큐 크기 또는 heartbeat 설정 오류"));
    }
    let listener = TcpListener::bind(&config.addr).await?;
    let addr = listener.local_addr()?;
    let http = match &config.http_addr {
        Some(addr) => Some(http::bind(addr).await?),
        None => None,
    };
    let http_addr = http.as_ref().map(TcpListener::local_addr).transpose()?;
    let hub = Hub::configured(config.limits.clone(), config.snapshot_topics.clone());
    #[cfg(feature = "ha")]
    let replica = config.ha.clone().map(|input| crate::ha::ReplicaTask::spawn(input, hub.clone()));
    #[cfg(feature = "ha")]
    let lifecycle = config.ha_lifecycle.clone();
    let bridge = config
        .ring
        .as_ref()
        .map(|path| bridge::spawn(path.clone(), std::time::Duration::from_micros(config.poll_us), hub.clone()));
    let http = http.map(|listener| {
        http::spawn(
            listener,
            hub.clone(),
            #[cfg(feature = "ha")]
            lifecycle.clone().map(|lifecycle| crate::ha::Management {
                lifecycle,
                protocol: config.ha_protocol.clone(),
                auth: config.ha_auth.clone(),
            }),
        )
    });
    let auth_reload = config.auth.as_ref().and_then(crate::auth::Auth::reload);
    #[cfg(feature = "ha")]
    let policy_reload = config.ha_auth.as_ref().map(crate::ha::SharedAuth::spawn);
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
        #[cfg(feature = "ha")]
        let mut stop_received = false;
        #[cfg(not(feature = "ha"))]
        let stop_received = false;
        let mut health_tick = tokio::time::interval(std::time::Duration::from_millis(100));
        loop {
            tokio::select! {
                _ = &mut stopped, if !stop_received => {
                    #[cfg(feature = "ha")]
                    if let Some(life) = &config.ha_lifecycle { stop_received = true; life.drain(); continue; }
                    break;
                },
                _ = health_tick.tick(), if cfg!(feature = "ha") => {
                    #[cfg(feature = "ha")]
                    if let Some(life) = &config.ha_lifecycle {
                        if replica.is_some() {
                            let dependencies_ready = config.ha_auth.as_ref().is_none_or(crate::ha::SharedAuth::fresh)
                                && config.ha_protocol.as_ref().is_none_or(crate::ha::Protocol::ready);
                            let (ready, failed) = accept_hub.replica_health(dependencies_ready);
                            life.health(ready, failed || replica.as_ref().is_some_and(crate::ha::ReplicaTask::failed));
                        }
                        if let Some(deadline) = life.close_deadline() {
                            if tokio::time::Instant::now() >= deadline { break; }
                            if life.flush_started() { shutdown.send_replace(true); }
                        }
                    }
                },
                _ = connections.join_next(), if !connections.is_empty() => {},
                accepted = listener.accept() => {
                    let (stream, _) = match accepted {
                        Ok(accepted) => accepted,
                        Err(error) => { tracing::warn!(event = "websocket_accept_failed", kind = ?error.kind()); tokio::time::sleep(std::time::Duration::from_millis(100)).await; continue; }
                    };
                    #[cfg(feature = "ha")]
                    if config.ha_lifecycle.as_ref().is_some_and(|life| !life.ready()) {
                        rejects.reject(stream);
                        continue;
                    }
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
        shutdown.send_replace(true);
        let flush_timeout = config.shutdown_timeout;
        #[cfg(feature = "ha")]
        let flush_timeout = if config.ha_lifecycle.is_some() {
            flush_timeout.min(std::time::Duration::from_secs(3))
        } else {
            flush_timeout
        };
        let drain = async { while connections.join_next().await.is_some() {} };
        if tokio::time::timeout(flush_timeout, drain).await.is_err() {
            connections.shutdown().await;
        }
        #[cfg(feature = "ha")]
        if let Some(task) = policy_reload {
            task.abort();
            let _ = task.await;
        }
        if let Some(task) = auth_reload {
            task.abort();
            let _ = task.await;
        }
        if let Some(bridge) = &bridge {
            bridge.stop();
        }
        #[cfg(feature = "ha")]
        if let Some(replica) = &replica {
            replica.stop();
        }
        accept_hub.stop().await;
        #[cfg(feature = "ha")]
        if let Some(replica) = replica {
            let _ = replica.join().await;
        }
        if let Some(bridge) = bridge {
            bridge.join().await;
        }
        #[cfg(feature = "ha")]
        if let Some(life) = &config.ha_lifecycle {
            life.stop();
        }
        if let Some(http) = http {
            http.abort();
            let _ = http.await;
        }
    });
    Ok(Server {
        addr,
        http_addr,
        hub,
        #[cfg(feature = "ha")]
        lifecycle,
        stop: Some(stop),
        task: Some(task),
    })
}
