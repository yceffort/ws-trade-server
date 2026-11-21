use super::{Cache, Hub, State};
use crate::ha::{Error, LogRecord, Replica, ReplicaView, Snapshot, SourceConfig};
use tokio::sync::oneshot;

pub(super) enum Command {
    Restore(SourceConfig, Box<Snapshot>, oneshot::Sender<Result<(), Error>>),
    Apply(u64, LogRecord, oneshot::Sender<Result<bool, Error>>),
    Boundary(u64, u64, oneshot::Sender<Result<(), Error>>),
    Observed(u64, u64, std::time::Instant, oneshot::Sender<Result<(), Error>>),
    View(oneshot::Sender<Result<ReplicaView, Error>>),
    Follow(u64, Vec<String>, oneshot::Sender<Result<(ReplicaView, crate::ha::LiveStream), &'static str>>),
    Unfollow(u64),
    Fail,
}

impl State {
    fn clear_replica_clients(&mut self) {
        self.live.fail_all("source-stale");
        for client in self.clients.values_mut() {
            if let Some(kick) = client.kick.take() {
                let _ = kick.send("source-stale");
            }
        }
        self.clients.clear();
        self.index.clear();
    }
    pub(super) fn replica(&mut self, command: Command) {
        match command {
            Command::Restore(config, snapshot, reply) => {
                let result = (|| {
                    if self.replica.as_ref().is_some_and(|replica| !replica.view().failed)
                        || self.replica.is_none() && !self.clients.is_empty()
                    {
                        return Err(Error::InvalidState);
                    }
                    let replica = Replica::restore(config.clone(), *snapshot)?;
                    self.clear_replica_clients();
                    let view = replica.view();
                    self.cache = Cache::new(config.topics);
                    for (topic, latest) in &view.snapshot.topics {
                        if let Some(latest) = latest {
                            self.cache.restore(topic, latest.event.message().1, latest.event.received_at_ms());
                        }
                    }
                    self.replica = Some(replica);
                    Ok(())
                })();
                let _ = reply.send(result);
            }
            Command::Apply(offset, record, reply) => {
                let result =
                    self.replica.as_mut().ok_or(Error::InvalidState).and_then(|replica| replica.apply(offset, &record));
                if matches!(result, Ok(true))
                    && let LogRecord::Trade(event) = &record
                {
                    let (topic, body) = event.message();
                    self.publish(&topic, body.clone());
                    self.cache.restore(&topic, body, event.received_at_ms());
                }
                if result.is_err()
                    && let Some(replica) = &mut self.replica
                {
                    replica.fail();
                }
                if matches!(result, Ok(true)) {
                    self.live.publish(offset, record);
                }
                let _ = reply.send(result);
            }
            Command::Boundary(next, log_end, reply) => {
                let result = self
                    .replica
                    .as_mut()
                    .ok_or(Error::InvalidState)
                    .and_then(|replica| replica.boundary(next, log_end));
                let _ = reply.send(result);
            }
            Command::Observed(next, log_end, at, reply) => {
                let result = self.replica.as_mut().ok_or(Error::InvalidState).and_then(|replica| {
                    replica.boundary(next, log_end)?;
                    replica.observed(at);
                    Ok(())
                });
                let _ = reply.send(result);
            }
            Command::View(reply) => {
                let _ = reply.send(self.replica.as_ref().map(Replica::view).ok_or(Error::InvalidState));
            }
            Command::Follow(id, topics, reply) => {
                let result = self.replica.as_ref().map(Replica::view).ok_or("retry-later").and_then(|view| {
                    if !view.ready(true) {
                        return Err("source-stale");
                    }
                    if topics.iter().any(|topic| !view.snapshot.topics.contains_key(topic)) {
                        return Err("forbidden-scope");
                    }
                    Ok((view, self.live.attach(id, topics)?))
                });
                let _ = reply.send(result);
            }
            Command::Unfollow(id) => self.live.detach(id),
            Command::Fail => {
                self.clear_replica_clients();
                if let Some(replica) = &mut self.replica {
                    replica.fail();
                }
            }
        }
    }
}

impl Hub {
    pub(crate) fn replica_ready(&self) -> bool {
        self.replica_health(true).0
    }

    pub(crate) fn replica_health(&self, dependencies_ready: bool) -> (bool, bool) {
        if self.replica_status.has_changed().is_err() {
            return (false, true);
        }
        self.replica_status
            .borrow()
            .as_ref()
            .map_or((false, false), |view| (view.ready(dependencies_ready), view.failed))
    }

    pub async fn follow_replica(
        &self,
        id: u64,
        topics: Vec<String>,
    ) -> Result<(ReplicaView, crate::ha::LiveStream), &'static str> {
        let (reply, applied) = oneshot::channel();
        self.replica_command(Command::Follow(id, topics, reply)).await.map_err(|_| "retry-later")?;
        applied.await.map_err(|_| "retry-later")?
    }

    pub fn unfollow_replica(&self, id: u64) {
        let _ = self.commands.try_send(super::Command::Replica(Box::new(Command::Unfollow(id))));
    }
    async fn replica_command(&self, command: Command) -> Result<(), Error> {
        self.commands.send(super::Command::Replica(Box::new(command))).await.map_err(|_| Error::ReplicaFailed)
    }

    pub async fn restore_replica(&self, config: SourceConfig, snapshot: Snapshot) -> Result<(), Error> {
        let (reply, applied) = oneshot::channel();
        self.replica_command(Command::Restore(config, Box::new(snapshot), reply)).await?;
        applied.await.map_err(|_| Error::ReplicaFailed)?
    }

    pub async fn apply_replica(&self, offset: u64, record: LogRecord) -> Result<bool, Error> {
        let (reply, applied) = oneshot::channel();
        self.replica_command(Command::Apply(offset, record, reply)).await?;
        applied.await.map_err(|_| Error::ReplicaFailed)?
    }

    pub async fn replica_boundary(&self, next: u64, log_end: u64) -> Result<(), Error> {
        let (reply, applied) = oneshot::channel();
        self.replica_command(Command::Boundary(next, log_end, reply)).await?;
        applied.await.map_err(|_| Error::ReplicaFailed)?
    }

    pub(crate) async fn replica_observation(
        &self,
        next: u64,
        log_end: u64,
        at: std::time::Instant,
    ) -> Result<(), Error> {
        let (reply, applied) = oneshot::channel();
        self.replica_command(Command::Observed(next, log_end, at, reply)).await?;
        applied.await.map_err(|_| Error::ReplicaFailed)?
    }

    pub async fn replica_view(&self) -> Result<ReplicaView, Error> {
        let (reply, applied) = oneshot::channel();
        self.replica_command(Command::View(reply)).await?;
        applied.await.map_err(|_| Error::ReplicaFailed)?
    }

    pub(crate) async fn fail_replica(&self) {
        let _ = self.replica_command(Command::Fail).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn cached_freshness_reads_do_not_wait_for_the_actor() {
        let config = SourceConfig {
            log_id: "test-log".into(),
            source_id: crate::source::SourceId::new("demo".into(), "trades".into()).unwrap(),
            epoch: "epoch-1".into(),
            start_seq: crate::ha::Number(1),
            topics: vec!["/topic/v1/kr/stock/trade/DEMO".into()],
        };
        let mut snapshot = Snapshot::empty(&config).unwrap();
        snapshot
            .apply(
                &LogRecord::Status(crate::ha::SourceStatus::new(
                    &config,
                    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64,
                    false,
                )),
                0,
                &config,
            )
            .unwrap();
        let hub = Hub::spawn();
        hub.restore_replica(config, snapshot).await.unwrap();
        hub.replica_observation(1, 1, std::time::Instant::now()).await.unwrap();
        let (entered, paused) = oneshot::channel();
        let (resume, resumed) = oneshot::channel();
        hub.commands.send(crate::hub::Command::Pause { entered, resume: resumed }).await.unwrap();
        paused.await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
        for _ in 0..10000 {
            assert_eq!(hub.replica_health(true), (true, false));
            assert_eq!(hub.replica_health(false), (false, false));
        }
        resume.send(()).unwrap();
        hub.stop().await;
        assert_eq!(hub.replica_health(true), (false, true));
    }
}
