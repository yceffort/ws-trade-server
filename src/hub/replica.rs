use super::{Cache, Hub, State};
use crate::ha::{Error, LogRecord, Replica, ReplicaView, Snapshot, SourceConfig};
use tokio::sync::oneshot;

pub(super) enum Command {
    Restore(SourceConfig, Box<Snapshot>, oneshot::Sender<Result<(), Error>>),
    Apply(u64, LogRecord, oneshot::Sender<Result<bool, Error>>),
    Boundary(u64, u64, oneshot::Sender<Result<(), Error>>),
    View(oneshot::Sender<Result<ReplicaView, Error>>),
    Fail,
}

impl State {
    pub(super) fn replica(&mut self, command: Command) {
        match command {
            Command::Restore(config, snapshot, reply) => {
                let result = (|| {
                    if self.replica.is_some() || !self.clients.is_empty() {
                        return Err(Error::InvalidState);
                    }
                    let replica = Replica::restore(config.clone(), *snapshot)?;
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
            Command::View(reply) => {
                let _ = reply.send(self.replica.as_ref().map(Replica::view).ok_or(Error::InvalidState));
            }
            Command::Fail => {
                if let Some(replica) = &mut self.replica {
                    replica.fail();
                }
            }
        }
    }
}

impl Hub {
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

    pub async fn replica_view(&self) -> Result<ReplicaView, Error> {
        let (reply, applied) = oneshot::channel();
        self.replica_command(Command::View(reply)).await?;
        applied.await.map_err(|_| Error::ReplicaFailed)?
    }

    pub(crate) async fn fail_replica(&self) {
        let _ = self.replica_command(Command::Fail).await;
    }
}
