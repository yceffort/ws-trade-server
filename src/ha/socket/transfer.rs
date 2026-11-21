use super::super::{LogRecord, MAX_STATE_BYTES, Protocol, ReplicaInput, digest, protocol::Resume};
use super::{Writer, replay, send};
use crate::hub::Hub;
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use std::{sync::atomic::Ordering, time::Duration};
use tokio::time::{Instant, interval};

const BYTES: usize = 10 * 1024 * 1024;
const RESERVE: usize = 16 * 1024;

async fn wait<F: std::future::Future>(
    writer: &mut Writer,
    future: F,
    deadline: Instant,
) -> Result<F::Output, &'static str> {
    tokio::pin!(future);
    let mut heartbeat = interval(Duration::from_secs(1));
    heartbeat.tick().await;
    loop {
        tokio::select! {
            result = &mut future => return Ok(result),
            _ = tokio::time::sleep_until(deadline) => return Err("retry-later"),
            _ = heartbeat.tick() => { send(writer, json!({"type":"heartbeat"}), deadline).await?; },
        }
    }
}

struct Progress<'a> {
    protocol: &'a Protocol,
    log_id: &'a str,
    subject: &'a str,
    resume: &'a Resume,
    position: Option<u64>,
    checkpoint: Option<u64>,
    events: usize,
    bytes: usize,
    total: usize,
    first: bool,
    last: Instant,
}

impl Progress<'_> {
    async fn finish_replay(
        &mut self,
        writer: &mut Writer,
        result: Result<(), &'static str>,
        deadline: Instant,
    ) -> Result<(), &'static str> {
        // 실패한 send의 이벤트가 버퍼에 남을 수 있어 이전 위치의 checkpoint를 뒤에 붙이면 안 된다.
        if matches!(result, Err("slow-client" | "connection-closed")) {
            return result;
        }
        self.checkpoint(writer, deadline).await?;
        result
    }

    fn advance_live(&mut self, position: Option<u64>) {
        if let Some(position) = position.filter(|offset| self.resume.start.is_none_or(|start| *offset >= start)) {
            self.position = Some(self.position.map_or(position, |old| old.max(position)));
        }
    }

    async fn checkpoint(&mut self, writer: &mut Writer, deadline: Instant) -> Result<(), &'static str> {
        if let Some(position) = self.position
            && self.checkpoint != Some(position)
        {
            let cursor = self.protocol.keys.sign(self.log_id, position, &self.resume.topics, self.subject)?;
            self.total += send(writer, json!({"type":"checkpoint","request_id":self.resume.request_id,"cursor":cursor,"offset":position.to_string()}), deadline).await?;
            self.protocol.counters.checkpoints.fetch_add(1, Ordering::Relaxed);
            self.checkpoint = Some(position);
            self.events = 0;
            self.bytes = 0;
            self.last = Instant::now();
        }
        Ok(())
    }

    async fn record(
        &mut self,
        writer: &mut Writer,
        offset: u64,
        record: &LogRecord,
        deadline: Instant,
        limited: bool,
    ) -> Result<(), &'static str> {
        if self.resume.start.is_some_and(|start| offset < start) {
            return Ok(());
        }
        let value = match record {
            LogRecord::Trade(event) => {
                let (topic, body) = event.message();
                if self.resume.topics.binary_search(&topic).is_ok() {
                    Some(
                        json!({"type":"event","request_id":self.resume.request_id,"event_id":event.event_id(),"topic":topic,
                        "position":{"log_id":self.log_id,"partition":0,"offset":offset.to_string()},
                        "body":serde_json::from_str::<Value>(&body).map_err(|_| "source-error")?}),
                    )
                } else {
                    None
                }
            }
            LogRecord::Status(status) => Some(json!({"type":"source-status","status":status})),
        };
        if let Some(value) = value {
            let size = serde_json::to_vec(&value).map_err(|_| "source-error")?.len();
            if limited && self.total + size + RESERVE >= BYTES {
                return Err("replay-limit");
            }
            let bytes = send(writer, value, deadline).await?;
            if limited {
                self.protocol.counters.replay_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
            }
            self.total += bytes;
            self.bytes += bytes;
            if matches!(record, LogRecord::Trade(_)) {
                self.events += 1;
            }
        }
        self.position = Some(offset);
        let first = !self.first && self.events > 0;
        if first || self.events >= 100 || self.bytes >= 256 * 1024 || self.last.elapsed() >= Duration::from_millis(250)
        {
            self.checkpoint(writer, deadline).await?;
            self.first |= first;
        }
        Ok(())
    }
}

pub(super) async fn run(
    writer: &mut Writer,
    hub: &Hub,
    input: &ReplicaInput,
    protocol: &Protocol,
    conn: u64,
    subject: &str,
    resume: Resume,
) -> Result<(), &'static str> {
    protocol.counters.resumes.fetch_add(1, Ordering::Relaxed);
    let started = Instant::now();
    let waiting = protocol.waiting.clone().try_acquire_owned().map_err(|_| "retry-later")?;
    let permit = wait(writer, protocol.workers.clone().acquire_owned(), Instant::now() + Duration::from_secs(10))
        .await
        .map_err(|_| "retry-later")?
        .map_err(|_| "retry-later")?;
    drop(waiting);
    let deadline = Instant::now() + Duration::from_secs(10);
    let data_deadline = deadline - Duration::from_secs(1);
    let kafka = input.kafka.clone();
    let start = resume.start;
    let reader = wait(writer, replay::Reader::open(kafka, start, permit), data_deadline).await?.map_err(|error| {
        if matches!(error, super::super::Error::InvalidState) {
            if resume.cursor { "cursor-expired" } else { "start-out-of-range" }
        } else {
            replay::error(error)
        }
    })?;
    let (low, stable) = reader.bounds();
    if start.is_some_and(|n| n < low || n > stable) {
        return Err(if resume.cursor { "cursor-expired" } else { "start-out-of-range" });
    }
    wait(
        writer,
        async {
            while start.is_some_and(|start| start > 0) {
                let view = hub.replica_view().await.map_err(|_| "source-error")?;
                if start.is_none_or(|start| view.next >= start) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Ok::<_, &'static str>(())
        },
        data_deadline,
    )
    .await??;
    let (view, mut live) = wait(writer, hub.follow_replica(conn, resume.topics.clone()), data_deadline).await??;
    let end = view.next.checked_sub(1).ok_or("retry-later")?;
    let mut progress = Progress {
        protocol,
        log_id: &input.source.log_id,
        subject,
        resume: &resume,
        position: start.and_then(|n| n.checked_sub(1)),
        checkpoint: None,
        events: 0,
        bytes: 0,
        total: 0,
        first: false,
        last: Instant::now(),
    };
    let mut heartbeat = interval(Duration::from_secs(1));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    heartbeat.tick().await;
    if resume.snapshot {
        drop(reader);
        let mut topics = std::collections::BTreeMap::new();
        for topic in &resume.topics {
            let value = match view.snapshot.topics.get(topic).ok_or("forbidden-scope")? {
                Some(latest) => Some(json!({"event_id":latest.event.event_id(),"offset":latest.offset,
                    "received_at_ms":latest.event.received_at_ms().to_string(),"body":serde_json::from_str::<Value>(&latest.event.message().1).map_err(|_| "source-error")?})),
                None => None,
            };
            topics.insert(topic, value);
        }
        let snapshot = json!({"schema_version":1,"log_id":input.source.log_id,"through":end.to_string(),"topics":topics,"status":view.snapshot.status});
        let bytes = serde_json::to_vec(&snapshot).map_err(|_| "snapshot-too-large")?;
        if bytes.len() > MAX_STATE_BYTES {
            return Err("snapshot-too-large");
        }
        let checksum = digest(&bytes);
        let id = format!("snapshot-{conn}-{end}");
        let transmission = async {
            progress.total += send(writer, json!({"type":"snapshot-begin","request_id":resume.request_id,"id":id,"offset":end.to_string(),"total_bytes":bytes.len(),"chunks":bytes.chunks(32768).len()}), data_deadline).await?;
            for (index, chunk) in bytes.chunks(32768).enumerate() {
                progress.total += send(
                    writer,
                    json!({"type":"snapshot-chunk","id":id,"index":index,"data":STANDARD.encode(chunk)}),
                    data_deadline,
                )
                .await?;
            }
            progress.total += send(
            writer,
            json!({"type":"snapshot-end","id":id,"offset":end.to_string(),"total_bytes":bytes.len(),"sha256":checksum}),
            data_deadline,
        )
        .await?;
            Ok::<_, &'static str>(())
        };
        // live 큐가 찼어도 완료된 snapshot 경계를 먼저 저장해야 다음 요청이 replay로 전진한다.
        transmission.await?;
        protocol.counters.snapshot_bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        progress.position = Some(end);
        progress.checkpoint(writer, deadline).await?;
    } else {
        let replay_started = Instant::now();
        let mut replay = replay::Replay::spawn(reader, input.source.clone(), end, data_deadline);
        let mut checkpoint_tick = interval(Duration::from_millis(250));
        let result = loop {
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(data_deadline) => break Err("replay-limit"),
                reason = &mut live.failed => break Err(reason.unwrap_or("source-error")),
                _ = heartbeat.tick() => { progress.total += send(writer, json!({"type":"heartbeat"}), data_deadline).await?; },
                _ = checkpoint_tick.tick() => { progress.checkpoint(writer, data_deadline).await?; },
                item = replay.inbox.recv() => match item {
                    Some(replay::Item::Record(offset, record)) => {
                        if let Err(code) = progress.record(writer, offset, &record, data_deadline, true).await { break Err(code); }
                    }
                    Some(replay::Item::End(position)) => {
                        protocol.counters.replay.0.observe(replay_started.elapsed());
                        progress.position = Some(position);
                        break Ok(());
                    }
                    Some(replay::Item::Failed(code)) => break Err(code),
                    None => break Err("source-error"),
                },
            }
        };
        progress.finish_replay(writer, result, deadline).await?;
    }
    match live.failed.try_recv() {
        Ok(reason) => return Err(if resume.snapshot { "replay-limit" } else { reason }),
        Err(tokio::sync::oneshot::error::TryRecvError::Closed) => return Err("source-error"),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {}
    }
    progress.total +=
        send(writer, json!({"type":"caught-up","request_id":resume.request_id,"offset":end.to_string()}), deadline)
            .await?;
    protocol.counters.caught_up.fetch_add(1, Ordering::Relaxed);
    protocol.counters.catch_up.0.observe(started.elapsed());
    let mut checkpoint_tick = interval(Duration::from_millis(250));
    let mut applied = 0;
    loop {
        tokio::select! {
            biased;
            reason = &mut live.failed => return Err(reason.unwrap_or("source-error")),
            _ = heartbeat.tick() => { send(writer, json!({"type":"heartbeat"}), Instant::now() + Duration::from_secs(1)).await?; },
            _ = checkpoint_tick.tick() => { progress.checkpoint(writer, Instant::now() + Duration::from_secs(1)).await?; },
            item = live.records.recv() => {
                let item = item.ok_or("source-error")?;
                progress.record(writer, item.offset, &item.record, Instant::now() + Duration::from_secs(1), false).await?;
                applied = item.sequence;
                progress.advance_live(live.progress.borrow_and_update().through(applied));
                progress.total = 0;
            },
            observed = live.progress.changed() => {
                observed.map_err(|_| "source-error")?;
                progress.advance_live(live.progress.borrow_and_update().through(applied));
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio_tungstenite::{
        WebSocketStream,
        tungstenite::{Message, protocol::Role},
    };

    #[tokio::test]
    async fn transport_failure_does_not_checkpoint_behind_a_buffered_event() {
        for reason in ["slow-client", "connection-closed", "replay-limit"] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let (client, accepted) =
                tokio::join!(TcpStream::connect(listener.local_addr().unwrap()), listener.accept());
            let socket = WebSocketStream::from_raw_socket(accepted.unwrap().0, Role::Server, None).await;
            let mut client = WebSocketStream::from_raw_socket(client.unwrap(), Role::Client, None).await;
            let (mut writer, _reader) = socket.split();
            let protocol = Protocol::new(serde_json::from_value(json!({"current":"test","keys":{"test":{"secret":base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([7;32]),"retain_until":u64::MAX}}})).unwrap()).unwrap();
            let resume = Resume {
                request_id: "test".into(),
                topics: vec!["a".into()],
                start: Some(5),
                snapshot: false,
                cursor: true,
            };
            let mut progress = Progress {
                protocol: &protocol,
                log_id: "test",
                subject: "test",
                resume: &resume,
                position: Some(5),
                checkpoint: Some(4),
                events: 1,
                bytes: 1,
                total: 1,
                first: true,
                last: Instant::now(),
            };
            if reason != "replay-limit" {
                // send가 enqueue 뒤 flush 중 취소된 상태를 같은 Sink에 만든다.
                writer
                    .feed(Message::text(json!({"type":"event","position":{"offset":"6"}}).to_string()))
                    .await
                    .unwrap();
            }
            assert_eq!(
                progress.finish_replay(&mut writer, Err(reason), Instant::now() + Duration::from_secs(1)).await,
                Err(reason)
            );
            writer.flush().await.unwrap();
            let frame: Value = serde_json::from_str(client.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
            if reason == "replay-limit" {
                assert_eq!(frame["type"], "checkpoint");
                assert_eq!(frame["offset"], "5");
            } else {
                assert_eq!(frame["type"], "event");
                assert!(tokio::time::timeout(Duration::from_millis(50), client.next()).await.is_err());
            }
        }
    }
}
