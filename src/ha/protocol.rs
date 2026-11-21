use super::{CursorKeys, Number};
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::Semaphore;

#[derive(Clone)]
pub struct Protocol {
    pub(crate) counters: Arc<super::metrics::Counters>,
    pub(crate) keys: Arc<CursorKeys>,
    pub(crate) workers: Arc<Semaphore>,
    pub(crate) waiting: Arc<Semaphore>,
}

impl Protocol {
    pub fn ready(&self) -> bool {
        self.keys.ready()
    }

    pub fn new(keys: CursorKeys) -> Result<Self, &'static str> {
        keys.validate()?;
        Ok(Self {
            counters: Arc::default(),
            keys: Arc::new(keys),
            workers: Arc::new(Semaphore::new(32)),
            waiting: Arc::new(Semaphore::new(128)),
        })
    }
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case", deny_unknown_fields)]
pub(super) enum Request {
    Connect { token: String },
    Resume { request_id: String, topics: Vec<String>, mode: Mode, cursor: Option<String>, start_at: Option<StartAt> },
    Heartbeat,
}

#[derive(Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(super) enum Mode {
    State,
    Events,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StartAt {
    pub log_id: String,
    pub partition: u32,
    pub offset: Number,
}

pub(super) struct Resume {
    pub request_id: String,
    pub topics: Vec<String>,
    pub start: Option<u64>,
    pub snapshot: bool,
    pub cursor: bool,
}

impl Resume {
    pub fn parse(
        request: Request,
        protocol: &Protocol,
        log_id: &str,
        allowed: &[String],
        subject: &str,
    ) -> Result<Self, &'static str> {
        let Request::Resume { request_id, topics, mode, cursor, start_at } = request else {
            return Err("invalid-resume");
        };
        if !crate::source::identifier(&request_id)
            || topics.is_empty()
            || topics.len() > 256
            || topics.windows(2).any(|pair| pair[0] >= pair[1])
            || topics.iter().any(|topic| !allowed.contains(topic))
        {
            return Err("forbidden-scope");
        }
        if cursor.is_some() && start_at.is_some()
            || mode == Mode::State && start_at.is_some()
            || mode == Mode::Events && cursor.is_none() && start_at.is_none()
        {
            return Err("invalid-resume");
        }
        let signed = cursor.is_some();
        let start = if let Some(cursor) = cursor {
            Some(protocol.keys.verify(&cursor, log_id, &topics, subject)?.checked_add(1).ok_or("invalid-cursor")?)
        } else if let Some(start) = start_at {
            if start.log_id != log_id || start.partition != 0 || start.offset.0 > i64::MAX as u64 {
                return Err("start-out-of-range");
            }
            Some(start.offset.0)
        } else {
            None
        };
        Ok(Self { request_id, topics, start, snapshot: mode == Mode::State && !signed, cursor: signed })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resume_modes_reject_missing_ambiguous_numeric_and_changed_scope() {
        let keys = serde_json::from_value(serde_json::json!({"current":"test","keys":{"test":{"secret":"BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc","retain_until":u64::MAX}}})).unwrap();
        let protocol = Protocol::new(keys).unwrap();
        let topics = vec!["/topic/v1/test".into()];
        let request = |mode, cursor: Option<String>, start_at| Request::Resume {
            request_id: "test".into(),
            topics: topics.clone(),
            mode,
            cursor,
            start_at,
        };
        assert!(Resume::parse(request(Mode::Events, None, None), &protocol, "log", &topics, "user").is_err());
        assert!(Resume::parse(request(Mode::State, None, None), &protocol, "log", &topics, "user").unwrap().snapshot);
        let start = || Some(StartAt { log_id: "log".into(), partition: 0, offset: Number(5) });
        assert!(Resume::parse(request(Mode::State, None, start()), &protocol, "log", &topics, "user").is_err());
        assert_eq!(
            Resume::parse(request(Mode::Events, None, start()), &protocol, "log", &topics, "user").unwrap().start,
            Some(5)
        );
        let cursor = protocol.keys.sign("log", 5, &topics, "user").unwrap();
        assert!(
            Resume::parse(request(Mode::Events, Some(cursor.clone()), start()), &protocol, "log", &topics, "user")
                .is_err()
        );
        assert_eq!(
            Resume::parse(request(Mode::Events, Some(cursor), None), &protocol, "log", &topics, "user").unwrap().start,
            Some(6)
        );
        assert!(serde_json::from_value::<Request>(serde_json::json!({"type":"resume","request_id":"test","topics":topics,"mode":"events","start_at":{"log_id":"log","partition":0,"offset":5}})).is_err());
    }
}
