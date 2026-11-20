use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Default)]
pub(super) struct Cache(BTreeMap<String, Option<(Arc<str>, u128)>>);

impl Cache {
    pub fn new(topics: Vec<String>) -> Self {
        Self(topics.into_iter().map(|topic| (topic, None)).collect())
    }

    pub fn record(&mut self, topic: &str, body: Arc<str>) {
        if let Some(entry) = self.0.get_mut(topic) {
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis();
            *entry = Some((body, now));
        }
    }

    pub fn json(&self) -> String {
        let entries: BTreeMap<_, _> = self
            .0
            .iter()
            .filter_map(|(topic, entry)| {
                entry.as_ref().map(|(body, time)| (topic, serde_json::json!({"body": &**body, "updated_at_ms": time})))
            })
            .collect();
        serde_json::to_string(&entries).expect("문자열과 정수만 직렬화한다")
    }
}
