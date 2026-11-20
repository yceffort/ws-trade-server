use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Default)]
pub(super) struct Cache(BTreeMap<String, Option<(Arc<str>, u128)>>);

impl Cache {
    pub fn new(topics: Vec<String>) -> Self {
        // 조회 API는 인증이 없으므로 개인 topic은 지정해도 보관하지 않는다.
        let public = topics.into_iter().filter(|topic| super::personal_owner(topic).is_none());
        Self(public.map(|topic| (topic, None)).collect())
    }

    pub fn record(&mut self, topic: &str, body: Arc<str>) {
        if let Some(entry) = self.0.get_mut(topic) {
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis();
            *entry = Some((body, now));
        }
    }

    #[cfg(feature = "ha")]
    pub fn restore(&mut self, topic: &str, body: Arc<str>, at_ms: u64) {
        if let Some(entry) = self.0.get_mut(topic) {
            *entry = Some((body, u128::from(at_ms)));
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
