use super::token::Jwk;
use crate::ha::{Number, replica::now_ms};
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::watch;

pub const FRESHNESS: Duration = Duration::from_secs(55);
const MAX_BYTES: usize = 1024 * 1024;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyConfig {
    pub url: String,
    pub issuer: String,
    pub audience: String,
    pub ca_file: Option<PathBuf>,
    pub credential_file: Option<PathBuf>,
    pub max_token_seconds: u64,
}

#[derive(Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct Policy {
    pub version: Number,
    pub keys: Vec<Jwk>,
    pub subjects: BTreeMap<String, Grant>,
    pub revoked: BTreeMap<String, Revocation>,
}

#[derive(Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct Grant {
    pub generation: Number,
    pub topics: Vec<String>,
}

#[derive(Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct Revocation {
    pub revoked_at: u64,
    pub retain_until: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    generated_at: u64,
    valid_until: u64,
    policy: Policy,
}

pub(super) struct Cache {
    pub policy: Policy,
    pub generations: BTreeMap<String, Arc<()>>,
    pub observed: Instant,
    pub valid_until: u64,
}

impl Cache {
    pub fn fresh(&self) -> bool {
        self.observed.elapsed() < FRESHNESS && now_ms() / 1000 < self.valid_until
    }
}

#[derive(Clone)]
pub struct SharedAuth {
    pub(super) config: Arc<PolicyConfig>,
    pub(super) cache: watch::Sender<Option<Arc<Cache>>>,
    client: reqwest::Client,
}

fn read(path: &std::path::Path, max: usize) -> Result<Vec<u8>, &'static str> {
    use std::io::Read;
    use std::os::unix::fs::OpenOptionsExt;
    let mut bytes = Vec::new();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(path)
        .map_err(|_| "policy-config")?;
    if !file.metadata().map_err(|_| "policy-config")?.is_file() {
        return Err("policy-config");
    }
    file.take((max + 1) as u64).read_to_end(&mut bytes).map_err(|_| "policy-config")?;
    if bytes.len() > max {
        return Err("policy-config");
    }
    Ok(bytes)
}

impl SharedAuth {
    pub fn new(config: PolicyConfig) -> Result<Self, &'static str> {
        let url = reqwest::Url::parse(&config.url).map_err(|_| "policy-config")?;
        if url.scheme() != "https"
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
            || config.issuer.is_empty()
            || config.audience.is_empty()
            || !(60..=3600).contains(&config.max_token_seconds)
        {
            return Err("policy-config");
        }
        let mut client = reqwest::Client::builder()
            .https_only(true)
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(2));
        if let Some(path) = &config.ca_file {
            client = client.add_root_certificate(
                reqwest::Certificate::from_pem(&read(path, 65536)?).map_err(|_| "policy-config")?,
            );
        }
        let client = client.build().map_err(|_| "policy-config")?;
        let (cache, _) = watch::channel(None);
        Ok(Self { config: Arc::new(config), cache, client })
    }
    pub fn fresh(&self) -> bool {
        self.cache.borrow().as_ref().is_some_and(|cache| cache.fresh())
    }
    pub fn version(&self) -> Option<u64> {
        self.cache.borrow().as_ref().map(|cache| cache.policy.version.0)
    }
    pub(super) fn install(&self, bytes: &[u8], observed: Instant) -> Result<(), &'static str> {
        if bytes.len() > MAX_BYTES || observed.elapsed() >= FRESHNESS {
            return Err("policy-stale");
        }
        let document: Document = serde_json::from_slice(bytes).map_err(|_| "policy-invalid")?;
        let now = now_ms() / 1000;
        if document.generated_at > now.saturating_add(1)
            || now.saturating_sub(document.generated_at) > 5
            || document.valid_until <= now
            || document.valid_until > document.generated_at.saturating_add(55)
        {
            return Err("policy-stale");
        }
        let policy = &document.policy;
        if policy.version.0 == 0
            || policy.keys.is_empty()
            || policy.keys.len() > 8
            || policy.subjects.len() > 10000
            || policy.revoked.len() > 8192
        {
            return Err("policy-invalid");
        }
        let mut kids = std::collections::BTreeSet::new();
        for key in &policy.keys {
            key.validate()?;
            if !kids.insert(&key.kid) {
                return Err("policy-invalid");
            }
        }
        for (subject, grant) in &policy.subjects {
            let topics = &grant.topics;
            if grant.generation.0 == 0
                || subject.is_empty()
                || subject.len() > 256
                || subject.chars().any(char::is_control)
                || topics.len() > 256
                || topics.windows(2).any(|pair| pair[0] >= pair[1])
                || topics.iter().any(|topic| {
                    !topic.starts_with("/topic/v1/") || topic.len() > 256 || topic.chars().any(char::is_control)
                })
            {
                return Err("policy-invalid");
            }
        }
        let old = self.cache.borrow().clone();
        if let Some(old) = &old {
            if policy.version < old.policy.version {
                return Err("policy-rollback");
            }
            if policy.version == old.policy.version && policy != &old.policy {
                return Err("policy-version-conflict");
            }
            for key in &old.policy.keys {
                if key.retain_until > now
                    && !policy.keys.iter().any(|next| {
                        next.kid == key.kid
                            && next.n == key.n
                            && next.e == key.e
                            && next.retain_until >= key.retain_until
                    })
                {
                    return Err("key-overlap-required");
                }
            }
            for (id, revoked) in &old.policy.revoked {
                if revoked.retain_until > now
                    && policy.revoked.get(id).is_none_or(|next| {
                        next.retain_until < revoked.retain_until || next.revoked_at != revoked.revoked_at
                    })
                {
                    return Err("revocation-retention-required");
                }
            }
        }
        for (id, revoked) in &policy.revoked {
            if !crate::source::identifier(id)
                || revoked.revoked_at > document.generated_at
                || revoked
                    .retain_until
                    .checked_sub(revoked.revoked_at)
                    .is_none_or(|duration| duration < self.config.max_token_seconds)
            {
                return Err("policy-invalid");
            }
        }
        if let Some(old) = &old {
            for (subject, previous) in &old.policy.subjects {
                if let Some(next) = policy.subjects.get(subject)
                    && (next.generation < previous.generation
                        || next.topics != previous.topics && next.generation == previous.generation)
                {
                    return Err("subject-generation-invalid");
                }
            }
        }
        let generations = policy
            .subjects
            .iter()
            .map(|(subject, topics)| {
                let previous = old
                    .as_ref()
                    .filter(|old| old.policy.subjects.get(subject) == Some(topics))
                    .and_then(|old| old.generations.get(subject))
                    .cloned();
                (subject.clone(), previous.unwrap_or_else(|| Arc::new(())))
            })
            .collect();
        self.cache.send_replace(Some(Arc::new(Cache {
            policy: document.policy,
            generations,
            observed,
            valid_until: document.valid_until,
        })));
        Ok(())
    }
    async fn fetch(&self) -> Result<(), &'static str> {
        let mut request = self.client.get(&self.config.url);
        if let Some(path) = &self.config.credential_file {
            let path = path.clone();
            let bytes =
                tokio::task::spawn_blocking(move || read(&path, 16384)).await.map_err(|_| "policy-credential")??;
            let token = std::str::from_utf8(&bytes).map_err(|_| "policy-credential")?.trim();
            if token.is_empty() || token.chars().any(char::is_control) {
                return Err("policy-credential");
            }
            request = request.bearer_auth(token);
        }
        let observed = Instant::now();
        let mut response = request.send().await.map_err(|_| "policy-unavailable")?;
        if response.status() != reqwest::StatusCode::OK
            || response.content_length().is_some_and(|n| n > MAX_BYTES as u64)
        {
            return Err("policy-unavailable");
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| "policy-unavailable")? {
            if bytes.len() + chunk.len() > MAX_BYTES {
                return Err("policy-too-large");
            }
            bytes.extend_from_slice(&chunk);
        }
        self.install(&bytes, observed)
    }
    pub(crate) fn spawn(&self) -> tokio::task::JoinHandle<()> {
        let auth = self.clone();
        tokio::spawn(async move {
            let mut failed = false;
            loop {
                match auth.fetch().await {
                    Ok(()) => {
                        if failed {
                            tracing::info!(event = "ha_policy_restored");
                        }
                        failed = false;
                    }
                    Err(code) => {
                        if !failed {
                            tracing::warn!(event = "ha_policy_refresh_failed", reason = code);
                        }
                        failed = true;
                    }
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        })
    }
}

#[cfg(test)]
mod tests;
