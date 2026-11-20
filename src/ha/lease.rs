use super::Error;
use reqwest::StatusCode;
use reqwest::blocking::{Client, Response};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::Read;
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaseConfig {
    pub url: String,
    pub token_file: PathBuf,
    pub ca_file: Option<PathBuf>,
    pub identity: String,
    pub duration_seconds: u64,
    #[serde(default)]
    pub development_loopback_http: bool,
}

pub struct Lease {
    config: LeaseConfig,
    client: Client,
    observed: Option<(String, Instant)>,
    renewed: Option<Instant>,
}

#[derive(Clone, Copy)]
struct Validity {
    renewed: Instant,
    active: bool,
}

pub struct LeaseGuard {
    state: tokio::sync::watch::Receiver<Validity>,
    freshness: Duration,
    stop: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}

impl LeaseGuard {
    pub fn check(&self) -> Result<(), Error> {
        let state = *self.state.borrow();
        if state.active && state.renewed.elapsed() < self.freshness { Ok(()) } else { Err(Error::LeaseLost) }
    }
}

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(worker) = self.worker.take() {
            worker.thread().unpark();
            let _ = worker.join();
        }
    }
}

impl Lease {
    pub fn into_guard(mut self) -> Result<LeaseGuard, Error> {
        let renewed = self.renewed.ok_or(Error::LeaseLost)?;
        let freshness = Duration::from_secs(self.config.duration_seconds / 2);
        let (updates, state) = tokio::sync::watch::channel(Validity { renewed, active: true });
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        // Kafka의 blocking 호출 중에도 Lease를 갱신하고, 프로세스 정지는 guard의 단조 시간 검사로 차단한다.
        let worker = thread::spawn(move || {
            while !stopped.load(Ordering::Relaxed) {
                if self.check().is_err() {
                    updates.send_replace(Validity { renewed, active: false });
                    break;
                }
                if let Some(renewed) = self.renewed {
                    updates.send_replace(Validity { renewed, active: true });
                }
                thread::park_timeout(Duration::from_millis(250));
            }
        });
        Ok(LeaseGuard { state, freshness, stop, worker: Some(worker) })
    }
    pub fn new(config: LeaseConfig) -> Result<Self, Error> {
        let url = reqwest::Url::parse(&config.url).map_err(|_| Error::InvalidConfig)?;
        let local = matches!(url.host_str(), Some("127.0.0.1" | "[::1]" | "localhost"));
        if (url.scheme() != "https" && !(url.scheme() == "http" && local && config.development_loopback_http))
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || !crate::source::identifier(&config.identity)
            || !(6..=20).contains(&config.duration_seconds)
        {
            return Err(Error::InvalidConfig);
        }
        let mut client = Client::builder().timeout(Duration::from_secs(2)).redirect(reqwest::redirect::Policy::none());
        if let Some(path) = &config.ca_file {
            client = client.add_root_certificate(
                reqwest::Certificate::from_pem(&std::fs::read(path)?).map_err(|_| Error::InvalidConfig)?,
            );
        }
        Ok(Self { config, client: client.build().map_err(|_| Error::InvalidConfig)?, observed: None, renewed: None })
    }

    fn token(&self) -> Result<String, Error> {
        let mut token = String::new();
        std::fs::File::open(&self.config.token_file)?.take(16385).read_to_string(&mut token)?;
        if token.trim().is_empty() || token.len() > 16384 {
            return Err(Error::LeaseLost);
        }
        Ok(token.trim().into())
    }

    fn document(response: Response) -> Result<Value, Error> {
        if !response.status().is_success() {
            return Err(Error::LeaseLost);
        }
        let mut bytes = Vec::new();
        response.take(65537).read_to_end(&mut bytes)?;
        if bytes.len() > 65536 {
            return Err(Error::LeaseLost);
        }
        let document: Value = serde_json::from_slice(&bytes).map_err(|_| Error::LeaseLost)?;
        if document["kind"] != "Lease"
            || document["apiVersion"] != "coordination.k8s.io/v1"
            || !document["spec"].is_object()
            || document["metadata"]["resourceVersion"].as_str().is_none_or(|s| s.is_empty() || s.len() > 128)
        {
            return Err(Error::LeaseLost);
        }
        Ok(document)
    }

    fn get(&self) -> Result<Value, Error> {
        Self::document(
            self.client.get(&self.config.url).bearer_auth(self.token()?).send().map_err(|_| Error::LeaseLost)?,
        )
    }

    fn claim(&mut self, mut document: Value) -> Result<bool, Error> {
        let previous = document["spec"]["holderIdentity"].as_str();
        let transitions = document["spec"]["leaseTransitions"].as_u64().unwrap_or_default();
        if previous != Some(&self.config.identity) {
            document["spec"]["leaseTransitions"] = Value::from(transitions.checked_add(1).ok_or(Error::LeaseLost)?);
        }
        document["spec"]["holderIdentity"] = Value::from(self.config.identity.clone());
        document["spec"]["leaseDurationSeconds"] = Value::from(self.config.duration_seconds);
        document["spec"]["renewTime"] =
            Value::from(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true));
        let response = self
            .client
            .put(&self.config.url)
            .bearer_auth(self.token()?)
            .json(&document)
            .send()
            .map_err(|_| Error::LeaseLost)?;
        if response.status() == StatusCode::CONFLICT {
            self.renewed = None;
            return Ok(false);
        }
        let current = Self::document(response)?;
        if current["spec"]["holderIdentity"].as_str() != Some(&self.config.identity) {
            return Err(Error::LeaseLost);
        }
        self.renewed = Some(Instant::now());
        Ok(true)
    }

    pub fn acquire(&mut self) -> Result<bool, Error> {
        let document = self.get()?;
        let version = document["metadata"]["resourceVersion"]
            .as_str()
            .filter(|s| !s.is_empty() && s.len() <= 128)
            .ok_or(Error::LeaseLost)?;
        if document["spec"]["holderIdentity"].as_str().is_none_or(str::is_empty) {
            return self.claim(document);
        }
        if self.observed.as_ref().is_none_or(|(old, _)| old != version) {
            self.observed = Some((version.into(), Instant::now()));
        }
        let seconds = document["spec"]["leaseDurationSeconds"]
            .as_u64()
            .filter(|s| (1..=60).contains(s))
            .ok_or(Error::LeaseLost)?;
        // 다른 프로세스의 벽시계 대신 같은 resourceVersion을 관측한 단조 시간으로 만료를 판단한다.
        if self.observed.as_ref().is_some_and(|(_, since)| since.elapsed() >= Duration::from_secs(seconds)) {
            return self.claim(document);
        }
        Ok(false)
    }

    pub fn check(&mut self) -> Result<(), Error> {
        let previous = self.renewed.ok_or(Error::LeaseLost)?;
        if previous.elapsed() >= Duration::from_secs(self.config.duration_seconds / 2) {
            self.renewed = None;
            return Err(Error::LeaseLost);
        }
        if previous.elapsed() < Duration::from_secs(1) {
            return Ok(());
        }
        let document = self.get()?;
        if document["spec"]["holderIdentity"].as_str() != Some(&self.config.identity) || !self.claim(document)? {
            self.renewed = None;
            return Err(Error::LeaseLost);
        }
        // 오래 멈췄던 writer는 성공한 응답만으로 복귀하지 않고 새 프로세스의 fencing과 복원을 거친다.
        if previous.elapsed() >= Duration::from_secs(self.config.duration_seconds / 2) {
            self.renewed = None;
            return Err(Error::LeaseLost);
        }
        Ok(())
    }
}
