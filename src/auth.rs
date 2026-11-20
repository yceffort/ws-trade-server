use serde::Deserialize;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use subtle::ConstantTimeEq;
use tokio::sync::watch;

#[derive(Clone, Deserialize, PartialEq)]
pub struct Token {
    pub token: String,
    pub expires_at: u64,
    pub principal: Option<String>,
}

struct Entry {
    token: Token,
    generation: Arc<()>,
}

#[derive(Clone)]
pub struct Auth {
    tokens: watch::Sender<Vec<Entry>>,
    path: Option<PathBuf>,
}

#[derive(Clone)]
pub(crate) struct Session {
    tokens: watch::Receiver<Vec<Entry>>,
    token: String,
    generation: Arc<()>,
    pub(crate) principal: Option<String>,
}

fn find<'a>(tokens: &'a [Entry], token: &str) -> Option<&'a Entry> {
    // 일치 위치에 따라 비교 시간이 달라지지 않도록 일치해도 멈추지 않고 끝까지 비교하고, 중복이면 첫 항목을 고른다.
    let mut found = None;
    for entry in tokens {
        let matched = entry.token.token.as_bytes().ct_eq(token.as_bytes());
        found = found.or(bool::from(matched).then_some(entry));
    }
    found
}

fn expiry(tokens: &[Entry], token: &str, now: SystemTime) -> Result<SystemTime, &'static str> {
    let entry = find(tokens, token).ok_or("auth-invalid")?;
    let expires = UNIX_EPOCH.checked_add(Duration::from_secs(entry.token.expires_at)).ok_or("auth-expired")?;
    if expires <= now {
        return Err("auth-expired");
    }
    Ok(expires)
}

impl Auth {
    pub fn new(tokens: Vec<Token>) -> Self {
        let entries = tokens.into_iter().map(|token| Entry { token, generation: Arc::new(()) }).collect();
        let (tokens, _) = watch::channel(entries);
        Self { tokens, path: None }
    }

    pub fn from_file(path: PathBuf) -> io::Result<Self> {
        let mut auth = Self::new(read(&path)?);
        auth.path = Some(path);
        Ok(auth)
    }

    pub fn replace(&self, tokens: Vec<Token>) {
        self.tokens.send_if_modified(|current| {
            if current.iter().map(|entry| &entry.token).eq(tokens.iter()) {
                false
            } else {
                // watch가 중간 변경을 합쳐도 삭제되거나 주체가 바뀐 토큰의 이전 세대는 복원하지 않는다.
                *current = tokens
                    .into_iter()
                    .map(|token| {
                        let generation = find(current, &token.token)
                            .filter(|entry| entry.token.principal == token.principal)
                            .map_or_else(|| Arc::new(()), |entry| entry.generation.clone());
                        Entry { token, generation }
                    })
                    .collect();
                true
            }
        });
    }

    pub(crate) fn authorize(&self, header: Option<&str>) -> Result<Session, &'static str> {
        let token = header
            .and_then(|header| header.strip_prefix("Bearer "))
            .filter(|token| !token.is_empty())
            .ok_or("auth-invalid")?;
        let tokens = self.tokens.subscribe();
        let (principal, generation) = {
            let current = tokens.borrow();
            expiry(&current, token, SystemTime::now())?;
            // 주체를 바꾸려면 기존 연결을 폐기하고 새로 인증해야 한다.
            let entry = find(&current, token).ok_or("auth-invalid")?;
            (entry.token.principal.clone(), entry.generation.clone())
        };
        Ok(Session { tokens, token: token.into(), generation, principal })
    }

    pub(crate) fn reload(&self) -> Option<tokio::task::JoinHandle<()>> {
        let path = self.path.clone()?;
        let auth = self.clone();
        Some(tokio::spawn(async move {
            let mut failed = false;
            loop {
                tokio::time::sleep(Duration::from_millis(500)).await;
                let path = path.clone();
                let tokens = tokio::task::spawn_blocking(move || read(&path)).await;
                match tokens {
                    Ok(Ok(tokens)) => {
                        if failed {
                            tracing::info!(event = "auth_reload_restored");
                        }
                        failed = false;
                        auth.replace(tokens);
                    }
                    _ => {
                        if !failed {
                            tracing::warn!(event = "auth_reload_failed");
                        }
                        failed = true;
                        auth.replace(Vec::new());
                    }
                }
            }
        }))
    }
}

impl Session {
    pub async fn invalidated(&mut self) -> &'static str {
        self.invalidated_at(SystemTime::now).await
    }

    async fn invalidated_at(&mut self, now: impl Fn() -> SystemTime) -> &'static str {
        loop {
            let until = {
                let current = self.tokens.borrow_and_update();
                if find(&current, &self.token).is_none_or(|entry| !Arc::ptr_eq(&entry.generation, &self.generation)) {
                    return "auth-revoked";
                }
                match expiry(&current, &self.token, now()) {
                    Ok(until) => until,
                    Err("auth-expired") => return "auth-expired",
                    Err(_) => return "auth-revoked",
                }
            };
            // 시스템 시각 변경도 반영하도록 단조 시계의 대기를 짧게 끊는다.
            let remaining = until.duration_since(now()).unwrap_or_default().min(Duration::from_millis(500));
            tokio::select! {
                _ = tokio::time::sleep(remaining) => {},
                result = self.tokens.changed() => { if result.is_err() { return "auth-revoked"; } }
            }
        }
    }
}

pub(crate) async fn invalidated(session: &mut Option<Session>) -> &'static str {
    match session {
        Some(session) => session.invalidated().await,
        None => std::future::pending().await,
    }
}

fn read(path: &Path) -> io::Result<Vec<Token>> {
    let mut bytes = Vec::new();
    open_regular(path)?.take(65537).read_to_end(&mut bytes)?;
    if bytes.len() > 65536 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "인증 파일 크기 초과"));
    }
    serde_json::from_slice(&bytes).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "인증 파일 형식 오류"))
}

#[cfg(unix)]
fn open_regular(path: &Path) -> io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    // 경로가 FIFO로 바뀌어도 open이 기다리지 않으며 열린 파일 자체의 종류를 검사한다.
    let file = std::fs::OpenOptions::new().read(true).custom_flags(libc::O_NONBLOCK).open(path)?;
    if !file.metadata()?.is_file() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "일반 인증 파일만 허용한다"));
    }
    Ok(file)
}

#[cfg(not(unix))]
fn open_regular(_: &Path) -> io::Result<std::fs::File> {
    Err(io::Error::new(io::ErrorKind::Unsupported, "인증 파일 재로딩은 Unix에서만 지원한다"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[tokio::test]
    async fn coalesced_principal_changes_and_removals_revoke_old_sessions() {
        let original = Token { token: "demo".into(), expires_at: u32::MAX.into(), principal: Some("a".into()) };
        let changed = Token { principal: Some("b".into()), ..original.clone() };
        for intermediate in [vec![changed], Vec::new()] {
            let auth = Auth::new(vec![original.clone()]);
            let mut old = auth.authorize(Some("Bearer demo")).unwrap();
            let mut old_writer = old.clone();
            auth.replace(intermediate);
            auth.replace(vec![original.clone()]);
            for session in [&mut old, &mut old_writer] {
                assert_eq!(
                    tokio::time::timeout(Duration::from_secs(1), session.invalidated()).await.unwrap(),
                    "auth-revoked"
                );
            }
            let mut fresh = auth.authorize(Some("Bearer demo")).unwrap();
            assert!(tokio::time::timeout(Duration::from_millis(10), fresh.invalidated()).await.is_err());
        }
    }

    #[tokio::test]
    async fn unchanged_principal_survives_reload_and_unrelated_changes() {
        let token = Token { token: "demo".into(), expires_at: u32::MAX.into(), principal: Some("a".into()) };
        let auth = Auth::new(vec![token.clone()]);
        let mut session = auth.authorize(Some("Bearer demo")).unwrap();
        auth.replace(vec![token.clone()]);
        auth.replace(vec![
            Token { token: "other".into(), ..token.clone() },
            Token { expires_at: token.expires_at + 1, ..token },
        ]);
        assert!(tokio::time::timeout(Duration::from_millis(10), session.invalidated()).await.is_err());
    }

    #[tokio::test]
    async fn expiration_rechecks_wall_time_without_token_changes() {
        let clock = Arc::new(AtomicU64::new(100));
        let (tokens, receiver) = watch::channel(vec![Entry {
            token: Token { token: "demo".into(), expires_at: 1000, principal: None },
            generation: Arc::new(()),
        }]);
        let generation = receiver.borrow()[0].generation.clone();
        let mut session = Session { tokens: receiver, token: "demo".into(), generation, principal: None };
        let advanced = clock.clone();
        let task = tokio::spawn(async move {
            session.invalidated_at(|| UNIX_EPOCH + Duration::from_secs(advanced.load(Ordering::Relaxed))).await
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        clock.store(1001, Ordering::Relaxed);
        assert_eq!(tokio::time::timeout(Duration::from_secs(2), task).await.unwrap().unwrap(), "auth-expired");
        drop(tokens);
    }

    fn entry(token: &str, expires_at: u64) -> Entry {
        Entry { token: Token { token: token.into(), expires_at, principal: None }, generation: Arc::new(()) }
    }

    #[test]
    fn same_length_different_token_is_invalid() {
        let tokens = [entry("demo-aaaa", 1000)];
        assert_eq!(expiry(&tokens, "demo-aaab", UNIX_EPOCH), Err("auth-invalid"));
    }

    #[test]
    fn shorter_prefix_of_token_is_invalid() {
        let tokens = [entry("demo-valid", 1000)];
        assert_eq!(expiry(&tokens, "demo", UNIX_EPOCH), Err("auth-invalid"));
        assert_eq!(expiry(&tokens, "demo-valid-extra", UNIX_EPOCH), Err("auth-invalid"));
    }

    #[test]
    fn duplicate_token_uses_first_entry() {
        let tokens = [entry("demo", 1000), entry("demo", 2000)];
        assert_eq!(expiry(&tokens, "demo", UNIX_EPOCH), Ok(UNIX_EPOCH + Duration::from_secs(1000)));
        // 첫 항목이 만료됐으면 뒤의 유효한 항목이 있어도 만료로 판정한다.
        assert_eq!(expiry(&tokens, "demo", UNIX_EPOCH + Duration::from_secs(1500)), Err("auth-expired"));
    }
}
