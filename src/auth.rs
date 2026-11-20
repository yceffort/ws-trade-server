use serde::Deserialize;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::watch;

#[derive(Clone, Deserialize, PartialEq)]
pub struct Token {
    pub token: String,
    pub expires_at: u64,
    pub principal: Option<String>,
}

#[derive(Clone)]
pub struct Auth {
    tokens: watch::Sender<Vec<Token>>,
    path: Option<PathBuf>,
}

#[derive(Clone)]
pub(crate) struct Session {
    tokens: watch::Receiver<Vec<Token>>,
    token: String,
    pub(crate) principal: Option<String>,
}

fn find<'a>(tokens: &'a [Token], token: &str) -> Option<&'a Token> {
    tokens.iter().find(|entry| entry.token == token)
}

fn expiry(tokens: &[Token], token: &str, now: SystemTime) -> Result<SystemTime, &'static str> {
    let entry = find(tokens, token).ok_or("auth-invalid")?;
    let expires = UNIX_EPOCH.checked_add(Duration::from_secs(entry.expires_at)).ok_or("auth-expired")?;
    if expires <= now {
        return Err("auth-expired");
    }
    Ok(expires)
}

impl Auth {
    pub fn new(tokens: Vec<Token>) -> Self {
        let (tokens, _) = watch::channel(tokens);
        Self { tokens, path: None }
    }

    pub fn from_file(path: PathBuf) -> io::Result<Self> {
        let mut auth = Self::new(read(&path)?);
        auth.path = Some(path);
        Ok(auth)
    }

    pub fn replace(&self, tokens: Vec<Token>) {
        self.tokens.send_if_modified(|current| {
            if *current == tokens {
                false
            } else {
                *current = tokens;
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
        let principal = {
            let current = tokens.borrow();
            expiry(&current, token, SystemTime::now())?;
            // 주체를 바꾸려면 기존 연결을 폐기하고 새로 인증해야 한다.
            find(&current, token).and_then(|entry| entry.principal.clone())
        };
        Ok(Session { tokens, token: token.into(), principal })
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
                if find(&current, &self.token).is_none_or(|entry| entry.principal != self.principal) {
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
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };

    #[tokio::test]
    async fn expiration_rechecks_wall_time_without_token_changes() {
        let clock = Arc::new(AtomicU64::new(100));
        let (tokens, receiver) =
            watch::channel(vec![Token { token: "demo".into(), expires_at: 1000, principal: None }]);
        let mut session = Session { tokens: receiver, token: "demo".into(), principal: None };
        let advanced = clock.clone();
        let task = tokio::spawn(async move {
            session.invalidated_at(|| UNIX_EPOCH + Duration::from_secs(advanced.load(Ordering::Relaxed))).await
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        clock.store(1001, Ordering::Relaxed);
        assert_eq!(tokio::time::timeout(Duration::from_secs(2), task).await.unwrap().unwrap(), "auth-expired");
        drop(tokens);
    }
}
