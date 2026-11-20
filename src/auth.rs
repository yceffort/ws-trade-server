use serde::Deserialize;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::watch;

#[derive(Clone, Deserialize, PartialEq)]
pub struct Token {
    pub token: String,
    pub expires_at: u64,
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
}

fn expiry(tokens: &[Token], token: &str) -> Result<SystemTime, &'static str> {
    let entry = tokens.iter().find(|entry| entry.token == token).ok_or("auth-invalid")?;
    let expires = UNIX_EPOCH.checked_add(Duration::from_secs(entry.expires_at)).ok_or("auth-expired")?;
    if expires <= SystemTime::now() {
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
        expiry(&tokens.borrow(), token)?;
        Ok(Session { tokens, token: token.into() })
    }

    pub(crate) fn reload(&self) -> Option<tokio::task::JoinHandle<()>> {
        let path = self.path.clone()?;
        let auth = self.clone();
        Some(tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(500)).await;
                let path = path.clone();
                let tokens = tokio::task::spawn_blocking(move || read(&path)).await;
                match tokens {
                    Ok(Ok(tokens)) => auth.replace(tokens),
                    _ => {
                        eprintln!("인증 파일을 읽지 못해 토큰 목록을 비웠다");
                        auth.replace(Vec::new());
                    }
                }
            }
        }))
    }
}

impl Session {
    pub async fn invalidated(&mut self) -> &'static str {
        loop {
            let until = match expiry(&self.tokens.borrow_and_update(), &self.token) {
                Ok(until) => until,
                Err("auth-expired") => return "auth-expired",
                Err(_) => return "auth-revoked",
            };
            let remaining = until.duration_since(SystemTime::now()).unwrap_or_default();
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
    std::fs::File::open(path)?.take(65537).read_to_end(&mut bytes)?;
    if bytes.len() > 65536 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "인증 파일 크기 초과"));
    }
    serde_json::from_slice(&bytes).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "인증 파일 형식 오류"))
}
