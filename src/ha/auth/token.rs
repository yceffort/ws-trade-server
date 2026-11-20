use super::policy::{Cache, SharedAuth};
use crate::ha::replica::now_ms;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::Deserialize;
use std::{sync::Arc, time::Duration};
use tokio::sync::watch;

#[derive(Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct Jwk {
    pub kid: String,
    kty: String,
    alg: String,
    #[serde(rename = "use")]
    usage: String,
    pub n: String,
    pub e: String,
    pub retain_until: u64,
}
impl Jwk {
    pub fn validate(&self) -> Result<(), &'static str> {
        let n = URL_SAFE_NO_PAD.decode(&self.n).map_err(|_| "policy-key-invalid")?;
        let e = URL_SAFE_NO_PAD.decode(&self.e).map_err(|_| "policy-key-invalid")?;
        if !crate::source::identifier(&self.kid)
            || self.kty != "RSA"
            || self.alg != "RS256"
            || self.usage != "sig"
            || !(256..=512).contains(&n.len())
            || n.first() == Some(&0)
            || e != [1, 0, 1]
        {
            return Err("policy-key-invalid");
        }
        Ok(())
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    alg: String,
    kid: String,
    typ: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Claims {
    iss: String,
    aud: String,
    sub: String,
    jti: String,
    iat: u64,
    nbf: u64,
    exp: u64,
    topics: Vec<String>,
}

pub(crate) struct Session {
    auth: SharedAuth,
    receiver: watch::Receiver<Option<Arc<Cache>>>,
    claims: Claims,
    kid: String,
    generation: Arc<()>,
}
impl SharedAuth {
    pub(crate) fn authorize(&self, header: &str) -> Result<Session, &'static str> {
        let token = header.strip_prefix("Bearer ").filter(|token| token.len() <= 16384).ok_or("auth-invalid")?;
        let (message, signature) = token.rsplit_once('.').ok_or("auth-invalid")?;
        let (head, body) = message.split_once('.').ok_or("auth-invalid")?;
        let header: Header = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(head).map_err(|_| "auth-invalid")?)
            .map_err(|_| "auth-invalid")?;
        if header.alg != "RS256" || header.typ != "at+jwt" {
            return Err("auth-invalid");
        }
        let receiver = self.cache.subscribe();
        let cache = receiver.borrow().clone().ok_or("auth-policy-stale")?;
        if !cache.fresh() {
            return Err("auth-policy-stale");
        }
        let key = cache.policy.keys.iter().find(|key| key.kid == header.kid).ok_or("auth-invalid")?;
        let n = URL_SAFE_NO_PAD.decode(&key.n).map_err(|_| "auth-invalid")?;
        let e = URL_SAFE_NO_PAD.decode(&key.e).map_err(|_| "auth-invalid")?;
        let signature = URL_SAFE_NO_PAD.decode(signature).map_err(|_| "auth-invalid")?;
        ring::signature::RsaPublicKeyComponents { n, e }
            .verify(&ring::signature::RSA_PKCS1_2048_8192_SHA256, message.as_bytes(), &signature)
            .map_err(|_| "auth-invalid")?;
        let claims: Claims = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(body).map_err(|_| "auth-invalid")?)
            .map_err(|_| "auth-invalid")?;
        let generation = cache.generations.get(&claims.sub).cloned().ok_or("auth-revoked")?;
        let session = Session { auth: self.clone(), receiver, claims, kid: header.kid, generation };
        session.validate(&cache)?;
        Ok(session)
    }
}
impl Session {
    fn validate(&self, cache: &Cache) -> Result<(), &'static str> {
        if !cache.fresh() {
            return Err("auth-policy-stale");
        }
        let claims = &self.claims;
        let now = now_ms() / 1000;
        if claims.exp <= now {
            return Err("auth-expired");
        }
        if claims.iss != self.auth.config.issuer
            || claims.aud != self.auth.config.audience
            || claims.iat > now
            || claims.nbf > now
            || claims.nbf < claims.iat
            || claims.exp <= claims.nbf
            || claims.exp.checked_sub(claims.iat).is_none_or(|ttl| ttl > self.auth.config.max_token_seconds)
            || !crate::source::identifier(&claims.jti)
            || claims.topics.is_empty()
            || claims.topics.len() > 256
            || claims.topics.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err("auth-invalid");
        }
        if cache.generations.get(&claims.sub).is_none_or(|generation| !Arc::ptr_eq(generation, &self.generation))
            || cache.policy.revoked.get(&claims.jti).is_some_and(|revoked| revoked.retain_until > now)
        {
            return Err("auth-revoked");
        }
        let key = cache.policy.keys.iter().find(|key| key.kid == self.kid).ok_or("auth-revoked")?;
        if key.retain_until < claims.exp {
            return Err("auth-invalid");
        }
        let allowed = cache.policy.subjects.get(&claims.sub).ok_or("auth-revoked")?;
        if claims.topics.iter().any(|topic| allowed.topics.binary_search(topic).is_err()) {
            return Err("forbidden-scope");
        }
        Ok(())
    }
    pub fn subject(&self) -> String {
        format!("{}\0{}", self.claims.iss, self.claims.sub)
    }
    pub fn allows(&self, topics: &[String]) -> bool {
        topics.iter().all(|topic| self.claims.topics.binary_search(topic).is_ok())
    }
    pub async fn invalidated(&mut self) -> &'static str {
        loop {
            let cache = self.receiver.borrow_and_update().clone();
            if let Err(reason) = cache.as_ref().ok_or("auth-policy-stale").and_then(|cache| self.validate(cache)) {
                return reason;
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(250)) => {},
                result = self.receiver.changed() => { if result.is_err() { return "auth-policy-stale"; } }
            }
        }
    }
}
