use super::{Number, digest, replica::now_ms};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::hmac;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

const LIFETIME: u64 = 86400;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CursorKey {
    secret: String,
    retain_until: u64,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CursorKeys {
    current: String,
    keys: BTreeMap<String, CursorKey>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Claims {
    version: u32,
    kid: String,
    log_id: String,
    partition: u32,
    offset: Number,
    scope: String,
    subject: String,
    issued_at: u64,
    expires_at: u64,
}

impl CursorKeys {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.keys.is_empty() || self.keys.len() > 8 || !self.keys.contains_key(&self.current) {
            return Err("cursor-key-config");
        }
        for (kid, key) in &self.keys {
            if !crate::source::identifier(kid)
                || !(32..=64).contains(&URL_SAFE_NO_PAD.decode(&key.secret).map_err(|_| "cursor-key-config")?.len())
            {
                return Err("cursor-key-config");
            }
        }
        Ok(())
    }

    pub(crate) fn ready(&self) -> bool {
        self.keys.get(&self.current).is_some_and(|key| key.retain_until >= (now_ms() / 1000).saturating_add(LIFETIME))
    }

    fn key(&self, kid: &str) -> Result<hmac::Key, &'static str> {
        let key = self.keys.get(kid).ok_or("invalid-cursor")?;
        if key.retain_until <= now_ms() / 1000 {
            return Err("cursor-expired");
        }
        let bytes = URL_SAFE_NO_PAD.decode(&key.secret).map_err(|_| "cursor-key-config")?;
        if !(32..=64).contains(&bytes.len()) {
            return Err("cursor-key-config");
        }
        Ok(hmac::Key::new(hmac::HMAC_SHA256, &bytes))
    }

    pub(crate) fn sign(
        &self,
        log_id: &str,
        offset: u64,
        scope: &[String],
        subject: &str,
    ) -> Result<String, &'static str> {
        let now = now_ms() / 1000;
        let expires_at = now.checked_add(LIFETIME).ok_or("cursor-key-config")?;
        if self.keys.get(&self.current).is_none_or(|key| key.retain_until < expires_at) {
            return Err("cursor-key-config");
        }
        let claims = Claims {
            version: 1,
            kid: self.current.clone(),
            log_id: log_id.into(),
            partition: 0,
            offset: Number(offset),
            scope: scope_hash(scope)?,
            subject: digest(subject.as_bytes()),
            issued_at: now,
            expires_at,
        };
        let payload = serde_json::to_vec(&claims).map_err(|_| "invalid-cursor")?;
        let encoded = URL_SAFE_NO_PAD.encode(payload);
        let signature = hmac::sign(&self.key(&self.current)?, encoded.as_bytes());
        Ok(format!("{encoded}.{}", URL_SAFE_NO_PAD.encode(signature.as_ref())))
    }

    pub(crate) fn verify(
        &self,
        cursor: &str,
        log_id: &str,
        scope: &[String],
        subject: &str,
    ) -> Result<u64, &'static str> {
        if cursor.len() > 4096 {
            return Err("invalid-cursor");
        }
        let (payload, signature) = cursor.split_once('.').ok_or("invalid-cursor")?;
        let claims: Claims = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).map_err(|_| "invalid-cursor")?)
            .map_err(|_| "invalid-cursor")?;
        let signature = URL_SAFE_NO_PAD.decode(signature).map_err(|_| "invalid-cursor")?;
        hmac::verify(&self.key(&claims.kid)?, payload.as_bytes(), &signature).map_err(|_| "invalid-cursor")?;
        let now = now_ms() / 1000;
        if claims.version != 1
            || claims.log_id != log_id
            || claims.partition != 0
            || claims.offset.0 > i64::MAX as u64
            || claims.scope != scope_hash(scope)?
            || claims.subject != digest(subject.as_bytes())
            || claims.issued_at > now.saturating_add(1)
            || claims.expires_at.checked_sub(claims.issued_at) != Some(LIFETIME)
        {
            return Err("invalid-cursor");
        }
        if now >= claims.expires_at {
            return Err("cursor-expired");
        }
        Ok(claims.offset.0)
    }
}

fn scope_hash(scope: &[String]) -> Result<String, &'static str> {
    Ok(digest(&serde_json::to_vec(scope).map_err(|_| "invalid-scope")?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_rotation_keeps_previous_gateway_cursors_valid() {
        let old = serde_json::json!({"secret":URL_SAFE_NO_PAD.encode([1;32]),"retain_until":u64::MAX});
        let new = serde_json::json!({"secret":URL_SAFE_NO_PAD.encode([2;32]),"retain_until":u64::MAX});
        let original: CursorKeys =
            serde_json::from_value(serde_json::json!({"current":"old","keys":{"old":old}})).unwrap();
        let staged: CursorKeys =
            serde_json::from_value(serde_json::json!({"current":"old","keys":{"old":old,"new":new}})).unwrap();
        let active: CursorKeys =
            serde_json::from_value(serde_json::json!({"current":"new","keys":{"old":old,"new":new}})).unwrap();
        let scope = vec!["/topic/v1/test".into()];
        let before = original.sign("log", 42, &scope, "subject").unwrap();
        assert_eq!(active.verify(&before, "log", &scope, "subject"), Ok(42));
        let after = active.sign("log", 43, &scope, "subject").unwrap();
        assert_eq!(staged.verify(&after, "log", &scope, "subject"), Ok(43));
        assert!(original.verify(&after, "log", &scope, "subject").is_err());
        assert!(active.ready());
        let expiring: CursorKeys = serde_json::from_value(serde_json::json!({"current":"old","keys":{"old":{"secret":URL_SAFE_NO_PAD.encode([1;32]),"retain_until":now_ms()/1000+10}}})).unwrap();
        assert!(!expiring.ready());
    }

    #[test]
    fn cursor_signature_binds_identity_scope_and_large_position() {
        let keys: CursorKeys = serde_json::from_value(serde_json::json!({"current":"test","keys":{"test":{"secret":URL_SAFE_NO_PAD.encode([7; 32]),"retain_until":u64::MAX}}})).unwrap();
        keys.validate().unwrap();
        let scope = vec!["/topic/v1/test".to_owned()];
        let cursor = keys.sign("log", i64::MAX as u64, &scope, "account").unwrap();
        assert_eq!(keys.verify(&cursor, "log", &scope, "account"), Ok(i64::MAX as u64));
        assert!(keys.verify(&cursor, "other", &scope, "account").is_err());
        assert!(keys.verify(&cursor, "log", &scope, "other").is_err());
        assert!(keys.verify(&cursor, "log", &["extra".into()], "account").is_err());
        let (payload, signature) = cursor.split_once('.').unwrap();
        let mut claims: Claims = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).unwrap()).unwrap();
        claims.offset = Number(1);
        let changed = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap());
        assert!(keys.verify(&format!("{changed}.{signature}"), "log", &scope, "account").is_err());
        claims.issued_at = 0;
        claims.expires_at = LIFETIME;
        let encoded = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap());
        let signature = URL_SAFE_NO_PAD.encode(hmac::sign(&keys.key("test").unwrap(), encoded.as_bytes()).as_ref());
        assert_eq!(keys.verify(&format!("{encoded}.{signature}"), "log", &scope, "account"), Err("cursor-expired"));
    }
}
