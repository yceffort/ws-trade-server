use super::*;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde_json::{Value, json};

fn auth() -> SharedAuth {
    SharedAuth::new(PolicyConfig {
        url: "https://localhost/policy".into(),
        issuer: "https://issuer.test".into(),
        audience: "wts-ha".into(),
        ca_file: None,
        credential_file: None,
        max_token_seconds: 900,
    })
    .unwrap()
}
fn key(index: usize) -> Value {
    serde_json::from_str(if index == 1 {
        include_str!("../../../../tests/fixtures/ha-test-key1.json")
    } else {
        include_str!("../../../../tests/fixtures/ha-test-key2.json")
    })
    .unwrap()
}
fn policy(version: u64) -> Value {
    json!({"version":version.to_string(),"keys":[key(1)],"subjects":{"alice":{"generation":"1","topics":["/topic/v1/a","/topic/v1/b"]}},"revoked":{}})
}
fn install(auth: &SharedAuth, policy: Value) -> Result<(), &'static str> {
    let now = now_ms() / 1000;
    auth.install(
        &serde_json::to_vec(&json!({"generated_at":now,"valid_until":now+55,"policy":policy})).unwrap(),
        Instant::now(),
    )
}
fn claims() -> Value {
    let now = now_ms() / 1000;
    json!({"iss":"https://issuer.test","aud":"wts-ha","sub":"alice","jti":"test-token","iat":now,"nbf":now,"exp":now+300,"topics":["/topic/v1/a"]})
}
fn token(claims: Value, index: usize, alg: &str) -> String {
    let private = if index == 1 {
        include_bytes!("../../../../tests/fixtures/ha-test-key1.pk8").as_slice()
    } else {
        include_bytes!("../../../../tests/fixtures/ha-test-key2.pk8").as_slice()
    };
    let key = ring::signature::RsaKeyPair::from_pkcs8(private).unwrap();
    let header = json!({"alg":alg,"kid":format!("test-{index}"),"typ":"at+jwt"});
    let message = format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header).unwrap()),
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap())
    );
    let mut signature = vec![0; key.public().modulus_len()];
    key.sign(&ring::signature::RSA_PKCS1_SHA256, &ring::rand::SystemRandom::new(), message.as_bytes(), &mut signature)
        .unwrap();
    format!("Bearer {message}.{}", URL_SAFE_NO_PAD.encode(signature))
}
fn expire(auth: &SharedAuth) {
    let old = auth.cache.borrow().clone().unwrap();
    auth.cache.send_replace(Some(Arc::new(Cache {
        policy: old.policy.clone(),
        generations: old.generations.clone(),
        observed: Instant::now() - Duration::from_secs(56),
        valid_until: old.valid_until,
    })));
}

#[tokio::test]
async fn signed_tokens_validate_algorithm_issuer_audience_expiry_and_scope() {
    let auth = auth();
    install(&auth, policy(1)).unwrap();
    let session = auth.authorize(&token(claims(), 1, "RS256")).unwrap();
    assert!(session.allows(&["/topic/v1/a".into()]));
    assert!(!session.allows(&["/topic/v1/b".into()]));
    for (field, value) in [
        ("iss", json!("other")),
        ("aud", json!("other")),
        ("sub", json!("unknown")),
        ("exp", json!(0)),
        ("iat", json!(now_ms() / 1000 + 100)),
        ("topics", json!(["/topic/v1/forbidden"])),
    ] {
        let mut claims = claims();
        claims[field] = value;
        assert!(auth.authorize(&token(claims, 1, "RS256")).is_err(), "{field}");
    }
    assert!(auth.authorize(&token(claims(), 1, "HS256")).is_err());
    let mut changed = token(claims(), 1, "RS256");
    changed.push('x');
    assert!(auth.authorize(&changed).is_err());
}

#[tokio::test]
async fn revocation_and_coalesced_scope_changes_close_existing_sessions() {
    let auth = auth();
    let original = policy(1);
    install(&auth, original.clone()).unwrap();
    let bearer = token(claims(), 1, "RS256");
    let mut session = auth.authorize(&bearer).unwrap();
    let mut reduced = policy(2);
    reduced["subjects"]["alice"] = json!({"generation":"2","topics":[]});
    install(&auth, reduced).unwrap();
    let mut restored = policy(3);
    restored["subjects"]["alice"]["generation"] = json!("3");
    install(&auth, restored).unwrap();
    assert_eq!(session.invalidated().await, "auth-revoked");
    let mut fresh = auth.authorize(&bearer).unwrap();
    let mut revoked = policy(4);
    revoked["subjects"]["alice"]["generation"] = json!("3");
    revoked["revoked"] = json!({"test-token":{"revoked_at":now_ms()/1000,"retain_until":now_ms()/1000+1000}});
    install(&auth, revoked).unwrap();
    assert_eq!(fresh.invalidated().await, "auth-revoked");
    assert!(auth.authorize(&bearer).is_err());
    assert_eq!(install(&auth, policy(5)), Err("revocation-retention-required"));
}

#[tokio::test]
async fn policy_expiry_fails_closed_on_old_pods_without_refreshing_old_versions() {
    let auth = auth();
    install(&auth, policy(2)).unwrap();
    let bearer = token(claims(), 1, "RS256");
    let mut session = auth.authorize(&bearer).unwrap();
    assert_eq!(install(&auth, policy(1)), Err("policy-rollback"));
    let mut conflicting = policy(2);
    conflicting["subjects"]["alice"]["topics"] = json!([]);
    assert_eq!(install(&auth, conflicting), Err("policy-version-conflict"));
    expire(&auth);
    assert!(!auth.fresh());
    assert!(matches!(auth.authorize(&bearer), Err("auth-policy-stale")));
    assert_eq!(session.invalidated().await, "auth-policy-stale");
    install(&auth, policy(2)).unwrap();
    assert!(auth.fresh());
}

#[tokio::test]
async fn key_prepublication_preserves_old_and_new_tokens_across_gateways() {
    let a = auth();
    let b = auth();
    install(&a, policy(1)).unwrap();
    install(&b, policy(1)).unwrap();
    let old = token(claims(), 1, "RS256");
    let new = token(claims(), 2, "RS256");
    assert!(b.authorize(&new).is_err());
    let mut overlap = policy(2);
    overlap["keys"] = json!([key(1), key(2)]);
    install(&a, overlap.clone()).unwrap();
    install(&b, overlap).unwrap();
    for auth in [&a, &b] {
        assert!(auth.authorize(&old).is_ok());
        assert!(auth.authorize(&new).is_ok());
    }
    let mut removed = policy(3);
    removed["keys"] = json!([key(2)]);
    assert_eq!(install(&a, removed), Err("key-overlap-required"));
    let mut changed = policy(3);
    changed["keys"][0]["n"] = key(2)["n"].clone();
    assert_eq!(install(&a, changed), Err("key-overlap-required"));
}

struct Provider {
    child: std::process::Child,
    directory: std::path::PathBuf,
}
impl Drop for Provider {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

#[tokio::test]
#[ignore = "Node와 openssl을 사용하는 독립 HTTPS 정책 공급자 시험"]
async fn https_policy_revocation_and_provider_outage_obey_the_sixty_second_bound() {
    let directory = std::env::temp_dir().join(format!("wts-policy-{}-{}", std::process::id(), now_ms()));
    std::fs::create_dir(&directory).unwrap();
    let child = std::process::Command::new("node").arg("tests/ha-policy-provider.js").arg(&directory).spawn().unwrap();
    let mut provider = Provider { child, directory };
    let control = provider.directory.join("control.json");
    std::fs::write(&control, serde_json::to_vec(&json!({"policy":policy(1)})).unwrap()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !provider.directory.join("ready.json").is_file() {
            assert!(provider.child.try_wait().unwrap().is_none());
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let settings: Value =
        serde_json::from_slice(&std::fs::read(provider.directory.join("ready.json")).unwrap()).unwrap();
    let mut config = auth().config.as_ref().clone();
    config.url = settings["url"].as_str().unwrap().into();
    config.ca_file = Some(settings["ca_file"].as_str().unwrap().into());
    let a = SharedAuth::new(config.clone()).unwrap();
    let b = SharedAuth::new(config).unwrap();
    a.fetch().await.unwrap();
    b.fetch().await.unwrap();
    let bearer = token(claims(), 1, "RS256");
    let mut first = a.authorize(&bearer).unwrap();
    let mut second = b.authorize(&bearer).unwrap();
    let mut revoked = policy(2);
    revoked["revoked"] = json!({"test-token":{"revoked_at":now_ms()/1000,"retain_until":now_ms()/1000+1000}});
    std::fs::write(&control, serde_json::to_vec(&json!({"policy":revoked})).unwrap()).unwrap();
    let workers = [a.spawn(), b.spawn()];
    let reasons =
        tokio::time::timeout(Duration::from_secs(8), async { tokio::join!(first.invalidated(), second.invalidated()) })
            .await
            .unwrap();
    assert_eq!(reasons, ("auth-revoked", "auth-revoked"));
    let mut other = claims();
    other["jti"] = json!("other");
    let mut session = a.authorize(&token(other, 1, "RS256")).unwrap();
    std::fs::write(&control, r#"{"failure":true}"#).unwrap();
    assert_eq!(a.fetch().await, Err("policy-unavailable"));
    assert!(a.fresh());
    std::fs::write(&control, r#"{"slow":true}"#).unwrap();
    let started = Instant::now();
    assert_eq!(a.fetch().await, Err("policy-unavailable"));
    assert!(started.elapsed() < Duration::from_secs(3));
    provider.child.kill().unwrap();
    provider.child.wait().unwrap();
    let outage = Instant::now();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(57), session.invalidated()).await.unwrap(),
        "auth-policy-stale"
    );
    assert!(outage.elapsed() < Duration::from_secs(60));
    assert!(!a.fresh());
    for worker in workers {
        worker.abort();
        let _ = worker.await;
    }
}

#[tokio::test]
async fn active_session_expires_without_policy_changes() {
    let auth = auth();
    install(&auth, policy(1)).unwrap();
    let mut claims = claims();
    claims["exp"] = json!(now_ms() / 1000 + 2);
    let mut session = auth.authorize(&token(claims, 1, "RS256")).unwrap();
    assert_eq!(tokio::time::timeout(Duration::from_secs(3), session.invalidated()).await.unwrap(), "auth-expired");
}

#[tokio::test]
async fn skipped_policy_versions_still_revoke_restored_subjects() {
    let auth = auth();
    install(&auth, policy(1)).unwrap();
    let bearer = token(claims(), 1, "RS256");
    let mut session = auth.authorize(&bearer).unwrap();
    let mut restored = policy(3);
    restored["subjects"]["alice"]["generation"] = json!("3");
    install(&auth, restored).unwrap();
    assert_eq!(session.invalidated().await, "auth-revoked");
    assert_eq!(install(&auth, policy(4)), Err("subject-generation-invalid"));
}

#[tokio::test]
async fn delayed_revocations_and_provider_recovery_use_revocation_time() {
    let auth = auth();
    install(&auth, policy(1)).unwrap();
    let bearer = token(claims(), 1, "RS256");
    let mut session = auth.authorize(&bearer).unwrap();
    let now = now_ms() / 1000;
    let mut delayed = policy(2);
    delayed["revoked"] = json!({"test-token":{"revoked_at":now-30,"retain_until":now-30+900}});
    install(&auth, delayed.clone()).unwrap();
    assert_eq!(session.invalidated().await, "auth-revoked");
    expire(&auth);
    install(&auth, delayed).unwrap();
    assert!(auth.fresh());
    assert!(auth.authorize(&bearer).is_err());
    assert_eq!(install(&auth, policy(3)), Err("revocation-retention-required"));
}
