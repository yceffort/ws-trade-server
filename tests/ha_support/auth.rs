use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde_json::{Value, json};
use std::{os::unix::fs::PermissionsExt, path::PathBuf, time::Duration};
use ws_trade_server::ha::PolicyConfig;

pub struct Provider {
    child: std::process::Child,
    pub directory: PathBuf,
    pub config: PolicyConfig,
}
impl Drop for Provider {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}
impl Provider {
    pub async fn start(topics: &[String]) -> Self {
        let directory = std::env::temp_dir().join(format!("wts-jwt-{}-{}", std::process::id(), super::now()));
        std::fs::create_dir(&directory).unwrap();
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
        let child =
            std::process::Command::new("node").arg("tests/ha-policy-provider.js").arg(&directory).spawn().unwrap();
        let mut provider = Self {
            child,
            directory,
            config: PolicyConfig {
                url: "https://localhost/policy".into(),
                issuer: "https://issuer.test".into(),
                audience: "wts-ha".into(),
                ca_file: None,
                credential_file: None,
                max_token_seconds: 900,
            },
        };
        let key: Value = serde_json::from_str(include_str!("../fixtures/ha-test-key1.json")).unwrap();
        let policy =
            json!({"version":"1","keys":[key],"subjects":{"alice":{"generation":"1","topics":topics}},"revoked":{}});
        std::fs::write(provider.directory.join("control.json"), serde_json::to_vec(&json!({"policy":policy})).unwrap())
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while !provider.directory.join("ready.json").is_file() {
                assert!(provider.child.try_wait().unwrap().is_none());
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        let ready: Value =
            serde_json::from_slice(&std::fs::read(provider.directory.join("ready.json")).unwrap()).unwrap();
        provider.config.url = ready["url"].as_str().unwrap().into();
        provider.config.ca_file = Some(ready["ca_file"].as_str().unwrap().into());
        let now = super::now() / 1000;
        let claims = json!({"iss":provider.config.issuer,"aud":provider.config.audience,"sub":"alice","jti":"oracle","iat":now,"nbf":now,"exp":now+600,"topics":topics});
        let header = json!({"alg":"RS256","kid":"test-1","typ":"at+jwt"});
        let message = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header).unwrap()),
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap())
        );
        let key = ring::signature::RsaKeyPair::from_pkcs8(include_bytes!("../fixtures/ha-test-key1.pk8")).unwrap();
        let mut signature = vec![0; key.public().modulus_len()];
        key.sign(
            &ring::signature::RSA_PKCS1_SHA256,
            &ring::rand::SystemRandom::new(),
            message.as_bytes(),
            &mut signature,
        )
        .unwrap();
        let token = provider.directory.join("token");
        std::fs::write(&token, format!("Bearer {message}.{}", URL_SAFE_NO_PAD.encode(signature))).unwrap();
        std::fs::set_permissions(token, std::fs::Permissions::from_mode(0o600)).unwrap();
        provider
    }
}
