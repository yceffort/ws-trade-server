#![cfg(unix)]
#[allow(dead_code)]
mod client;
#[allow(dead_code)]
mod common;
mod temp_file;

use client::*;
use serde_json::Value;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use temp_file::TempFile;

struct Gateway(Child);
impl Drop for Gateway {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn json_gateway_logs_internal_connection_ids_and_flushes_shutdown() {
    const SECRET: &str = "synthetic-sensitive-marker";
    let auth = TempFile::new("logging-auth");
    let log = TempFile::new("logging-output");
    let ring = TempFile::new("logging-ring");
    let expires = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() + 60;
    std::fs::write(auth.path(), format!(r#"[{{"token":"{SECRET}","expires_at":{expires}}}]"#)).unwrap();
    let output = std::fs::File::create(log.path()).unwrap();
    let mut gateway = Gateway(
        Command::new(env!("CARGO_BIN_EXE_gateway"))
            .args(["--addr", "127.0.0.1:0", "--auth-file"])
            .arg(auth.path())
            .arg("--ring")
            .arg(ring.path())
            .env("WTS_LOG_FORMAT", "json")
            .env("RUST_LOG", "trace")
            .stdout(Stdio::piped())
            .stderr(output)
            .spawn()
            .unwrap(),
    );
    let addr = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let text = std::fs::read_to_string(log.path()).unwrap();
            for line in text.lines() {
                if let Ok(row) = serde_json::from_str::<Value>(line)
                    && row["fields"]["event"] == "gateway_started"
                {
                    return row["fields"]["addr"].as_str().unwrap().to_string();
                }
            }
            assert!(gateway.0.try_wait().unwrap().is_none(), "{text}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let (mut socket, _) = open(&addr, "/ws", Some("v12.stomp")).await.unwrap();
    let token = format!("Bearer {SECRET}");
    send(&mut socket, "CONNECT", &[("connection-id", SECRET), ("authorization", &token)]).await;
    assert_eq!(recv_frame(&mut socket).await.command, "CONNECTED");
    send(&mut socket, "DISCONNECT", &[]).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if std::fs::read_to_string(log.path()).unwrap().contains("connection_closed") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(Command::new("kill").args(["-TERM", &gateway.0.id().to_string()]).status().unwrap().success());
    tokio::time::timeout(Duration::from_secs(5), async {
        while gateway.0.try_wait().unwrap().is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let text = std::fs::read_to_string(log.path()).unwrap();
    assert!(!text.contains(SECRET));
    let rows: Vec<Value> = text.lines().map(|line| serde_json::from_str(line).unwrap()).collect();
    assert!(rows.iter().any(|row| row["fields"]["event"] == "shutdown_complete"));
    let closed = rows.iter().find(|row| row["fields"]["event"] == "connection_closed").unwrap();
    assert_eq!(closed["fields"]["reason"], "client_disconnect");
    assert!(closed["fields"]["conn"].as_u64().is_some());
    use std::io::Read;
    let mut stdout = String::new();
    gateway.0.stdout.take().unwrap().read_to_string(&mut stdout).unwrap();
    assert!(stdout.is_empty());
}
