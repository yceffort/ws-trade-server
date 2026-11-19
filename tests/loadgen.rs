mod temp_file;

use serde_json::Value;
use std::process::Command;
use temp_file::TempFile;
use ws_trade_server::{Config, start};

// loadgen 실행 파일을 돌려 결과 JSON을 받는다. 블로킹 호출이라 gateway가 도는 런타임을 막지 않게 따로 돌린다.
async fn run_loadgen(name: &str, queue_capacity: usize, args: &[&str]) -> Value {
    let ring = TempFile::new(name);
    let config =
        Config { addr: "127.0.0.1:0".into(), ring: Some(ring.path().into()), queue_capacity, ..Config::default() };
    let (addr, _hub) = start(config).await.unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_loadgen"));
    command.args(["--url", &format!("ws://{addr}/ws"), "--ring", ring.path().to_str().unwrap()]).args(args);
    let output = tokio::task::spawn_blocking(move || command.output().unwrap()).await.unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    serde_json::from_slice(&output.stdout).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn loadgen_measures_delivery_and_latency() {
    let report = run_loadgen(
        "loadgen-basic",
        1024,
        &["--connections", "20", "--probes", "5", "--stalled", "0", "--rate", "50", "--warmup", "1", "--seconds", "2"],
    )
    .await;
    assert_eq!(report["connect"]["failed"], 0);
    assert_eq!(report["connect"]["ready"], 20);
    assert_eq!(report["seq_gap_clients"], 0);
    assert_eq!(report["unexpected_disconnects"], 0);
    assert!(report["messages"]["total"].as_u64().unwrap() > 0);
    let latency = &report["latency_us"];
    assert!(latency["samples"].as_u64().unwrap() > 0, "{report}");
    assert_eq!(latency["unmatched"], 0);
    assert_eq!(latency["unparsed"], 0);
    let (p50, p99, max) =
        (latency["p50"].as_u64().unwrap(), latency["p99"].as_u64().unwrap(), latency["max"].as_u64().unwrap());
    assert!(p50 <= p99 && p99 <= max, "{latency}");
}

// 송신 큐를 256으로 줄이고 초당 3000건을 쓴다. 읽지 않는 연결은 커널 버퍼가 찬 뒤 큐까지 차서 끊기고, 읽는 연결은 큐가 비어 있어 남는다.
#[tokio::test(flavor = "multi_thread")]
async fn stalled_clients_are_dropped_and_the_rest_are_unaffected() {
    let report = run_loadgen(
        "loadgen-stalled",
        256,
        &[
            "--connections",
            "10",
            "--probes",
            "2",
            "--stalled",
            "3",
            "--rate",
            "1000",
            "--warmup",
            "1",
            "--seconds",
            "2",
        ],
    )
    .await;
    assert_eq!(report["clients"]["stalled"], 3);
    assert_eq!(report["stalled_kicked"], 3, "{report}");
    assert_eq!(report["seq_gap_clients"], 0, "{report}");
    assert_eq!(report["unexpected_disconnects"], 0, "{report}");
    assert!(report["messages"]["measured"].as_u64().unwrap() > 0);
}
