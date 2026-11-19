// 테스트 파일마다 일부만 쓰므로 안 쓰는 함수 경고를 막는다.
#[allow(dead_code)]
mod client;
mod common;
mod temp_file;

use client::*;
use common::{assert_body_shape, contract};
use std::process::{Child, Command};
use std::time::Duration;
use temp_file::TempFile;
use tokio::time::timeout;
use ws_trade_server::{Config, start};

// 테스트가 실패해도 feed 프로세스를 반드시 종료한다.
struct Feed(Child);

impl Drop for Feed {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn trades_from_feed_process_reach_client_through_the_ring() {
    let contract = contract();
    let ring = TempFile::new("feed-to-client");
    let ring_arg = ring.path().to_str().unwrap();
    let _feed =
        Feed(Command::new(env!("CARGO_BIN_EXE_feed")).args(["--ring", ring_arg, "--rate", "50"]).spawn().unwrap());
    let config = Config { addr: "127.0.0.1:0".into(), ring: Some(ring.path().into()), ..Config::default() };
    let (addr, _hub) = start(config).await.unwrap();

    let mut client = connect_stomp(&addr.to_string(), "feed").await;
    subscribe(&mut client, "kr", KR).await;
    subscribe(&mut client, "us", US).await;
    let mut pending = vec!["kr", "us"];
    let received = timeout(Duration::from_secs(10), async {
        while !pending.is_empty() {
            let message = recv_frame(&mut client).await;
            let subscription = message.header("subscription").unwrap();
            let pattern = match subscription {
                "kr" => "/topic/v1/kr/stock/trade/{code}",
                _ => "/topic/v1/us/stock/trade/{code}",
            };
            assert_eq!(message.command, "MESSAGE");
            assert_body_shape(&contract, pattern, &message.body);
            pending.retain(|id| *id != subscription);
        }
    })
    .await;
    received.expect("국내와 미국 MESSAGE가 10초 안에 와야 한다");
}
