mod client;
mod common;

use client::*;
use common::{assert_body_shape, contract};
use std::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::Error;
use ws_trade_server::feed::{Generator, Market, Symbol};
use ws_trade_server::stomp::{self, Incoming};
use ws_trade_server::trade::to_message;

#[tokio::test]
async fn handshake_selects_v12_and_rejects_other_paths() {
    let contract = contract();
    let offered = contract["handshake"]["offeredProtocols"].as_str().unwrap();
    let (addr, _hub) = start_server(5000).await;

    let (_client, selected) = open(&addr, "/ws", Some(offered)).await.unwrap();
    assert_eq!(selected, contract["handshake"]["selectedProtocol"].as_str().unwrap());

    for (path, protocols, status) in [("/other", Some(offered), 404), ("/ws", Some("v10.stomp"), 400)] {
        let Err(Error::Http(response)) = open(&addr, path, protocols).await else { panic!("거절되어야 한다") };
        assert_eq!(response.status().as_u16(), status);
    }
}

#[tokio::test]
async fn connect_gets_connected_in_contract_order_ending_with_nul() {
    let contract = contract();
    let (addr, _hub) = start_server(5000).await;
    let (mut client, _) = open(&addr, "/ws", Some("v12.stomp")).await.unwrap();
    let headers = connect_headers(&contract, &contract_connection_id(&contract));
    let headers: Vec<(&str, &str)> = headers.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    send(&mut client, "CONNECT", &headers).await;

    let text = recv_text(&mut client).await;
    assert!(text.ends_with('\0'));
    let Some(Incoming::Frame(frame)) = stomp::parse(&text) else { panic!() };
    assert_eq!(frame.command, "CONNECTED");
    assert_eq!(frame.headers, expected_pairs(&contract["server"]["connected"]));
}

#[tokio::test]
async fn frame_larger_than_read_buffer_is_still_accepted() {
    let (addr, _hub) = start_server(5000).await;
    let (mut client, _) = open(&addr, "/ws", Some("v12.stomp")).await.unwrap();
    let mut headers = connect_headers(&contract(), "big");
    headers.iter_mut().filter(|(key, _)| key == "authorization").for_each(|(_, value)| *value = "x".repeat(16 << 10));
    let headers: Vec<(&str, &str)> = headers.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    send(&mut client, "CONNECT", &headers).await;
    assert_eq!(recv_frame(&mut client).await.command, "CONNECTED");
}

#[tokio::test]
async fn every_contract_subscribe_gets_contract_shaped_receipt() {
    let contract = contract();
    let (addr, _hub) = start_server(5000).await;
    let mut client = connect_stomp(&addr, &contract_connection_id(&contract)).await;

    let subscribes: Vec<_> =
        contract["clientFrames"].as_array().unwrap().iter().filter(|f| f["command"] == "SUBSCRIBE").collect();
    for frame in &subscribes {
        let headers = expected_pairs(&frame["headers"]);
        let headers: Vec<(&str, &str)> = headers.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        send(&mut client, "SUBSCRIBE", &headers).await;
    }
    for frame in subscribes {
        let receipt_id = expected_pairs(&frame["headers"]).into_iter().find(|(k, _)| k == "receipt").unwrap().1;
        let expected: Vec<_> = expected_pairs(&contract["server"]["receipt"][0])
            .into_iter()
            .map(|(k, v)| (k, v.replace("{receipt}", &receipt_id)))
            .collect();
        let text = recv_text(&mut client).await;
        assert!(text.ends_with('\0'));
        let Some(Incoming::Frame(receipt)) = stomp::parse(&text) else { panic!() };
        assert_eq!((receipt.command.as_str(), receipt.headers), ("RECEIPT", expected));
    }
}

#[tokio::test]
async fn message_frames_match_contract_headers_and_bodies() {
    let contract = contract();
    let (addr, hub) = start_server(5000).await;
    let mut client = connect_stomp(&addr, &contract_connection_id(&contract)).await;
    subscribe(&mut client, "0", KR).await;
    subscribe(&mut client, "1", US).await;
    for (market, code, topic) in [(Market::Kr, "A005930", KR), (Market::Us, "US19990122001", US)] {
        let (_, body) = sample_trade(market, code);
        hub.publish(topic.to_string(), body).await;
    }

    let expected_order: Vec<&str> = contract["server"]["message"]["headerOrders"][0]
        .as_array()
        .unwrap()
        .iter()
        .map(|k| k.as_str().unwrap())
        .collect();
    for pattern in ["/topic/v1/kr/stock/trade/{code}", "/topic/v1/us/stock/trade/{code}"] {
        let message = recv_frame(&mut client).await;
        let names: Vec<&str> = message.headers.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(names, expected_order);
        assert_eq!(message.header("content-length"), Some(message.body.len().to_string().as_str()));
        assert_eq!(message.header("content-type"), contract["server"]["message"]["contentTypes"][0].as_str());
        assert_body_shape(&contract, pattern, &message.body);
    }
}

#[tokio::test]
async fn message_ids_count_from_one_across_topics() {
    let contract = contract();
    let id = contract_connection_id(&contract);
    let (addr, hub) = start_server(5000).await;
    let mut client = connect_stomp(&addr, &id).await;
    subscribe(&mut client, "0", KR).await;
    subscribe(&mut client, "1", US).await;
    for n in 0..6 {
        let (market, code, topic) =
            if n % 2 == 0 { (Market::Kr, "A005930", KR) } else { (Market::Us, "US19990122001", US) };
        hub.publish(topic.to_string(), sample_trade(market, code).1).await;
    }
    for n in 1..=6 {
        let message = recv_frame(&mut client).await;
        assert_eq!(message.header("message-id"), Some(format!("{id}-{n}").as_str()));
    }
}

#[tokio::test]
async fn duplicate_subscriptions_each_get_the_trade_once() {
    let (addr, hub) = start_server(5000).await;
    let mut client = connect_stomp(&addr, "dup").await;
    subscribe(&mut client, "a", KR).await;
    subscribe(&mut client, "b", KR).await;
    hub.publish(KR.to_string(), sample_trade(Market::Kr, "A005930").1).await;

    let (first, second) = (recv_frame(&mut client).await, recv_frame(&mut client).await);
    let mut subscriptions = [first.header("subscription").unwrap(), second.header("subscription").unwrap()];
    subscriptions.sort();
    assert_eq!(subscriptions, ["a", "b"]);
    assert_eq!(first.body, second.body);
}

#[tokio::test(flavor = "multi_thread")]
async fn receipt_always_precedes_first_message() {
    let (addr, hub) = start_server(5000).await;
    let publisher = tokio::spawn(async move {
        let trade = sample_trade(Market::Kr, "A005930");
        loop {
            hub.publish(trade.0.clone(), trade.1.clone()).await;
            tokio::task::yield_now().await;
        }
    });
    for n in 0..30 {
        let mut client = connect_stomp(&addr, &format!("race-{n}")).await;
        send(&mut client, "SUBSCRIBE", &[("id", "0"), ("receipt", "r"), ("destination", KR)]).await;
        assert_eq!(recv_frame(&mut client).await.command, "RECEIPT");
        assert_eq!(recv_frame(&mut client).await.command, "MESSAGE");
    }
    publisher.abort();
}

#[tokio::test]
async fn heartbeats_keep_coming_while_data_flows() {
    let (addr, hub) = start_server(100).await;
    let mut client = connect_stomp(&addr, "beat").await;
    subscribe(&mut client, "0", KR).await;
    let publisher = tokio::spawn(async move {
        let trade = sample_trade(Market::Kr, "A005930");
        loop {
            hub.publish(trade.0.clone(), trade.1.clone()).await;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });

    // (도착 시각, 그때까지 받은 MESSAGE 수). 전체에 마감을 걸어 heartbeat가 끊기면 hang 대신 실패하게 한다.
    let mut beats = Vec::new();
    let mut messages = 0;
    let collected = tokio::time::timeout(Duration::from_secs(3), async {
        while beats.len() < 4 {
            match stomp::parse(&recv_text(&mut client).await) {
                Some(Incoming::Heartbeat) => beats.push((Instant::now(), messages)),
                Some(Incoming::Frame(_)) => messages += 1,
                None => panic!("파싱 실패"),
            }
        }
    })
    .await;
    publisher.abort();
    collected.expect("heartbeat 4개가 3초 안에 와야 한다");
    for pair in beats.windows(2) {
        let gap = pair[1].0 - pair[0].0;
        assert!(gap > Duration::from_millis(70) && gap < Duration::from_millis(300), "{gap:?}");
        assert!(pair[1].1 > pair[0].1, "heartbeat 사이에 MESSAGE도 흘러야 한다");
    }
}

#[tokio::test]
async fn two_connections_each_count_their_own_sequence() {
    let (addr, hub) = start_server(5000).await;
    let mut first = connect_stomp(&addr, "one").await;
    let mut second = connect_stomp(&addr, "two").await;
    subscribe(&mut first, "0", KR).await;
    subscribe(&mut second, "0", KR).await;
    hub.publish(KR.to_string(), sample_trade(Market::Kr, "A005930").1).await;

    assert_eq!(recv_frame(&mut first).await.header("message-id"), Some("one-1"));
    assert_eq!(recv_frame(&mut second).await.header("message-id"), Some("two-1"));
}

#[test]
fn generated_bodies_match_contract_shape() {
    let contract = contract();
    for (market, code, pattern) in [
        (Market::Kr, "A005930", "/topic/v1/kr/stock/trade/{code}"),
        (Market::Us, "US19990122001", "/topic/v1/us/stock/trade/{code}"),
    ] {
        let mut generator = Generator::new(Symbol { market, code: code.into() });
        for seq in 1..=200 {
            let (topic, body) = to_message(&generator.next_trade(seq)).unwrap();
            assert!(topic.ends_with(code));
            assert_body_shape(&contract, pattern, &body);
        }
    }
}
