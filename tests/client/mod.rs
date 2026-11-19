use crate::common::contract;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::{Error, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use ws_trade_server::feed::{Generator, Market, Symbol};
use ws_trade_server::hub::Hub;
use ws_trade_server::stomp::{self, Frame, Incoming};
use ws_trade_server::trade::to_message;
use ws_trade_server::{Config, start};

pub type Client = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub const KR: &str = "/topic/v1/kr/stock/trade/A005930";
pub const US: &str = "/topic/v1/us/stock/trade/US19990122001";

pub async fn start_server(heartbeat_ms: u64) -> (String, Hub) {
    let config = Config { addr: "127.0.0.1:0".into(), heartbeat_ms, ..Config::default() };
    let (addr, hub) = start(config).await.unwrap();
    (addr.to_string(), hub)
}

pub async fn open(addr: &str, path: &str, protocols: Option<&str>) -> Result<(Client, String), Error> {
    let mut request = format!("ws://{addr}{path}").into_client_request().unwrap();
    if let Some(protocols) = protocols {
        request.headers_mut().insert("sec-websocket-protocol", HeaderValue::from_str(protocols).unwrap());
    }
    let (client, response) = connect_async(request).await?;
    let selected = response.headers().get("sec-websocket-protocol").map(|v| v.to_str().unwrap().to_string());
    Ok((client, selected.unwrap_or_default()))
}

pub async fn recv_text(client: &mut Client) -> String {
    match timeout(Duration::from_secs(2), client.next()).await.unwrap().unwrap().unwrap() {
        Message::Text(text) => text.as_str().to_string(),
        other => panic!("텍스트가 아님: {other:?}"),
    }
}

pub async fn recv_frame(client: &mut Client) -> Frame {
    match stomp::parse(&recv_text(client).await) {
        Some(Incoming::Frame(frame)) => frame,
        _ => panic!("STOMP 프레임이 와야 한다"),
    }
}

pub async fn send(client: &mut Client, command: &str, headers: &[(&str, &str)]) {
    client.send(Message::text(stomp::serialize(command, headers, ""))).await.unwrap();
}

pub fn expected_pairs(value: &Value) -> Vec<(String, String)> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|p| (p[0].as_str().unwrap().to_string(), p[1].as_str().unwrap().to_string()))
        .collect()
}

pub fn connect_headers(contract: &Value, connection_id: &str) -> Vec<(String, String)> {
    let mut headers = expected_pairs(&contract["clientFrames"][0]["headers"]);
    headers
        .iter_mut()
        .filter(|(key, _)| key == "connection-id")
        .for_each(|(_, value)| *value = connection_id.to_string());
    headers
}

pub fn contract_connection_id(contract: &Value) -> String {
    let headers = expected_pairs(&contract["clientFrames"][0]["headers"]);
    headers.into_iter().find(|(key, _)| key == "connection-id").unwrap().1
}

pub async fn connect_stomp(addr: &str, connection_id: &str) -> Client {
    let contract = contract();
    let (mut client, _) = open(addr, "/ws", Some("v12.stomp")).await.unwrap();
    let headers = connect_headers(&contract, connection_id);
    let headers: Vec<(&str, &str)> = headers.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    send(&mut client, "CONNECT", &headers).await;
    assert_eq!(recv_frame(&mut client).await.command, "CONNECTED");
    client
}

pub async fn subscribe(client: &mut Client, id: &str, destination: &str) {
    let receipt = format!("{id}-sub_receipt");
    send(client, "SUBSCRIBE", &[("id", id), ("receipt", &receipt), ("destination", destination)]).await;
    let frame = recv_frame(client).await;
    assert_eq!((frame.command.as_str(), frame.header("receipt-id")), ("RECEIPT", Some(receipt.as_str())));
}

pub fn sample_trade(market: Market, code: &str) -> (String, Arc<str>) {
    to_message(&Generator::new(Symbol { market, code: code.into() }).next_trade(1)).unwrap()
}
