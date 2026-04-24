use std::time::Duration;

use crossbeam_channel::Sender;
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use tokio::time::MissedTickBehavior;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

use crate::{
    engine::types::ShardEvent,
    gateway::market_ws::{MarketWsRuntime, binance_sync::DepthSynchronizer},
};

const RECONNECT_DELAY: Duration = Duration::from_secs(3);
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);
const FSTREAM_WS: &str = "wss://fstream.binance.com/stream?streams=";

pub async fn run(runtime: MarketWsRuntime, shard_tx: Sender<ShardEvent>) {
    let client = Client::new();
    loop {
        if let Err(error) = run_once(&runtime, &shard_tx, &client).await {
            warn!(
                "market ws {} exchange={} disconnected: {}",
                runtime.connection_id, runtime.exchange, error
            );
        }
        tokio::time::sleep(RECONNECT_DELAY).await;
    }
}

async fn run_once(
    runtime: &MarketWsRuntime,
    shard_tx: &Sender<ShardEvent>,
    client: &Client,
) -> Result<(), String> {
    let stream_url = stream_url(&runtime.subscribed_symbols)?;
    info!(
        "market ws {} -> {} binance connecting symbols=[{}]",
        runtime.connection_id,
        runtime.shard_id,
        runtime.subscribed_symbols.join(", ")
    );

    let (ws_stream, _) = connect_async(&stream_url)
        .await
        .map_err(|error| format!("connect failed: {error}"))?;
    let (mut write, mut read) = ws_stream.split();
    let mut keepalive = tokio::time::interval(KEEPALIVE_INTERVAL);
    keepalive.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut synchronizer = DepthSynchronizer::new(runtime, client.clone());

    loop {
        tokio::select! {
            _ = keepalive.tick() => {
                write.send(Message::Ping(Vec::new().into())).await.map_err(|error| format!("ping failed: {error}"))?;
            }
            message = read.next() => {
                let message = message.ok_or_else(|| "websocket stream ended".to_owned())?
                    .map_err(|error| format!("websocket read failed: {error}"))?;
                match message {
                    Message::Text(text) => synchronizer.handle_payload(text.as_bytes(), shard_tx).await?,
                    Message::Binary(bytes) => synchronizer.handle_payload(&bytes, shard_tx).await?,
                    Message::Ping(payload) => write.send(Message::Pong(payload)).await.map_err(|error| format!("pong failed: {error}"))?,
                    Message::Close(frame) => return Err(format!("remote close: {frame:?}")),
                    Message::Pong(_) | Message::Frame(_) => {}
                }
            }
        }
    }
}

fn stream_url(symbols: &[String]) -> Result<String, String> {
    if symbols.is_empty() {
        return Err("no subscribed symbols".to_owned());
    }
    let streams = symbols
        .iter()
        .map(|symbol| format!("{}@depth@100ms", symbol.to_lowercase()))
        .collect::<Vec<_>>()
        .join("/");
    Ok(format!("{FSTREAM_WS}{streams}"))
}
