use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use crossbeam_channel::Sender;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::time::MissedTickBehavior;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

use crate::{
    engine::{
        book::{RawDepthMessage, decode_raw_depth},
        types::ShardEvent,
    },
    gateway::market_ws::MarketWsRuntime,
};

const RECONNECT_DELAY: Duration = Duration::from_secs(3);
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);
const BYBIT_LINEAR_WS: &str = "wss://stream.bybit.com/v5/public/linear";
const ORDERBOOK_DEPTH: u16 = 1000;

pub async fn run(runtime: MarketWsRuntime, shard_tx: Sender<ShardEvent>) {
    loop {
        if let Err(error) = run_once(&runtime, &shard_tx).await {
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
) -> Result<(), String> {
    if runtime.subscribed_symbols.is_empty() {
        return Err("no subscribed symbols".to_owned());
    }

    info!(
        "market ws {} -> {} bybit connecting symbols=[{}]",
        runtime.connection_id,
        runtime.shard_id,
        runtime.subscribed_symbols.join(", ")
    );
    let (ws_stream, _) = connect_async(BYBIT_LINEAR_WS)
        .await
        .map_err(|error| format!("connect failed: {error}"))?;
    info!(
        "market ws {} -> {} bybit connected symbols=[{}]",
        runtime.connection_id,
        runtime.shard_id,
        runtime.subscribed_symbols.join(", ")
    );

    let (mut write, mut read) = ws_stream.split();
    write
        .send(Message::Text(subscription_message(&runtime.subscribed_symbols)?.into()))
        .await
        .map_err(|error| format!("subscribe failed: {error}"))?;

    let mut keepalive = tokio::time::interval(KEEPALIVE_INTERVAL);
    keepalive.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut synchronizer = BybitDepthSynchronizer::new(runtime);

    loop {
        tokio::select! {
            _ = keepalive.tick() => {
                write.send(Message::Text(json!({"op": "ping"}).to_string().into())).await.map_err(|error| format!("ping failed: {error}"))?;
            }
            message = read.next() => {
                let message = message.ok_or_else(|| "websocket stream ended".to_owned())?
                    .map_err(|error| format!("websocket read failed: {error}"))?;
                match message {
                    Message::Text(text) => synchronizer.handle_payload(text.as_bytes(), shard_tx)?,
                    Message::Binary(bytes) => synchronizer.handle_payload(&bytes, shard_tx)?,
                    Message::Ping(payload) => write.send(Message::Pong(payload)).await.map_err(|error| format!("pong failed: {error}"))?,
                    Message::Close(frame) => return Err(format!("remote close: {frame:?}")),
                    Message::Pong(_) | Message::Frame(_) => {}
                }
            }
        }
    }
}

fn subscription_message(symbols: &[String]) -> Result<String, String> {
    let args = symbols
        .iter()
        .map(|symbol| format!("orderbook.{ORDERBOOK_DEPTH}.{symbol}"))
        .collect::<Vec<_>>();
    serde_json::to_string(&json!({
        "op": "subscribe",
        "args": args,
    }))
    .map_err(|error| format!("subscribe encode failed: {error}"))
}

struct BybitDepthSynchronizer {
    runtime: MarketWsRuntime,
    states: HashMap<String, SymbolState>,
    seen_first_payload: bool,
}

impl BybitDepthSynchronizer {
    fn new(runtime: &MarketWsRuntime) -> Self {
        let states = runtime
            .subscribed_symbols
            .iter()
            .map(|symbol| (symbol.clone(), SymbolState::default()))
            .collect();
        Self {
            runtime: runtime.clone(),
            states,
            seen_first_payload: false,
        }
    }

    fn handle_payload(
        &mut self,
        payload: &[u8],
        shard_tx: &Sender<ShardEvent>,
    ) -> Result<(), String> {
        let received_at = Instant::now();
        let Some(message) = decode_raw_depth(self.runtime.exchange, payload) else {
            return Ok(());
        };
        if !self.seen_first_payload {
            info!(
                "market ws {} bybit first depth payload received",
                self.runtime.connection_id
            );
            self.seen_first_payload = true;
        }

        let symbol = message.market().symbol.clone();
        let Some(state) = self.states.get_mut(&symbol) else {
            return Ok(());
        };

        if !state.accept(&message)? {
            return Ok(());
        }

        let payload = payload.to_vec();
        let serialized_at = Instant::now();
        shard_tx
            .try_send(ShardEvent::MarketWsRaw {
                connection_id: self.runtime.connection_id.clone(),
                exchange: self.runtime.exchange,
                received_at,
                serialized_at,
                payload,
            })
            .map_err(|error| format!("shard queue send failed: {error}"))
    }
}

#[derive(Default)]
struct SymbolState {
    has_snapshot: bool,
    last_sequence: Option<u64>,
}

impl SymbolState {
    fn accept(&mut self, message: &RawDepthMessage) -> Result<bool, String> {
        match message {
            RawDepthMessage::Snapshot { sequence, .. } => {
                self.has_snapshot = true;
                self.last_sequence = *sequence;
                Ok(true)
            }
            RawDepthMessage::Delta { market, sequence, .. } => {
                if !self.has_snapshot {
                    return Ok(false);
                }

                let Some(current) = *sequence else {
                    return Ok(true);
                };
                if let Some(last) = self.last_sequence {
                    if current <= last {
                        return Ok(false);
                    }
                    if current > last + 1 {
                        return Err(format!(
                            "symbol={} sequence gap detected last={} current={}",
                            market.symbol, last, current
                        ));
                    }
                }
                self.last_sequence = Some(current);
                Ok(true)
            }
        }
    }
}
