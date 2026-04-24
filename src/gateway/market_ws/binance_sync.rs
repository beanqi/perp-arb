use std::collections::HashMap;

use crossbeam_channel::Sender;
use reqwest::Client;
use serde::Deserialize;
use tracing::warn;

use crate::{
    config::model::{Exchange, MarketKey},
    engine::{
        book::{PriceLevel, RawDepthMessage, decode_raw_depth},
        types::ShardEvent,
    },
    gateway::market_ws::MarketWsRuntime,
};

const DEPTH_LIMIT: u16 = 1000;
const FAPI_BASE: &str = "https://fapi.binance.com";

pub(super) struct DepthSynchronizer {
    runtime: MarketWsRuntime,
    client: Client,
    states: HashMap<String, SymbolSyncState>,
}

impl DepthSynchronizer {
    pub(super) fn new(runtime: &MarketWsRuntime, client: Client) -> Self {
        let states = runtime
            .subscribed_symbols
            .iter()
            .map(|symbol| (symbol.clone(), SymbolSyncState::new()))
            .collect();
        Self {
            runtime: runtime.clone(),
            client,
            states,
        }
    }

    pub(super) async fn handle_payload(
        &mut self,
        payload: &[u8],
        shard_tx: &Sender<ShardEvent>,
    ) -> Result<(), String> {
        let Some(message) = decode_raw_depth(self.runtime.exchange, payload) else {
            return Ok(());
        };
        let symbol = message.market().symbol.clone();
        let Some(state) = self.states.get_mut(&symbol) else {
            return Ok(());
        };
        state.buffer(message);

        if !state.ready {
            self.rebuild_symbol(&symbol, shard_tx).await?;
            return Ok(());
        }

        if state.has_sequence_gap() {
            warn!(
                "market ws {} binance symbol={} sequence gap detected; rebuilding snapshot",
                self.runtime.connection_id, symbol
            );
            state.ready = false;
            self.rebuild_symbol(&symbol, shard_tx).await?;
            return Ok(());
        }

        while let Some(message) = state.pop_ready_delta() {
            send_depth_message(&self.runtime, shard_tx, message)?;
        }
        Ok(())
    }

    async fn rebuild_symbol(
        &mut self,
        symbol: &str,
        shard_tx: &Sender<ShardEvent>,
    ) -> Result<(), String> {
        let snapshot = fetch_snapshot(&self.client, symbol).await?;
        let last_update_id = snapshot.last_update_id;
        let state = self
            .states
            .get_mut(symbol)
            .ok_or_else(|| format!("missing sync state for {symbol}"))?;
        state.discard_stale(last_update_id);

        let Some(first_delta_index) = state.find_snapshot_bridge(last_update_id) else {
            return Ok(());
        };

        send_depth_message(
            &self.runtime,
            shard_tx,
            RawDepthMessage::Snapshot {
                market: MarketKey::new(Exchange::BinanceUsdM, symbol.to_owned()),
                sequence: Some(last_update_id),
                bids: snapshot.bids.into_iter().filter_map(parse_snapshot_level).collect(),
                asks: snapshot.asks.into_iter().filter_map(parse_snapshot_level).collect(),
            },
        )?;

        for delta in state.mark_ready_and_drain_from(first_delta_index) {
            send_depth_message(&self.runtime, shard_tx, delta)?;
        }
        Ok(())
    }
}

struct SymbolSyncState {
    ready: bool,
    last_sequence: Option<u64>,
    pending: Vec<RawDepthMessage>,
}

impl SymbolSyncState {
    fn new() -> Self {
        Self {
            ready: false,
            last_sequence: None,
            pending: Vec::new(),
        }
    }

    fn buffer(&mut self, message: RawDepthMessage) {
        self.pending.push(message);
    }

    fn discard_stale(&mut self, last_update_id: u64) {
        self.pending.retain(|message| match message {
            RawDepthMessage::Delta { sequence, .. } => sequence.is_none_or(|value| value > last_update_id),
            RawDepthMessage::Snapshot { .. } => false,
        });
    }

    fn find_snapshot_bridge(&self, last_update_id: u64) -> Option<usize> {
        self.pending.iter().position(|message| match message {
            RawDepthMessage::Delta {
                first_sequence,
                sequence,
                ..
            } => {
                let first = first_sequence.unwrap_or(u64::MAX);
                let current = sequence.unwrap_or(0);
                first <= last_update_id + 1 && current >= last_update_id + 1
            }
            RawDepthMessage::Snapshot { .. } => false,
        })
    }

    fn mark_ready_and_drain_from(&mut self, index: usize) -> Vec<RawDepthMessage> {
        self.ready = true;
        self.pending.drain(..index).for_each(drop);
        self.record_pending_sequences();
        self.pending.drain(..).collect()
    }

    fn pop_ready_delta(&mut self) -> Option<RawDepthMessage> {
        if self.ready && !self.pending.is_empty() {
            let message = self.pending.remove(0);
            if let RawDepthMessage::Delta { sequence, .. } = &message {
                self.last_sequence = sequence.or(self.last_sequence);
            }
            Some(message)
        } else {
            None
        }
    }

    fn has_sequence_gap(&self) -> bool {
        let mut expected_previous = self.last_sequence;
        for message in &self.pending {
            let RawDepthMessage::Delta {
                previous_sequence,
                sequence,
                ..
            } = message
            else {
                continue;
            };
            if let (Some(expected), Some(previous)) = (expected_previous, previous_sequence) {
                if *previous != expected {
                    return true;
                }
            }
            expected_previous = sequence.or(expected_previous);
        }
        false
    }

    fn record_pending_sequences(&mut self) {
        for message in &self.pending {
            if let RawDepthMessage::Delta { sequence, .. } = message {
                self.last_sequence = sequence.or(self.last_sequence);
            }
        }
    }
}

fn send_depth_message(
    runtime: &MarketWsRuntime,
    shard_tx: &Sender<ShardEvent>,
    message: RawDepthMessage,
) -> Result<(), String> {
    let payload = encode_depth_message(message)?;
    shard_tx
        .try_send(ShardEvent::MarketWsRaw {
            connection_id: runtime.connection_id.clone(),
            exchange: runtime.exchange,
            payload,
        })
        .map_err(|error| format!("shard queue send failed: {error}"))
}

fn encode_depth_message(message: RawDepthMessage) -> Result<Vec<u8>, String> {
    let value = match message {
        RawDepthMessage::Snapshot {
            market,
            sequence,
            bids,
            asks,
        } => serde_json::json!({
            "s": market.symbol,
            "lastUpdateId": sequence,
            "bids": serialize_levels(bids),
            "asks": serialize_levels(asks),
        }),
        RawDepthMessage::Delta {
            market,
            first_sequence,
            previous_sequence,
            sequence,
            bids,
            asks,
        } => serde_json::json!({
            "e": "depthUpdate",
            "s": market.symbol,
            "U": first_sequence,
            "u": sequence,
            "pu": previous_sequence,
            "b": serialize_levels(bids),
            "a": serialize_levels(asks),
        }),
    };
    serde_json::to_vec(&value).map_err(|error| format!("depth encode failed: {error}"))
}

fn serialize_levels(levels: Vec<PriceLevel>) -> Vec<[String; 2]> {
    levels
        .into_iter()
        .map(|level| [level.price.to_string(), level.qty.to_string()])
        .collect()
}

async fn fetch_snapshot(client: &Client, symbol: &str) -> Result<BinanceSnapshot, String> {
    client
        .get(format!("{FAPI_BASE}/fapi/v1/depth"))
        .query(&[("symbol", symbol), ("limit", &DEPTH_LIMIT.to_string())])
        .send()
        .await
        .map_err(|error| format!("snapshot request failed for {symbol}: {error}"))?
        .error_for_status()
        .map_err(|error| format!("snapshot http error for {symbol}: {error}"))?
        .json::<BinanceSnapshot>()
        .await
        .map_err(|error| format!("snapshot decode failed for {symbol}: {error}"))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceSnapshot {
    last_update_id: u64,
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

fn parse_snapshot_level(level: [String; 2]) -> Option<PriceLevel> {
    Some(PriceLevel {
        price: level[0].parse().ok()?,
        qty: level[1].parse().ok()?,
    })
}
