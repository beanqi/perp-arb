use std::{collections::HashMap, time::Instant};

use crossbeam_channel::Sender;
use reqwest::Client;
use serde::Deserialize;
use tracing::{info, warn};

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
    seen_first_payload: bool,
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
            seen_first_payload: false,
        }
    }

    pub(super) async fn handle_payload(
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
                "market ws {} binance first depth payload received",
                self.runtime.connection_id
            );
            self.seen_first_payload = true;
        }
        let symbol = message.market().symbol.clone();
        let Some(state) = self.states.get_mut(&symbol) else {
            return Ok(());
        };
        state.buffer(message, received_at);

        if !state.ready {
            self.initialize_symbol(&symbol, shard_tx).await?;
            return Ok(());
        }

        if state.has_sequence_gap() {
            warn!(
                "market ws {} binance symbol={} sequence gap detected; rebuilding snapshot",
                self.runtime.connection_id, symbol
            );
            self.reset_symbol(&symbol);
            self.initialize_symbol(&symbol, shard_tx).await?;
            return Ok(());
        }

        while let Some(message) = state.pop_ready_delta() {
            send_depth_message(&self.runtime, shard_tx, message.message, message.received_at)?;
        }
        Ok(())
    }

    async fn initialize_symbol(
        &mut self,
        symbol: &str,
        shard_tx: &Sender<ShardEvent>,
    ) -> Result<(), String> {
        if self
            .states
            .get(symbol)
            .ok_or_else(|| format!("missing sync state for {symbol}"))?
            .snapshot
            .is_none()
        {
            let snapshot = fetch_snapshot(&self.client, symbol).await?;
            info!(
                "market ws {} binance symbol={} snapshot fetched last_update_id={} bids={} asks={}",
                self.runtime.connection_id,
                symbol,
                snapshot.last_update_id,
                snapshot.bids.len(),
                snapshot.asks.len()
            );
            let state = self
                .states
                .get_mut(symbol)
                .ok_or_else(|| format!("missing sync state for {symbol}"))?;
            state.snapshot = Some(snapshot);
        }

        let state = self
            .states
            .get_mut(symbol)
            .ok_or_else(|| format!("missing sync state for {symbol}"))?;
        let last_update_id = state
            .snapshot
            .as_ref()
            .map(|snapshot| snapshot.last_update_id)
            .ok_or_else(|| format!("missing snapshot for {symbol}"))?;
        state.discard_stale(last_update_id);

        let Some(first_delta_index) = state.find_snapshot_bridge(last_update_id) else {
            warn!(
                "market ws {} binance symbol={} waiting snapshot bridge last_update_id={} pending_deltas={}",
                self.runtime.connection_id,
                symbol,
                last_update_id,
                state.pending.len()
            );
            return Ok(());
        };

        let snapshot = state
            .snapshot
            .take()
            .ok_or_else(|| format!("missing snapshot for {symbol}"))?;

        send_depth_message(
            &self.runtime,
            shard_tx,
            RawDepthMessage::Snapshot {
                market: MarketKey::new(Exchange::BinanceUsdM, symbol.to_owned()),
                sequence: Some(last_update_id),
                bids: snapshot.bids.into_iter().filter_map(parse_snapshot_level).collect(),
                asks: snapshot.asks.into_iter().filter_map(parse_snapshot_level).collect(),
            },
            Instant::now(),
        )?;

        for delta in state.mark_ready_and_drain_from(first_delta_index) {
            send_depth_message(&self.runtime, shard_tx, delta.message, delta.received_at)?;
        }
        info!(
            "market ws {} binance symbol={} depth synchronized last_update_id={}",
            self.runtime.connection_id,
            symbol,
            last_update_id
        );
        Ok(())
    }

    fn reset_symbol(&mut self, symbol: &str) {
        if let Some(state) = self.states.get_mut(symbol) {
            state.reset();
        }
    }
}

struct PendingDepthMessage {
    message: RawDepthMessage,
    received_at: Instant,
}

struct SymbolSyncState {
    ready: bool,
    last_sequence: Option<u64>,
    snapshot: Option<BinanceSnapshot>,
    pending: Vec<PendingDepthMessage>,
}

impl SymbolSyncState {
    fn new() -> Self {
        Self {
            ready: false,
            last_sequence: None,
            snapshot: None,
            pending: Vec::new(),
        }
    }

    fn buffer(&mut self, message: RawDepthMessage, received_at: Instant) {
        self.pending.push(PendingDepthMessage {
            message,
            received_at,
        });
    }

    fn discard_stale(&mut self, last_update_id: u64) {
        self.pending.retain(|message| match &message.message {
            RawDepthMessage::Delta { sequence, .. } => sequence.is_none_or(|value| value > last_update_id),
            RawDepthMessage::Snapshot { .. } => false,
        });
    }

    fn find_snapshot_bridge(&self, last_update_id: u64) -> Option<usize> {
        self.pending.iter().position(|message| match &message.message {
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

    fn mark_ready_and_drain_from(&mut self, index: usize) -> Vec<PendingDepthMessage> {
        self.ready = true;
        self.pending.drain(..index).for_each(drop);
        self.record_pending_sequences();
        self.pending.drain(..).collect()
    }

    fn pop_ready_delta(&mut self) -> Option<PendingDepthMessage> {
        if self.ready && !self.pending.is_empty() {
            let message = self.pending.remove(0);
            if let RawDepthMessage::Delta { sequence, .. } = &message.message {
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
            } = &message.message
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
            if let RawDepthMessage::Delta { sequence, .. } = &message.message {
                self.last_sequence = sequence.or(self.last_sequence);
            }
        }
    }

    fn reset(&mut self) {
        self.ready = false;
        self.last_sequence = None;
        self.snapshot = None;
        self.pending.clear();
    }
}

fn send_depth_message(
    runtime: &MarketWsRuntime,
    shard_tx: &Sender<ShardEvent>,
    message: RawDepthMessage,
    received_at: Instant,
) -> Result<(), String> {
    let payload = encode_depth_message(message)?;
    let serialized_at = Instant::now();
    shard_tx
        .try_send(ShardEvent::MarketWsRaw {
            connection_id: runtime.connection_id.clone(),
            exchange: runtime.exchange,
            received_at,
            serialized_at,
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
