use std::{collections::HashMap, time::Instant};

use crossbeam_channel::Sender;
use reqwest::Client;
use serde::Deserialize;
use tracing::{info, warn};

use crate::{
    config::model::{Exchange, MarketKey},
    engine::{
        book::{DepthDecodeTiming, PriceLevel, RawDepthMessage, decode_raw_depth_with_timing},
        types::{DepthEventTiming, ShardEvent},
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
        received_at: Instant,
        shard_tx: &Sender<ShardEvent>,
    ) -> Result<(), String> {
        let Some(decoded) = decode_raw_depth_with_timing(self.runtime.exchange, payload) else {
            return Ok(());
        };
        let timing = DepthEventTiming::new(payload.len(), decoded.timing);
        let message = decoded.message;
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
        state.buffer(TimedDepthMessage {
            message,
            received_at,
            timing,
        });

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
            send_depth_message(
                &self.runtime,
                shard_tx,
                message.message,
                message.received_at,
                message.timing,
            )?;
        }
        Ok(())
    }

    async fn initialize_symbol(
        &mut self,
        symbol: &str,
        shard_tx: &Sender<ShardEvent>,
    ) -> Result<(), String> {
        let (last_update_id, first_delta_index) = loop {
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
            match state.snapshot_bridge(last_update_id) {
                SnapshotBridge::Ready { first_delta_index } => {
                    break (last_update_id, first_delta_index);
                }
                SnapshotBridge::Waiting => {
                    warn!(
                        "market ws {} binance symbol={} waiting snapshot bridge last_update_id={} pending_deltas={}",
                        self.runtime.connection_id,
                        symbol,
                        last_update_id,
                        state.pending.len()
                    );
                    return Ok(());
                }
                SnapshotBridge::SnapshotBehind {
                    first_sequence,
                    target_sequence,
                } => {
                    warn!(
                        "market ws {} binance symbol={} snapshot behind buffered deltas; refetching snapshot last_update_id={} target_sequence={} first_pending_sequence={} pending_deltas={}",
                        self.runtime.connection_id,
                        symbol,
                        last_update_id,
                        target_sequence,
                        first_sequence,
                        state.pending.len()
                    );
                    state.snapshot = None;
                }
            }
        };

        let state = self
            .states
            .get_mut(symbol)
            .ok_or_else(|| format!("missing sync state for {symbol}"))?;
        let snapshot_received_at = state
            .pending
            .get(first_delta_index)
            .map(|message| message.received_at)
            .unwrap_or_else(Instant::now);
        let snapshot = state
            .snapshot
            .take()
            .ok_or_else(|| format!("missing snapshot for {symbol}"))?;
        let normalize_started_at = Instant::now();
        let bids = snapshot
            .bids
            .into_iter()
            .filter_map(parse_snapshot_level)
            .collect();
        let asks = snapshot
            .asks
            .into_iter()
            .filter_map(parse_snapshot_level)
            .collect();
        let timing = DepthEventTiming::from_gateway_started_at(
            0,
            DepthDecodeTiming {
                deserialize_us: 0,
                normalize_us: normalize_started_at.elapsed().as_micros(),
            },
            snapshot_received_at,
        );

        send_depth_message(
            &self.runtime,
            shard_tx,
            RawDepthMessage::Snapshot {
                market: MarketKey::new(Exchange::BinanceUsdM, symbol.to_owned()),
                sequence: Some(last_update_id),
                bids,
                asks,
            },
            snapshot_received_at,
            timing,
        )?;

        for delta in state.mark_ready_and_drain_from(first_delta_index) {
            send_depth_message(
                &self.runtime,
                shard_tx,
                delta.message,
                delta.received_at,
                delta.timing,
            )?;
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

struct SymbolSyncState {
    ready: bool,
    last_sequence: Option<u64>,
    snapshot: Option<BinanceSnapshot>,
    pending: Vec<TimedDepthMessage>,
}

struct TimedDepthMessage {
    message: RawDepthMessage,
    received_at: Instant,
    timing: DepthEventTiming,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SnapshotBridge {
    Ready {
        first_delta_index: usize,
    },
    Waiting,
    SnapshotBehind {
        first_sequence: u64,
        target_sequence: u64,
    },
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

    fn buffer(&mut self, message: TimedDepthMessage) {
        self.pending.push(message);
    }

    fn discard_stale(&mut self, last_update_id: u64) {
        self.pending.retain(|timed| match &timed.message {
            RawDepthMessage::Delta { sequence, .. } => {
                sequence.is_none_or(|value| value > last_update_id)
            }
            RawDepthMessage::Snapshot { .. } => false,
        });
    }

    fn snapshot_bridge(&mut self, last_update_id: u64) -> SnapshotBridge {
        self.discard_stale(last_update_id);
        let target_sequence = last_update_id.saturating_add(1);
        for (index, timed) in self.pending.iter().enumerate() {
            let RawDepthMessage::Delta {
                first_sequence,
                sequence,
                ..
            } = &timed.message
            else {
                continue;
            };
            let Some(first_sequence) = *first_sequence else {
                continue;
            };
            // Binance 初始化要求第一条可用增量覆盖 last_update_id + 1；如果
            // pending 已经越过目标，当前 snapshot 永远无法桥接，只能重拉。
            if first_sequence > target_sequence {
                return SnapshotBridge::SnapshotBehind {
                    first_sequence,
                    target_sequence,
                };
            }
            if sequence.is_some_and(|value| value >= target_sequence) {
                return SnapshotBridge::Ready {
                    first_delta_index: index,
                };
            }
        }
        SnapshotBridge::Waiting
    }

    fn mark_ready_and_drain_from(&mut self, index: usize) -> Vec<TimedDepthMessage> {
        self.ready = true;
        self.pending.drain(..index).for_each(drop);
        self.record_pending_sequences();
        self.pending.drain(..).collect()
    }

    fn pop_ready_delta(&mut self) -> Option<TimedDepthMessage> {
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
        for timed in &self.pending {
            let RawDepthMessage::Delta {
                previous_sequence,
                sequence,
                ..
            } = &timed.message
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
        for timed in &self.pending {
            if let RawDepthMessage::Delta { sequence, .. } = &timed.message {
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
    mut timing: DepthEventTiming,
) -> Result<(), String> {
    timing.mark_enqueued();
    shard_tx
        .try_send(ShardEvent::MarketWsRaw {
            connection_id: runtime.connection_id.clone(),
            message,
            received_at,
            timing,
        })
        .map_err(|error| format!("shard queue send failed: {error}"))
}

async fn fetch_snapshot(client: &Client, symbol: &str) -> Result<BinanceSnapshot, String> {
    let body = client
        .get(format!("{FAPI_BASE}/fapi/v1/depth"))
        .query(&[("symbol", symbol), ("limit", &DEPTH_LIMIT.to_string())])
        .send()
        .await
        .map_err(|error| format!("snapshot request failed for {symbol}: {error}"))?
        .error_for_status()
        .map_err(|error| format!("snapshot http error for {symbol}: {error}"))?
        .text()
        .await
        .map_err(|error| format!("snapshot body read failed for {symbol}: {error}"))?;
    sonic_rs::from_str::<BinanceSnapshot>(&body)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_bridge_reports_snapshot_behind_when_pending_starts_after_target() {
        let mut state = SymbolSyncState::new();
        state.buffer(delta(110, 120));

        assert_eq!(
            state.snapshot_bridge(100),
            SnapshotBridge::SnapshotBehind {
                first_sequence: 110,
                target_sequence: 101,
            }
        );
    }

    #[test]
    fn snapshot_bridge_waits_without_pending_delta() {
        let mut state = SymbolSyncState::new();

        assert_eq!(state.snapshot_bridge(100), SnapshotBridge::Waiting);
    }

    #[test]
    fn snapshot_bridge_discards_stale_and_returns_bridge() {
        let mut state = SymbolSyncState::new();
        state.buffer(delta(90, 100));
        state.buffer(delta(100, 101));

        assert_eq!(
            state.snapshot_bridge(100),
            SnapshotBridge::Ready {
                first_delta_index: 0
            }
        );
        assert_eq!(state.pending.len(), 1);
    }

    fn delta(first_sequence: u64, sequence: u64) -> TimedDepthMessage {
        TimedDepthMessage {
            message: RawDepthMessage::Delta {
                market: MarketKey::new(Exchange::BinanceUsdM, "BTCUSDT"),
                first_sequence: Some(first_sequence),
                previous_sequence: None,
                sequence: Some(sequence),
                bids: Vec::new(),
                asks: Vec::new(),
            },
            received_at: Instant::now(),
            timing: DepthEventTiming::default(),
        }
    }
}
