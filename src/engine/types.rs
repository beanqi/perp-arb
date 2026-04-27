use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::config::{
    ids::{AccountId, ClientOrderId, ConnectionId, ShardId, StrategyId},
    model::Exchange,
};

use super::book::{DepthDecodeTiming, RawDepthMessage};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShardEvent {
    MarketWsRaw {
        connection_id: ConnectionId,
        message: RawDepthMessage,
        #[serde(skip, default = "now_instant")]
        received_at: Instant,
        #[serde(skip, default)]
        timing: DepthEventTiming,
    },
    OrderWsRaw {
        shard_id: ShardId,
        account_id: AccountId,
        exchange: Exchange,
        payload: Vec<u8>,
    },
    AccountWsRaw {
        shard_id: ShardId,
        account_id: AccountId,
        exchange: Exchange,
        payload: Vec<u8>,
    },
    PositionWsRaw {
        shard_id: ShardId,
        account_id: AccountId,
        exchange: Exchange,
        payload: Vec<u8>,
    },
    TimerTick {
        shard_id: ShardId,
        now_ms: u64,
    },
    ConfigReload {
        shard_id: ShardId,
        strategy_ids: Vec<StrategyId>,
    },
    Shutdown {
        shard_id: ShardId,
    },
}

fn now_instant() -> Instant {
    Instant::now()
}

#[derive(Clone, Copy, Debug)]
pub struct DepthEventTiming {
    pub payload_bytes: usize,
    pub decode: DepthDecodeTiming,
    // 网关侧计时随深度事件进入 shard，最终和 book 合并耗时在同一条 perf 日志里对齐。
    pub gateway_sync_us: u128,
    pub gateway_started_at: Instant,
    pub enqueued_at: Instant,
}

impl DepthEventTiming {
    pub fn new(payload_bytes: usize, decode: DepthDecodeTiming) -> Self {
        Self::from_gateway_started_at(payload_bytes, decode, Instant::now())
    }

    pub fn from_gateway_started_at(
        payload_bytes: usize,
        decode: DepthDecodeTiming,
        gateway_started_at: Instant,
    ) -> Self {
        Self {
            payload_bytes,
            decode,
            gateway_sync_us: 0,
            gateway_started_at,
            enqueued_at: Instant::now(),
        }
    }

    pub fn mark_enqueued(&mut self) {
        self.gateway_sync_us = self.gateway_started_at.elapsed().as_micros();
        self.enqueued_at = Instant::now();
    }

    pub fn queue_wait_us(&self) -> u128 {
        self.enqueued_at.elapsed().as_micros()
    }
}

impl Default for DepthEventTiming {
    fn default() -> Self {
        Self::new(0, DepthDecodeTiming::default())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradeCommand {
    PlaceOrder {
        shard_id: ShardId,
        strategy_id: StrategyId,
        account_id: AccountId,
        exchange: Exchange,
        symbol: String,
        side: Side,
        qty: f64,
        limit_price: Option<f64>,
        client_order_id: ClientOrderId,
    },
    CancelOrder {
        shard_id: ShardId,
        strategy_id: StrategyId,
        account_id: AccountId,
        exchange: Exchange,
        symbol: String,
        client_order_id: ClientOrderId,
    },
    QueryOrder {
        shard_id: ShardId,
        strategy_id: StrategyId,
        account_id: AccountId,
        exchange: Exchange,
        symbol: String,
        client_order_id: ClientOrderId,
    },
}
