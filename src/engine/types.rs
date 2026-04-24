use serde::{Deserialize, Serialize};

use crate::config::{
    ids::{AccountId, ClientOrderId, ConnectionId, ShardId, StrategyId},
    model::Exchange,
};

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
        exchange: Exchange,
        payload: Vec<u8>,
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
