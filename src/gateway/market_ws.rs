mod binance;
mod binance_sync;
mod bybit;

use crossbeam_channel::Sender;
use tokio::task::JoinHandle;

use crate::{
    config::{
        ids::{ConnectionId, ShardId},
        model::{DepthMode, Exchange},
        planner::ConnectionPlan,
    },
    engine::types::ShardEvent,
};

#[derive(Clone, Debug)]
pub struct MarketWsRuntime {
    pub connection_id: ConnectionId,
    pub shard_id: ShardId,
    pub exchange: Exchange,
    pub depth_mode: DepthMode,
    pub subscribed_symbols: Vec<String>,
}

impl From<&ConnectionPlan> for MarketWsRuntime {
    fn from(plan: &ConnectionPlan) -> Self {
        Self {
            connection_id: plan.connection_id.clone(),
            shard_id: plan.shard_id.clone(),
            exchange: plan.exchange,
            depth_mode: plan.depth_mode,
            subscribed_symbols: plan.symbols.clone(),
        }
    }
}

#[derive(Debug)]
pub struct MarketWsHandle {
    task: JoinHandle<()>,
}

impl MarketWsHandle {
    pub fn spawn(runtime: MarketWsRuntime, shard_tx: Sender<ShardEvent>) -> Self {
        let task = tokio::spawn(async move {
            match runtime.exchange {
                Exchange::BinanceUsdM => binance::run(runtime, shard_tx).await,
                Exchange::BybitLinear => bybit::run(runtime, shard_tx).await,
            }
        });
        Self { task }
    }
}

impl Drop for MarketWsHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}
