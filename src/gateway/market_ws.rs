use crate::config::{
    ids::{ConnectionId, ShardId},
    model::Exchange,
    planner::ConnectionPlan,
};

#[derive(Clone, Debug)]
pub struct MarketWsRuntime {
    pub connection_id: ConnectionId,
    pub shard_id: ShardId,
    pub exchange: Exchange,
    pub subscribed_symbols: Vec<String>,
}

impl From<&ConnectionPlan> for MarketWsRuntime {
    fn from(plan: &ConnectionPlan) -> Self {
        Self {
            connection_id: plan.connection_id.clone(),
            shard_id: plan.shard_id.clone(),
            exchange: plan.exchange,
            subscribed_symbols: plan.symbols.clone(),
        }
    }
}
