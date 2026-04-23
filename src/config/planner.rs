use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::Serialize;

use crate::config::{
    ids::{ConnectionId, ShardId, StrategyId},
    model::{DepthMode, MarketKey, StrategyPlacementView, StrategyRecord, now_ms},
};

const MAX_MARKETS_PER_SHARD: usize = 12;

#[derive(Clone, Debug, Serialize)]
pub struct RuntimePlan {
    pub generated_at_ms: u64,
    pub total_enabled_strategies: usize,
    pub total_unique_markets: usize,
    pub shards: Vec<ShardPlan>,
    pub connections: Vec<ConnectionPlan>,
    pub strategy_placements: Vec<StrategyPlacementView>,
}

impl RuntimePlan {
    pub fn empty() -> Self {
        Self {
            generated_at_ms: now_ms(),
            total_enabled_strategies: 0,
            total_unique_markets: 0,
            shards: Vec::new(),
            connections: Vec::new(),
            strategy_placements: Vec::new(),
        }
    }

    pub fn render_text(&self) -> String {
        let mut lines = Vec::new();
        lines.push(format!(
            "runtime plan: {} enabled strategies, {} shards, {} connections, {} unique markets",
            self.total_enabled_strategies,
            self.shards.len(),
            self.connections.len(),
            self.total_unique_markets
        ));

        if self.shards.is_empty() {
            lines.push("  no enabled strategy loaded".to_owned());
            return lines.join("\n");
        }

        for shard in &self.shards {
            let strategies = shard
                .strategy_ids
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            let markets = shard
                .subscribed_markets
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            let connections = shard
                .connection_ids
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ");

            lines.push(format!(
                "  {} strategies=[{}] connections=[{}] markets=[{}]",
                shard.shard_id, strategies, connections, markets
            ));
        }

        for connection in &self.connections {
            lines.push(format!(
                "    {} -> {} exchange={} depth_mode={} symbols=[{}]",
                connection.connection_id,
                connection.shard_id,
                connection.exchange,
                connection.depth_mode,
                connection.symbols.join(", ")
            ));
        }

        lines.join("\n")
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ShardPlan {
    pub shard_id: ShardId,
    pub strategy_ids: Vec<StrategyId>,
    pub connection_ids: Vec<ConnectionId>,
    pub subscribed_markets: Vec<MarketKey>,
    pub component_count: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct ConnectionPlan {
    pub connection_id: ConnectionId,
    pub shard_id: ShardId,
    pub exchange: crate::config::model::Exchange,
    pub depth_mode: DepthMode,
    pub symbols: Vec<String>,
    pub strategy_ids: Vec<StrategyId>,
}

#[derive(Clone, Debug)]
struct StrategyComponent {
    strategy_indices: Vec<usize>,
    strategy_ids: Vec<StrategyId>,
    markets: BTreeSet<MarketKey>,
}

pub fn build_runtime_plan(enabled_strategies: &[StrategyRecord]) -> RuntimePlan {
    if enabled_strategies.is_empty() {
        return RuntimePlan::empty();
    }

    let total_unique_markets = enabled_strategies
        .iter()
        .flat_map(StrategyRecord::market_keys)
        .collect::<BTreeSet<_>>()
        .len();
    let components = build_components(enabled_strategies);

    let mut shard_components = Vec::<Vec<StrategyComponent>>::new();
    let mut current_group = Vec::<StrategyComponent>::new();
    let mut current_markets = BTreeSet::<MarketKey>::new();

    for component in components {
        let merged_markets = current_markets
            .iter()
            .cloned()
            .chain(component.markets.iter().cloned())
            .collect::<BTreeSet<_>>();

        if !current_group.is_empty() && merged_markets.len() > MAX_MARKETS_PER_SHARD {
            shard_components.push(current_group);
            current_group = Vec::new();
            current_markets = BTreeSet::new();
        }

        current_markets.extend(component.markets.iter().cloned());
        current_group.push(component);
    }

    if !current_group.is_empty() {
        shard_components.push(current_group);
    }

    let mut shards = Vec::new();
    let mut connections = Vec::new();
    let mut placements = Vec::new();
    let mut next_connection_index = 1_usize;

    for (shard_index, components_in_shard) in shard_components.into_iter().enumerate() {
        let shard_id = ShardId::new(format!("shard-{:03}", shard_index + 1));
        let mut shard_strategy_indices = components_in_shard
            .iter()
            .flat_map(|component| component.strategy_indices.iter().copied())
            .collect::<Vec<_>>();
        shard_strategy_indices.sort_unstable();
        shard_strategy_indices.dedup();

        let strategy_ids = shard_strategy_indices
            .iter()
            .map(|index| enabled_strategies[*index].id.clone())
            .collect::<Vec<_>>();
        let subscribed_markets = shard_strategy_indices
            .iter()
            .flat_map(|index| enabled_strategies[*index].market_keys())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();

        let mut shard_connection_ids = Vec::new();
        let mut connections_in_shard = Vec::new();
        let mut markets_by_exchange = BTreeMap::<crate::config::model::Exchange, Vec<MarketKey>>::new();
        for market in &subscribed_markets {
            markets_by_exchange
                .entry(market.exchange)
                .or_default()
                .push(market.clone());
        }

        for (exchange, markets) in markets_by_exchange {
            let chunk_size = exchange.capabilities().max_symbols_per_connection;
            for chunk in markets.chunks(chunk_size) {
                let connection_id = ConnectionId::new(format!("conn-{:03}", next_connection_index));
                next_connection_index += 1;
                let chunk_markets = chunk.to_vec();
                let chunk_market_set = chunk_markets.iter().cloned().collect::<BTreeSet<_>>();
                let strategy_ids = shard_strategy_indices
                    .iter()
                    .filter_map(|index| {
                        let strategy = &enabled_strategies[*index];
                        let strategy_markets = strategy.market_keys();
                        if strategy_markets
                            .iter()
                            .any(|market| chunk_market_set.contains(market))
                        {
                            Some(strategy.id.clone())
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();

                let connection_plan = ConnectionPlan {
                    connection_id: connection_id.clone(),
                    shard_id: shard_id.clone(),
                    exchange,
                    depth_mode: exchange.capabilities().depth_mode,
                    symbols: chunk_markets.iter().map(|market| market.symbol.clone()).collect(),
                    strategy_ids,
                };
                shard_connection_ids.push(connection_id);
                connections_in_shard.push(connection_plan);
            }
        }

        for index in &shard_strategy_indices {
            let strategy = &enabled_strategies[*index];
            let strategy_markets = strategy.market_keys();
            let connection_ids = connections_in_shard
                .iter()
                .filter_map(|connection| {
                    let symbols = connection
                        .symbols
                        .iter()
                        .map(|symbol| MarketKey::new(connection.exchange, symbol.clone()))
                        .collect::<BTreeSet<_>>();
                    if strategy_markets.iter().any(|market| symbols.contains(market)) {
                        Some(connection.connection_id.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();

            placements.push(StrategyPlacementView {
                strategy_id: strategy.id.clone(),
                shard_id: shard_id.clone(),
                connection_ids,
                markets: strategy_markets,
            });
        }

        connections.extend(connections_in_shard);
        shards.push(ShardPlan {
            shard_id,
            strategy_ids,
            connection_ids: shard_connection_ids,
            subscribed_markets,
            component_count: components_in_shard.len(),
        });
    }

    RuntimePlan {
        generated_at_ms: now_ms(),
        total_enabled_strategies: enabled_strategies.len(),
        total_unique_markets,
        shards,
        connections,
        strategy_placements: placements,
    }
}

fn build_components(enabled_strategies: &[StrategyRecord]) -> Vec<StrategyComponent> {
    let mut market_to_indices = HashMap::<MarketKey, Vec<usize>>::new();
    for (index, strategy) in enabled_strategies.iter().enumerate() {
        for market in strategy.market_keys() {
            market_to_indices.entry(market).or_default().push(index);
        }
    }

    let mut adjacency = vec![Vec::<usize>::new(); enabled_strategies.len()];
    for strategy_indices in market_to_indices.values() {
        for index in strategy_indices {
            adjacency[*index].extend(strategy_indices.iter().copied().filter(|other| other != index));
        }
    }

    let mut visited = vec![false; enabled_strategies.len()];
    let mut components = Vec::new();
    for start_index in 0..enabled_strategies.len() {
        if visited[start_index] {
            continue;
        }

        let mut stack = vec![start_index];
        let mut strategy_indices = Vec::new();
        let mut strategy_ids = Vec::new();
        let mut markets = BTreeSet::new();

        while let Some(index) = stack.pop() {
            if visited[index] {
                continue;
            }
            visited[index] = true;
            strategy_indices.push(index);
            let strategy = &enabled_strategies[index];
            strategy_ids.push(strategy.id.clone());
            markets.extend(strategy.market_keys());
            for neighbor in &adjacency[index] {
                if !visited[*neighbor] {
                    stack.push(*neighbor);
                }
            }
        }

        strategy_ids.sort();
        components.push(StrategyComponent {
            strategy_indices,
            strategy_ids,
            markets,
        });
    }

    components.sort_by(|left, right| left.strategy_ids.cmp(&right.strategy_ids));
    components
}
