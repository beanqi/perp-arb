use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    thread,
};

use crossbeam_channel::{Receiver, Sender, bounded};
use serde::Serialize;
use tracing::{info, warn};

use crate::{
    config::{
        ids::{AccountId, ConnectionId, ShardId, StrategyId},
        model::{ActiveOrderView, BalanceView, MarketKey, PositionView, RuntimeCatalog, StrategyRecord},
        planner::{RuntimePlan, ShardPlan, build_runtime_plan},
    },
    error::{AppError, AppResult},
    gateway::market_ws::{MarketWsHandle, MarketWsRuntime},
    market_rules::MarketRuleStore,
};

use super::{
    book::{BookApplyResult, LocalBookState, decode_raw_depth},
    strategy::{StrategyRuntimeState, strategy_markets},
    types::{ShardEvent, TradeCommand},
};

const EVENT_QUEUE_BOUND: usize = 4096;
const COMMAND_QUEUE_BOUND: usize = 1024;

#[derive(Debug)]
struct ShardMailbox {
    event_tx: Sender<ShardEvent>,
    #[allow(dead_code)]
    command_tx: Sender<TradeCommand>,
    worker: Option<thread::JoinHandle<()>>,
}

impl Drop for ShardMailbox {
    fn drop(&mut self) {
        let _ = self.event_tx.try_send(ShardEvent::Shutdown {
            shard_id: ShardId::new("reload"),
        });
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ShardMailboxView {
    pub shard_id: ShardId,
    pub strategy_ids: Vec<StrategyId>,
    pub connection_ids: Vec<ConnectionId>,
    pub event_queue_bound: usize,
    pub command_queue_bound: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct RuntimeStatusView {
    pub generated_at_ms: u64,
    pub store_revision: u64,
    pub loaded_strategy_ids: Vec<StrategyId>,
    pub loaded_account_ids: Vec<AccountId>,
    pub shard_mailboxes: Vec<ShardMailboxView>,
    pub plan: RuntimePlan,
    pub rendered_plan: String,
    pub positions: Vec<PositionView>,
    pub balances: Vec<BalanceView>,
    pub active_orders: Vec<ActiveOrderView>,
}

impl RuntimeStatusView {
    fn empty() -> Self {
        let plan = RuntimePlan::empty();
        Self {
            generated_at_ms: plan.generated_at_ms,
            store_revision: 0,
            loaded_strategy_ids: Vec::new(),
            loaded_account_ids: Vec::new(),
            shard_mailboxes: Vec::new(),
            rendered_plan: plan.render_text(),
            plan,
            positions: Vec::new(),
            balances: Vec::new(),
            active_orders: Vec::new(),
        }
    }
}

#[derive(Debug)]
struct RuntimeRegistry {
    status: RuntimeStatusView,
    shard_mailboxes: Vec<ShardMailbox>,
    market_ws_handles: Vec<MarketWsHandle>,
    route_by_connection: HashMap<ConnectionId, Sender<ShardEvent>>,
}

#[derive(Debug)]
pub struct RuntimeManager {
    inner: RwLock<RuntimeRegistry>,
    market_rules: Arc<MarketRuleStore>,
}

impl RuntimeManager {
    pub fn new(market_rules: Arc<MarketRuleStore>) -> Self {
        Self {
            inner: RwLock::new(RuntimeRegistry {
                status: RuntimeStatusView::empty(),
                shard_mailboxes: Vec::new(),
                market_ws_handles: Vec::new(),
                route_by_connection: HashMap::new(),
            }),
            market_rules,
        }
    }

    pub fn reload(&self, catalog: RuntimeCatalog) -> AppResult<RuntimeStatusView> {
        let plan = build_runtime_plan(&catalog.enabled_strategies);
        let mut route_by_connection = HashMap::new();
        let shard_mailboxes = plan
            .shards
            .iter()
            .map(|shard| {
                Self::bootstrap_mailbox(
                    shard,
                    &plan,
                    &catalog.enabled_strategies,
                    &mut route_by_connection,
                    self.market_rules.clone(),
                )
            })
            .collect::<Vec<_>>();
        let market_ws_handles = if tokio::runtime::Handle::try_current().is_ok() {
            plan.connections
                .iter()
                .filter_map(|connection| {
                    route_by_connection.get(&connection.connection_id).map(|sender| {
                        MarketWsHandle::spawn(MarketWsRuntime::from(connection), sender.clone())
                    })
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let status = RuntimeStatusView {
            generated_at_ms: plan.generated_at_ms,
            store_revision: catalog.store_revision,
            loaded_strategy_ids: catalog
                .enabled_strategies
                .iter()
                .map(|strategy| strategy.id.clone())
                .collect(),
            loaded_account_ids: catalog
                .referenced_accounts
                .iter()
                .map(|account| account.id.clone())
                .collect(),
            shard_mailboxes: plan
                .shards
                .iter()
                .map(|shard| ShardMailboxView {
                    shard_id: shard.shard_id.clone(),
                    strategy_ids: shard.strategy_ids.clone(),
                    connection_ids: shard.connection_ids.clone(),
                    event_queue_bound: EVENT_QUEUE_BOUND,
                    command_queue_bound: COMMAND_QUEUE_BOUND,
                })
                .collect(),
            rendered_plan: plan.render_text(),
            plan,
            positions: Vec::new(),
            balances: Vec::new(),
            active_orders: Vec::new(),
        };

        let mut registry = self
            .inner
            .write()
            .map_err(|_| AppError::lock("runtime registry"))?;
        *registry = RuntimeRegistry {
            status: status.clone(),
            shard_mailboxes,
            market_ws_handles,
            route_by_connection,
        };
        Ok(status)
    }

    pub fn status(&self) -> AppResult<RuntimeStatusView> {
        let registry = self
            .inner
            .read()
            .map_err(|_| AppError::lock("runtime registry"))?;
        let _ = registry.shard_mailboxes.len();
        let _ = registry.market_ws_handles.len();
        Ok(registry.status.clone())
    }

    pub fn route_market_event(&self, event: ShardEvent) -> AppResult<()> {
        let connection_id = match &event {
            ShardEvent::MarketWsRaw { connection_id, .. } => connection_id,
            _ => return Err(AppError::Internal("only market events can be routed".to_owned())),
        };
        let registry = self
            .inner
            .read()
            .map_err(|_| AppError::lock("runtime registry"))?;
        let sender = registry.route_by_connection.get(connection_id).ok_or_else(|| {
            AppError::NotFound(format!("no shard route for connection `{connection_id}`"))
        })?;
        sender
            .try_send(event)
            .map_err(|error| AppError::Internal(format!("failed to route market event: {error}")))
    }

    pub fn positions(&self) -> AppResult<Vec<PositionView>> {
        Ok(self.status()?.positions)
    }

    pub fn balances(&self) -> AppResult<Vec<BalanceView>> {
        Ok(self.status()?.balances)
    }

    pub fn active_orders(&self) -> AppResult<Vec<ActiveOrderView>> {
        Ok(self.status()?.active_orders)
    }

    fn bootstrap_mailbox(
        shard: &ShardPlan,
        plan: &RuntimePlan,
        enabled_strategies: &[StrategyRecord],
        route_by_connection: &mut HashMap<ConnectionId, Sender<ShardEvent>>,
        market_rules: Arc<MarketRuleStore>,
    ) -> ShardMailbox {
        let (event_tx, event_rx) = bounded(EVENT_QUEUE_BOUND);
        let (command_tx, command_rx) = bounded(COMMAND_QUEUE_BOUND);
        for connection_id in &shard.connection_ids {
            route_by_connection.insert(connection_id.clone(), event_tx.clone());
        }

        let runner = ShardRunner::from_plan(
            shard,
            plan,
            enabled_strategies,
            event_rx,
            command_tx.clone(),
            command_rx,
            market_rules,
        );
        let worker = thread::Builder::new()
            .name(shard.shard_id.to_string())
            .spawn(move || runner.run())
            .ok();

        ShardMailbox {
            event_tx,
            command_tx,
            worker,
        }
    }
}

struct ShardRunner {
    shard_id: ShardId,
    event_rx: Receiver<ShardEvent>,
    command_tx: Sender<TradeCommand>,
    command_rx: Receiver<TradeCommand>,
    books: HashMap<MarketKey, LocalBookState>,
    strategies: HashMap<StrategyId, StrategyRecord>,
    strategies_by_market: HashMap<MarketKey, Vec<StrategyId>>,
    strategy_states: HashMap<StrategyId, StrategyRuntimeState>,
    market_rules: Arc<MarketRuleStore>,
}

impl ShardRunner {
    fn from_plan(
        shard: &ShardPlan,
        plan: &RuntimePlan,
        enabled_strategies: &[StrategyRecord],
        event_rx: Receiver<ShardEvent>,
        command_tx: Sender<TradeCommand>,
        command_rx: Receiver<TradeCommand>,
        market_rules: Arc<MarketRuleStore>,
    ) -> Self {
        let mut depth_mode_by_market = HashMap::new();
        for connection in &plan.connections {
            if connection.shard_id != shard.shard_id {
                continue;
            }
            for symbol in &connection.symbols {
                depth_mode_by_market.insert(
                    MarketKey::new(connection.exchange, symbol.clone()),
                    connection.depth_mode,
                );
            }
        }
        let books = shard
            .subscribed_markets
            .iter()
            .map(|market| {
                let mode = depth_mode_by_market
                    .get(market)
                    .copied()
                    .unwrap_or_else(|| market.exchange.capabilities().depth_mode);
                (market.clone(), LocalBookState::new(market.clone(), mode))
            })
            .collect();
        let shard_strategy_ids = shard
            .strategy_ids
            .iter()
            .cloned()
            .collect::<std::collections::HashSet<_>>();
        let strategies = enabled_strategies
            .iter()
            .filter(|strategy| shard_strategy_ids.contains(&strategy.id))
            .map(|strategy| (strategy.id.clone(), strategy.clone()))
            .collect::<HashMap<_, _>>();
        let strategy_states = strategies
            .keys()
            .cloned()
            .map(|strategy_id| {
                (
                    strategy_id.clone(),
                    StrategyRuntimeState::new(strategy_id),
                )
            })
            .collect::<HashMap<_, _>>();
        let mut strategies_by_market = HashMap::<MarketKey, Vec<StrategyId>>::new();
        for strategy in strategies.values() {
            for market in strategy_markets(strategy) {
                strategies_by_market
                    .entry(market)
                    .or_default()
                    .push(strategy.id.clone());
            }
        }

        Self {
            shard_id: shard.shard_id.clone(),
            event_rx,
            command_tx,
            command_rx,
            books,
            strategies,
            strategies_by_market,
            strategy_states,
            market_rules,
        }
    }

    fn run(mut self) {
        loop {
            crossbeam_channel::select! {
                recv(self.event_rx) -> event => match event {
                    Ok(ShardEvent::Shutdown { .. }) | Err(_) => break,
                    Ok(event) => self.handle_event(event),
                },
                recv(self.command_rx) -> command => match command {
                    Ok(command) => self.handle_command(command),
                    Err(_) => break,
                },
            }
        }
    }

    fn handle_event(&mut self, event: ShardEvent) {
        match event {
            ShardEvent::MarketWsRaw {
                exchange, payload, ..
            } => {
                if let Some(message) = decode_raw_depth(exchange, &payload) {
                    let market = message.market().clone();
                    if let Some(book) = self.books.get_mut(&market) {
                        let result = book.apply(message);
                        // let (bid_depth, ask_depth) = book.level_counts();
                        // info!(
                        //     "{} market {} depth applied result={:?} best_bid={:?} best_ask={:?} bid_depth={} ask_depth={}",
                        //     self.shard_id,
                        //     market,
                        //     result,
                        //     book.best_bid(),
                        //     book.best_ask(),
                        //     bid_depth,
                        //     ask_depth
                        // );
                        if result == BookApplyResult::GapDetected {
                            warn!(
                                "{} market {} depth gap detected; waiting for gateway rebuild",
                                self.shard_id, market
                            );
                        } else if result == BookApplyResult::Applied {
                            self.evaluate_market(&market);
                        }
                    }
                }
            }
            ShardEvent::TimerTick { .. }
            | ShardEvent::ConfigReload { .. }
            | ShardEvent::OrderWsRaw { .. }
            | ShardEvent::AccountWsRaw { .. }
            | ShardEvent::PositionWsRaw { .. }
            | ShardEvent::Shutdown { .. } => {}
        }
    }

    fn handle_command(&mut self, command: TradeCommand) {
        if let TradeCommand::PlaceOrder {
            exchange,
            symbol,
            qty,
            limit_price,
            ..
        } = &command
        {
            let market = MarketKey::new(*exchange, symbol.clone());
            match self.market_rules.get(&market) {
                Some(rule) => info!(
                    "{} order market={} qty={} limit_price={:?} rule price_tick={:?} qty_step={:?} min_qty={:?} min_notional={:?} contract_multiplier={:?}",
                    self.shard_id,
                    market,
                    qty,
                    limit_price,
                    rule.price_tick,
                    rule.qty_step,
                    rule.min_qty,
                    rule.min_notional,
                    rule.contract_multiplier
                ),
                None => warn!(
                    "{} order market={} has no cached market rule yet",
                    self.shard_id, market
                ),
            }
        }
    }

    fn evaluate_market(&mut self, market: &MarketKey) {
        let Some(strategy_ids) = self.strategies_by_market.get(market).cloned() else {
            return;
        };
        let rules = self.market_rules.snapshot();

        for strategy_id in strategy_ids {
            let Some(strategy) = self.strategies.get(&strategy_id) else {
                continue;
            };
            let [long_market, short_market] = strategy_markets(strategy);
            let (Some(long_book), Some(short_book)) =
                (self.books.get(&long_market), self.books.get(&short_market))
            else {
                continue;
            };
            if !long_book.has_snapshot || !short_book.has_snapshot {
                continue;
            }
            let (Some(long_rule), Some(short_rule)) =
                (rules.get(&long_market), rules.get(&short_market))
            else {
                continue;
            };
            let Some(state) = self.strategy_states.get_mut(&strategy_id) else {
                continue;
            };

            for command in state.evaluate(
                &self.shard_id,
                strategy,
                long_book,
                short_book,
                long_rule,
                short_rule,
            ) {
                if let Err(error) = self.command_tx.try_send(command) {
                    warn!(
                        "{} strategy {} failed to enqueue trade command: {}",
                        self.shard_id, strategy_id, error
                    );
                }
            }
        }
    }
}
