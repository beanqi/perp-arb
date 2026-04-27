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
        model::{ActiveOrderView, BalanceView, MarketKey, PositionView, RuntimeCatalog, StrategyRecord, now_ms},
        planner::{RuntimePlan, ShardPlan, build_runtime_plan},
    },
    error::{AppError, AppResult},
    gateway::{
        market_ws::{MarketWsHandle, MarketWsRuntime},
        trade_exec::{PlaceOrderReject, PreparedPlaceOrder, prepare_place_order},
    },
    market_rules::MarketRuleStore,
};

use super::{
    book::{BookApplyResult, LocalBookState, decode_raw_depth},
    strategy::{StrategyAction, StrategyOrderPlan, StrategyRuntimeState, strategy_markets},
    telemetry::{
        StrategyMatchEvent, StrategyMatchEventKind, StrategyRuntimeDescriptor, StrategyRuntimeSnapshot,
        StrategyTelemetryEvent, StrategyTelemetryHub,
    },
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
    telemetry: StrategyTelemetryHub,
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
            telemetry: StrategyTelemetryHub::spawn(),
        }
    }

    pub fn reload(&self, catalog: RuntimeCatalog) -> AppResult<RuntimeStatusView> {
        let plan = build_runtime_plan(&catalog.enabled_strategies);
        let telemetry_generation = plan.generated_at_ms;
        self.telemetry.publish_loaded(
            telemetry_generation,
            catalog
                .enabled_strategies
                .iter()
                .map(strategy_descriptor)
                .collect(),
        );
        let mut route_by_connection = HashMap::new();
        let shard_mailboxes = plan
            .shards
            .iter()
            .map(|shard| {
                Self::bootstrap_mailbox(
                    shard,
                    &plan,
                    &catalog.enabled_strategies,
                    telemetry_generation,
                    self.telemetry.sender(),
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

    pub fn strategy_runtime_snapshot(&self) -> StrategyRuntimeSnapshot {
        self.telemetry.snapshot().as_ref().clone()
    }

    fn bootstrap_mailbox(
        shard: &ShardPlan,
        plan: &RuntimePlan,
        enabled_strategies: &[StrategyRecord],
        telemetry_generation: u64,
        telemetry_tx: Sender<StrategyTelemetryEvent>,
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
            telemetry_generation,
            telemetry_tx,
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
    telemetry_generation: u64,
    telemetry_tx: Sender<StrategyTelemetryEvent>,
    market_rules: Arc<MarketRuleStore>,
}

impl ShardRunner {
    fn from_plan(
        shard: &ShardPlan,
        plan: &RuntimePlan,
        enabled_strategies: &[StrategyRecord],
        telemetry_generation: u64,
        telemetry_tx: Sender<StrategyTelemetryEvent>,
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
            telemetry_generation,
            telemetry_tx,
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
            let Some((evaluation, open_spread_pct, close_spread_pct)) =
                self.strategy_states.get_mut(&strategy_id).map(|state| {
                    let evaluation = state.evaluate(strategy, long_book, short_book);
                    (
                        evaluation,
                        state.last_open_spread_pct,
                        state.last_close_spread_pct,
                    )
                })
            else {
                continue;
            };
            let mut metrics = evaluation.metrics;
            let mut planned_order = None;
            let mut commands = Vec::new();

            if let Some(plan) = evaluation.planned_order
                && let Some(prepared_plan) = self.prepare_plan_for_order(strategy, &plan)
            {
                commands = self
                    .strategy_states
                    .get_mut(&strategy_id)
                    .map(|state| state.commit_plan(&self.shard_id, strategy, &prepared_plan))
                    .unwrap_or_default();
                if let Some(state) = self.strategy_states.get(&strategy_id) {
                    metrics = state.metrics();
                }
                planned_order = Some(prepared_plan);
            }

            let ts_ms = now_ms();
            self.send_telemetry(StrategyTelemetryEvent::MetricsUpdated {
                generation: self.telemetry_generation,
                shard_id: self.shard_id.clone(),
                strategy_id: strategy_id.clone(),
                metrics,
                ts_ms,
            });

            if let Some(plan) = planned_order {
                let kind = match plan.action {
                    StrategyAction::Open => StrategyMatchEventKind::OpenPlanned,
                    StrategyAction::Close => StrategyMatchEventKind::ClosePlanned,
                };
                self.send_telemetry(StrategyTelemetryEvent::OrderPlanned {
                    generation: self.telemetry_generation,
                    event: StrategyMatchEvent {
                        ts_ms,
                        shard_id: self.shard_id.clone(),
                        strategy_id: strategy_id.clone(),
                        kind,
                        open_spread_pct,
                        close_spread_pct,
                        notional_usd: Some(plan.notional_usd),
                        message: format!(
                            "{:?} planned notional={} long_qty={} short_qty={}",
                            plan.action, plan.notional_usd, plan.long_qty, plan.short_qty
                        ),
                    },
                });
            }

            for command in commands {
                if let Err(error) = self.command_tx.try_send(command) {
                    warn!(
                        "{} strategy {} failed to enqueue trade command: {}",
                        self.shard_id, strategy_id, error
                    );
                }
            }
        }
    }

    fn prepare_plan_for_order(
        &self,
        strategy: &StrategyRecord,
        plan: &StrategyOrderPlan,
    ) -> Option<StrategyOrderPlan> {
        // market rule 只在下单边界应用，撮合阶段保持纯深度计算。
        let [long_market, short_market] = strategy_markets(strategy);
        let rules = self.market_rules.snapshot();
        let Some(long_rule) = rules.get(&long_market) else {
            warn!(
                "{} strategy {} skip order: missing market rule for {}",
                self.shard_id, strategy.id, long_market
            );
            return None;
        };
        let Some(short_rule) = rules.get(&short_market) else {
            warn!(
                "{} strategy {} skip order: missing market rule for {}",
                self.shard_id, strategy.id, short_market
            );
            return None;
        };

        let long_order = match prepare_place_order(
            plan.long_qty,
            Some(plan.long_limit_price),
            &plan.action.long_side(),
            long_rule,
        ) {
            Ok(order) => order,
            Err(reason) => {
                self.warn_order_reject(strategy, &long_market, reason);
                return None;
            }
        };
        let short_order = match prepare_place_order(
            plan.short_qty,
            Some(plan.short_limit_price),
            &plan.action.short_side(),
            short_rule,
        ) {
            Ok(order) => order,
            Err(reason) => {
                self.warn_order_reject(strategy, &short_market, reason);
                return None;
            }
        };

        let notional_usd = order_notional(long_order).min(order_notional(short_order));
        (notional_usd > 0.0).then(|| StrategyOrderPlan {
            action: plan.action,
            spread_pct: plan.spread_pct,
            notional_usd,
            long_qty: long_order.qty,
            short_qty: short_order.qty,
            long_limit_price: long_order.limit_price.unwrap_or(plan.long_limit_price),
            short_limit_price: short_order.limit_price.unwrap_or(plan.short_limit_price),
        })
    }

    fn warn_order_reject(
        &self,
        strategy: &StrategyRecord,
        market: &MarketKey,
        reason: PlaceOrderReject,
    ) {
        warn!(
            "{} strategy {} skip order market={} rule reject: {}",
            self.shard_id,
            strategy.id,
            market,
            reason
        );
    }

    fn send_telemetry(&self, event: StrategyTelemetryEvent) {
        if let Err(error) = self.telemetry_tx.try_send(event) {
            warn!("{} strategy telemetry event dropped: {}", self.shard_id, error);
        }
    }
}

fn order_notional(order: PreparedPlaceOrder) -> f64 {
    order.notional_usd.unwrap_or(0.0)
}

fn strategy_descriptor(strategy: &StrategyRecord) -> StrategyRuntimeDescriptor {
    let [long_market, short_market] = strategy_markets(strategy);
    StrategyRuntimeDescriptor {
        strategy_id: strategy.id.clone(),
        name: strategy.name.clone(),
        enabled: strategy.enabled,
        long_market,
        short_market,
    }
}
