use crate::{
    config::{
        ids::{ClientOrderId, ShardId, StrategyId},
        model::{MarketKey, SpreadLevel, StrategyRecord},
    },
    engine::{
        book::{LocalBookState, PriceLevel},
        types::{Side, TradeCommand},
    },
    market_rules::MarketRule,
};

pub const MAX_MATCH_LEVELS: usize = 10;

#[derive(Clone, Debug, Default)]
pub struct StrategyRuntimeState {
    pub strategy_id: Option<StrategyId>,
    pub current_pair_notional: f64,
    pub pending_open_notional: f64,
    pub pending_close_notional: f64,
    pub active_order_count: u32,
    pub last_open_spread_pct: Option<f64>,
    pub last_close_spread_pct: Option<f64>,
    next_order_seq: u64,
}

#[derive(Clone, Copy, Debug)]
pub enum StrategyAction {
    Open,
    Close,
}

#[derive(Clone, Debug)]
pub struct StrategyOrderPlan {
    pub action: StrategyAction,
    pub spread_pct: f64,
    pub notional_usd: f64,
    pub long_qty: f64,
    pub short_qty: f64,
    pub long_limit_price: f64,
    pub short_limit_price: f64,
}

impl StrategyRuntimeState {
    pub fn new(strategy_id: StrategyId) -> Self {
        Self {
            strategy_id: Some(strategy_id),
            ..Self::default()
        }
    }

    pub fn evaluate(
        &mut self,
        shard_id: &ShardId,
        strategy: &StrategyRecord,
        long_book: &LocalBookState,
        short_book: &LocalBookState,
        long_rule: &MarketRule,
        short_rule: &MarketRule,
    ) -> Vec<TradeCommand> {
        if self.active_order_count + 2 > strategy.max_open_orders {
            return Vec::new();
        }

        let close_plan = self.close_plan(strategy, long_book, short_book, long_rule, short_rule);
        if let Some(plan) = close_plan {
            return self.commands_for_plan(shard_id, strategy, plan);
        }

        let open_plan = self.open_plan(strategy, long_book, short_book, long_rule, short_rule);
        if let Some(plan) = open_plan {
            return self.commands_for_plan(shard_id, strategy, plan);
        }

        Vec::new()
    }

    fn open_plan(
        &mut self,
        strategy: &StrategyRecord,
        long_book: &LocalBookState,
        short_book: &LocalBookState,
        long_rule: &MarketRule,
        short_rule: &MarketRule,
    ) -> Option<StrategyOrderPlan> {
        let ask_long = long_book.best_ask()?;
        let bid_short = short_book.best_bid()?;
        let spread_pct = spread_pct(bid_short.price, ask_long.price)?;
        self.last_open_spread_pct = Some(spread_pct);

        let target = target_notional(&strategy.open_levels, spread_pct).min(strategy.max_total_notional);
        let current_or_pending = self.current_pair_notional + self.pending_open_notional;
        let desired = positive_delta(target, current_or_pending)?;
        let room = positive_delta(strategy.max_total_notional, current_or_pending)?;
        let desired = desired.min(room);

        let matched = match_open_depth(long_book, short_book, desired)?;
        let normalized = normalize_pair_notional(
            matched.notional_usd,
            matched.long_price,
            matched.short_price,
            long_rule,
            short_rule,
        )?;

        Some(StrategyOrderPlan {
            action: StrategyAction::Open,
            spread_pct,
            notional_usd: normalized.notional_usd,
            long_qty: normalized.long_qty,
            short_qty: normalized.short_qty,
            long_limit_price: matched.long_price,
            short_limit_price: matched.short_price,
        })
    }

    fn close_plan(
        &mut self,
        strategy: &StrategyRecord,
        long_book: &LocalBookState,
        short_book: &LocalBookState,
        long_rule: &MarketRule,
        short_rule: &MarketRule,
    ) -> Option<StrategyOrderPlan> {
        let bid_long = long_book.best_bid()?;
        let ask_short = short_book.best_ask()?;
        let spread_pct = spread_pct(bid_long.price, ask_short.price)?;
        self.last_close_spread_pct = Some(spread_pct);

        let close_target = target_notional(&strategy.close_levels, spread_pct);
        let available = positive_delta(self.current_pair_notional, self.pending_close_notional)?;
        let desired = close_target.min(available);
        if desired <= 0.0 {
            return None;
        }

        let matched = match_close_depth(long_book, short_book, desired)?;
        let normalized = normalize_pair_notional(
            matched.notional_usd,
            matched.long_price,
            matched.short_price,
            long_rule,
            short_rule,
        )?;

        Some(StrategyOrderPlan {
            action: StrategyAction::Close,
            spread_pct,
            notional_usd: normalized.notional_usd,
            long_qty: normalized.long_qty,
            short_qty: normalized.short_qty,
            long_limit_price: matched.long_price,
            short_limit_price: matched.short_price,
        })
    }

    fn commands_for_plan(
        &mut self,
        shard_id: &ShardId,
        strategy: &StrategyRecord,
        plan: StrategyOrderPlan,
    ) -> Vec<TradeCommand> {
        self.active_order_count += 2;
        match plan.action {
            StrategyAction::Open => self.pending_open_notional += plan.notional_usd,
            StrategyAction::Close => self.pending_close_notional += plan.notional_usd,
        }

        let long_side = match plan.action {
            StrategyAction::Open => Side::Buy,
            StrategyAction::Close => Side::Sell,
        };
        let short_side = match plan.action {
            StrategyAction::Open => Side::Sell,
            StrategyAction::Close => Side::Buy,
        };

        vec![
            TradeCommand::PlaceOrder {
                shard_id: shard_id.clone(),
                strategy_id: strategy.id.clone(),
                account_id: strategy.long_leg.account_id.clone(),
                exchange: strategy.long_leg.exchange,
                symbol: strategy.long_leg.symbol.clone(),
                side: long_side,
                qty: plan.long_qty,
                limit_price: Some(plan.long_limit_price),
                client_order_id: self.next_client_order_id(shard_id, &strategy.id, "long"),
            },
            TradeCommand::PlaceOrder {
                shard_id: shard_id.clone(),
                strategy_id: strategy.id.clone(),
                account_id: strategy.short_leg.account_id.clone(),
                exchange: strategy.short_leg.exchange,
                symbol: strategy.short_leg.symbol.clone(),
                side: short_side,
                qty: plan.short_qty,
                limit_price: Some(plan.short_limit_price),
                client_order_id: self.next_client_order_id(shard_id, &strategy.id, "short"),
            },
        ]
    }

    fn next_client_order_id(
        &mut self,
        shard_id: &ShardId,
        strategy_id: &StrategyId,
        leg: &str,
    ) -> ClientOrderId {
        self.next_order_seq += 1;
        ClientOrderId::new(format!(
            "{}-{}-{}-{}",
            shard_id.as_str(),
            strategy_id.as_str(),
            leg,
            self.next_order_seq
        ))
    }
}

#[derive(Clone, Copy, Debug)]
struct DepthMatch {
    notional_usd: f64,
    long_price: f64,
    short_price: f64,
}

#[derive(Clone, Copy, Debug)]
struct NormalizedPair {
    notional_usd: f64,
    long_qty: f64,
    short_qty: f64,
}

pub fn target_notional(levels: &[SpreadLevel], spread_pct: f64) -> f64 {
    levels
        .iter()
        .take_while(|level| spread_pct >= level.spread_pct)
        .map(|level| level.notional_usd)
        .sum()
}

pub fn strategy_markets(strategy: &StrategyRecord) -> [MarketKey; 2] {
    [
        MarketKey::new(strategy.long_leg.exchange, strategy.long_leg.symbol.clone()),
        MarketKey::new(strategy.short_leg.exchange, strategy.short_leg.symbol.clone()),
    ]
}

fn match_open_depth(
    long_book: &LocalBookState,
    short_book: &LocalBookState,
    desired_notional: f64,
) -> Option<DepthMatch> {
    match_depth(
        long_book.asks_desc().iter().rev(),
        short_book.bids_desc().iter(),
        desired_notional,
    )
}

fn match_close_depth(
    long_book: &LocalBookState,
    short_book: &LocalBookState,
    desired_notional: f64,
) -> Option<DepthMatch> {
    match_depth(
        long_book.bids_desc().iter(),
        short_book.asks_desc().iter().rev(),
        desired_notional,
    )
}

fn match_depth<'a>(
    long_levels: impl Iterator<Item = &'a PriceLevel>,
    short_levels: impl Iterator<Item = &'a PriceLevel>,
    desired_notional: f64,
) -> Option<DepthMatch> {
    if desired_notional <= 0.0 {
        return None;
    }

    // 两腿撮合只扫固定前10档，避免热路径分配和无界循环。
    let mut remaining = desired_notional;
    let mut matched = 0.0;
    let mut last_long_price = None;
    let mut last_short_price = None;
    let mut long_iter = long_levels.take(MAX_MATCH_LEVELS);
    let mut short_iter = short_levels.take(MAX_MATCH_LEVELS);
    let mut long_level = next_positive_level(&mut long_iter)?;
    let mut short_level = next_positive_level(&mut short_iter)?;
    let mut long_remaining = level_notional(long_level);
    let mut short_remaining = level_notional(short_level);

    while remaining > 0.0 {
        let take = remaining.min(long_remaining).min(short_remaining);
        matched += take;
        remaining -= take;
        last_long_price = Some(long_level.price);
        last_short_price = Some(short_level.price);

        long_remaining -= take;
        short_remaining -= take;

        if long_remaining <= 0.0 {
            long_level = match next_positive_level(&mut long_iter) {
                Some(level) => level,
                None => break,
            };
            long_remaining = level_notional(long_level);
        }
        if short_remaining <= 0.0 {
            short_level = match next_positive_level(&mut short_iter) {
                Some(level) => level,
                None => break,
            };
            short_remaining = level_notional(short_level);
        }
    }

    Some(DepthMatch {
        notional_usd: matched,
        long_price: last_long_price?,
        short_price: last_short_price?,
    })
    .filter(|matched| matched.notional_usd > 0.0)
}

fn next_positive_level<'a>(
    levels: &mut impl Iterator<Item = &'a PriceLevel>,
) -> Option<&'a PriceLevel> {
    levels.find(|level| level_notional(level) > 0.0)
}

fn level_notional(level: &PriceLevel) -> f64 {
    if level.price <= 0.0 || level.qty <= 0.0 {
        return 0.0;
    }
    level.price * level.qty
}

fn normalize_pair_notional(
    notional_usd: f64,
    long_price: f64,
    short_price: f64,
    long_rule: &MarketRule,
    short_rule: &MarketRule,
) -> Option<NormalizedPair> {
    let long_qty = normalize_qty(notional_usd / long_price, long_rule)?;
    let short_qty = normalize_qty(notional_usd / short_price, short_rule)?;
    let normalized_notional = (long_qty * long_price).min(short_qty * short_price);

    if !passes_minimums(long_qty, long_price, long_rule)
        || !passes_minimums(short_qty, short_price, short_rule)
        || normalized_notional <= 0.0
    {
        return None;
    }

    Some(NormalizedPair {
        notional_usd: normalized_notional,
        long_qty,
        short_qty,
    })
}

fn normalize_qty(qty: f64, rule: &MarketRule) -> Option<f64> {
    if !qty.is_finite() || qty <= 0.0 {
        return None;
    }

    let stepped = if let Some(step) = rule.qty_step.filter(|step| *step > 0.0) {
        (qty / step).floor() * step
    } else if let Some(precision) = rule.qty_precision {
        floor_to_precision(qty, precision)
    } else {
        qty
    };

    Some(stepped).filter(|qty| qty.is_finite() && *qty > 0.0)
}

fn passes_minimums(qty: f64, price: f64, rule: &MarketRule) -> bool {
    if rule.min_qty.is_some_and(|min_qty| qty < min_qty) {
        return false;
    }
    if rule
        .min_notional
        .is_some_and(|min_notional| qty * price < min_notional)
    {
        return false;
    }
    true
}

fn floor_to_precision(value: f64, precision: u32) -> f64 {
    let scale = 10_f64.powi(precision as i32);
    (value * scale).floor() / scale
}

fn spread_pct(exit_price: f64, entry_price: f64) -> Option<f64> {
    if entry_price <= 0.0 {
        return None;
    }
    Some((exit_price - entry_price) / entry_price * 100.0)
}

fn positive_delta(target: f64, current: f64) -> Option<f64> {
    let delta = target - current;
    (delta > 0.0).then_some(delta)
}
