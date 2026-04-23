use crate::config::ids::StrategyId;

#[derive(Clone, Debug, Default)]
pub struct StrategyRuntimeState {
    pub strategy_id: Option<StrategyId>,
    pub last_open_spread_pct: Option<f64>,
    pub last_close_spread_pct: Option<f64>,
}
