use std::{
    fmt::{Display, Formatter},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

use crate::config::{
    crypto::EncryptedValue,
    ids::{AccountId, ClientOrderId, ConnectionId, ShardId, StrategyId},
};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum Exchange {
    BinanceUsdM,
    BybitLinear,
}

impl Exchange {
    pub fn capabilities(&self) -> ExchangeCapabilities {
        match self {
            Self::BinanceUsdM => ExchangeCapabilities {
                depth_mode: DepthMode::SnapshotThenIncremental,
                max_symbols_per_connection: 64,
            },
            Self::BybitLinear => ExchangeCapabilities {
                depth_mode: DepthMode::SnapshotThenIncremental,
                max_symbols_per_connection: 32,
            },
        }
    }
}

impl Display for Exchange {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::BinanceUsdM => "binance_usd_m",
            Self::BybitLinear => "bybit_linear",
        };
        f.write_str(value)
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DepthMode {
    SnapshotThenIncremental,
    FullSnapshotOnly,
}

impl Display for DepthMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::SnapshotThenIncremental => "snapshot_then_incremental",
            Self::FullSnapshotOnly => "full_snapshot_only",
        };
        f.write_str(value)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ExchangeCapabilities {
    pub depth_mode: DepthMode,
    pub max_symbols_per_connection: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MarketKey {
    pub exchange: Exchange,
    pub symbol: String,
}

impl MarketKey {
    pub fn new(exchange: Exchange, symbol: impl Into<String>) -> Self {
        Self {
            exchange,
            symbol: symbol.into(),
        }
    }
}

impl Display for MarketKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.exchange, self.symbol)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SpreadLevel {
    pub spread_pct: f64,
    pub notional_usd: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StrategyLeg {
    pub exchange: Exchange,
    pub symbol: String,
    pub account_id: AccountId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StrategyUpsertRequest {
    pub id: StrategyId,
    pub name: String,
    pub enabled: bool,
    pub long_leg: StrategyLeg,
    pub short_leg: StrategyLeg,
    pub open_levels: Vec<SpreadLevel>,
    pub close_levels: Vec<SpreadLevel>,
    pub max_total_notional: f64,
    pub max_open_orders: u32,
    pub stale_order_query_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StrategyRecord {
    pub id: StrategyId,
    pub name: String,
    pub enabled: bool,
    pub long_leg: StrategyLeg,
    pub short_leg: StrategyLeg,
    pub open_levels: Vec<SpreadLevel>,
    pub close_levels: Vec<SpreadLevel>,
    pub max_total_notional: f64,
    pub max_open_orders: u32,
    pub stale_order_query_ms: u64,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

impl StrategyRecord {
    pub fn market_keys(&self) -> Vec<MarketKey> {
        vec![
            MarketKey::new(self.long_leg.exchange, self.long_leg.symbol.clone()),
            MarketKey::new(self.short_leg.exchange, self.short_leg.symbol.clone()),
        ]
    }
}

#[derive(Clone, Debug)]
pub struct ValidatedStrategy {
    pub id: StrategyId,
    pub name: String,
    pub enabled: bool,
    pub long_leg: StrategyLeg,
    pub short_leg: StrategyLeg,
    pub open_levels: Vec<SpreadLevel>,
    pub close_levels: Vec<SpreadLevel>,
    pub max_total_notional: f64,
    pub max_open_orders: u32,
    pub stale_order_query_ms: u64,
}

impl ValidatedStrategy {
    pub fn into_record(self, created_at_ms: u64, updated_at_ms: u64) -> StrategyRecord {
        StrategyRecord {
            id: self.id,
            name: self.name,
            enabled: self.enabled,
            long_leg: self.long_leg,
            short_leg: self.short_leg,
            open_levels: self.open_levels,
            close_levels: self.close_levels,
            max_total_notional: self.max_total_notional,
            max_open_orders: self.max_open_orders,
            stale_order_query_ms: self.stale_order_query_ms,
            created_at_ms,
            updated_at_ms,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountValidationStatus {
    Pending,
    DryRunOk,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AccountUpsertRequest {
    pub id: AccountId,
    pub name: String,
    pub exchange: Exchange,
    pub enabled: bool,
    pub api_key: String,
    pub api_secret: String,
    pub passphrase: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EncryptedAccountSecrets {
    pub api_key: EncryptedValue,
    pub api_secret: EncryptedValue,
    pub passphrase: Option<EncryptedValue>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AccountRecord {
    pub id: AccountId,
    pub name: String,
    pub exchange: Exchange,
    pub enabled: bool,
    pub masked_api_key: String,
    pub validation_status: AccountValidationStatus,
    pub credentials: EncryptedAccountSecrets,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Clone, Debug)]
pub struct ValidatedAccountInput {
    pub id: AccountId,
    pub name: String,
    pub exchange: Exchange,
    pub enabled: bool,
    pub api_key: String,
    pub api_secret: String,
    pub passphrase: Option<String>,
    pub masked_api_key: String,
    pub validation_status: AccountValidationStatus,
}

impl AccountRecord {
    pub fn from_validated(
        input: ValidatedAccountInput,
        credentials: EncryptedAccountSecrets,
        created_at_ms: u64,
        updated_at_ms: u64,
    ) -> Self {
        Self {
            id: input.id,
            name: input.name,
            exchange: input.exchange,
            enabled: input.enabled,
            masked_api_key: input.masked_api_key,
            validation_status: input.validation_status,
            credentials,
            created_at_ms,
            updated_at_ms,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct AccountView {
    pub id: AccountId,
    pub name: String,
    pub exchange: Exchange,
    pub enabled: bool,
    pub masked_api_key: String,
    pub validation_status: AccountValidationStatus,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

impl AccountView {
    pub fn from_record(record: &AccountRecord) -> Self {
        Self {
            id: record.id.clone(),
            name: record.name.clone(),
            exchange: record.exchange,
            enabled: record.enabled,
            masked_api_key: record.masked_api_key.clone(),
            validation_status: record.validation_status.clone(),
            created_at_ms: record.created_at_ms,
            updated_at_ms: record.updated_at_ms,
        }
    }
}

#[derive(Clone, Debug)]
pub struct AccountCredentials {
    pub api_key: String,
    pub api_secret: String,
    pub passphrase: Option<String>,
}

#[derive(Clone, Debug)]
pub struct RuntimeAccount {
    pub id: AccountId,
    pub name: String,
    pub exchange: Exchange,
    pub credentials: AccountCredentials,
}

#[derive(Clone, Debug)]
pub struct RuntimeCatalog {
    pub store_revision: u64,
    pub enabled_strategies: Vec<StrategyRecord>,
    pub referenced_accounts: Vec<RuntimeAccount>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PersistedStore {
    pub schema_version: u32,
    pub revision: u64,
    pub strategies: Vec<StrategyRecord>,
    pub accounts: Vec<AccountRecord>,
}

impl Default for PersistedStore {
    fn default() -> Self {
        Self {
            schema_version: 1,
            revision: 0,
            strategies: Vec::new(),
            accounts: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct PositionView {
    pub strategy_id: StrategyId,
    pub exchange: Exchange,
    pub symbol: String,
    pub net_qty: f64,
    pub notional_usd: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct BalanceView {
    pub account_id: AccountId,
    pub exchange: Exchange,
    pub asset: String,
    pub total: f64,
    pub free: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct ActiveOrderView {
    pub strategy_id: StrategyId,
    pub exchange: Exchange,
    pub symbol: String,
    pub client_order_id: ClientOrderId,
    pub status: String,
    pub qty: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StrategyToggleRequest {
    pub enabled: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct StrategyPlacementView {
    pub strategy_id: StrategyId,
    pub shard_id: ShardId,
    pub connection_ids: Vec<ConnectionId>,
    pub markets: Vec<MarketKey>,
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
