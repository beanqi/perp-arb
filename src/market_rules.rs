use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, RwLock},
    time::Duration,
};

use arc_swap::ArcSwap;
use reqwest::Client;
use serde::Deserialize;
use tokio::{task::JoinHandle, time::MissedTickBehavior};
use tracing::{info, warn};

use crate::config::model::{Exchange, MarketKey, now_ms};

const BINANCE_USD_M_EXCHANGE_INFO: &str = "https://fapi.binance.com/fapi/v1/exchangeInfo";
const BYBIT_LINEAR_INSTRUMENTS: &str = "https://api.bybit.com/v5/market/instruments-info";
pub const DEFAULT_MARKET_RULE_REFRESH_INTERVAL: Duration = Duration::from_secs(60 * 60);

#[derive(Clone, Debug)]
pub struct MarketRule {
    pub market: MarketKey,
    pub min_qty: Option<f64>,
    pub min_notional: Option<f64>,
    pub price_tick: Option<f64>,
    pub qty_step: Option<f64>,
    pub price_precision: Option<u32>,
    pub qty_precision: Option<u32>,
    pub contract_multiplier: Option<f64>,
    pub updated_at_ms: u64,
}

#[derive(Clone, Debug)]
pub struct MarketRuleSnapshot {
    pub updated_at_ms: u64,
    rules: HashMap<MarketKey, MarketRule>,
}

impl MarketRuleSnapshot {
    fn empty() -> Self {
        Self {
            updated_at_ms: 0,
            rules: HashMap::new(),
        }
    }

    pub fn get(&self, market: &MarketKey) -> Option<&MarketRule> {
        self.rules.get(market)
    }

    pub fn len(&self) -> usize {
        self.rules.len()
    }
}

#[derive(Debug)]
pub struct MarketRuleStore {
    // 撮合/下单热路径只 load Arc 快照并查 HashMap，不需要拿锁。
    snapshot: ArcSwap<MarketRuleSnapshot>,
    watched_markets: RwLock<BTreeSet<MarketKey>>,
}

impl Default for MarketRuleStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MarketRuleStore {
    pub fn new() -> Self {
        Self {
            snapshot: ArcSwap::from_pointee(MarketRuleSnapshot::empty()),
            watched_markets: RwLock::new(BTreeSet::new()),
        }
    }

    pub fn watch_markets(&self, markets: impl IntoIterator<Item = MarketKey>) {
        let markets = markets.into_iter().collect::<BTreeSet<_>>();
        if let Ok(mut watched) = self.watched_markets.write() {
            *watched = markets;
        }
    }

    pub fn watched_markets(&self) -> Vec<MarketKey> {
        self.watched_markets
            .read()
            .map(|markets| markets.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn snapshot(&self) -> Arc<MarketRuleSnapshot> {
        self.snapshot.load_full()
    }

    pub fn get(&self, market: &MarketKey) -> Option<MarketRule> {
        self.snapshot.load().get(market).cloned()
    }

    fn replace_rules(&self, rules: HashMap<MarketKey, MarketRule>) {
        self.snapshot.store(Arc::new(MarketRuleSnapshot {
            updated_at_ms: now_ms(),
            rules,
        }));
    }
}

#[derive(Debug)]
pub struct MarketRuleRefreshHandle {
    task: JoinHandle<()>,
}

impl MarketRuleRefreshHandle {
    pub fn spawn(store: Arc<MarketRuleStore>, interval: Duration) -> Self {
        let task = tokio::spawn(async move {
            let client = Client::new();
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

            loop {
                refresh_market_rules(&store, &client).await;
                ticker.tick().await;
            }
        });
        Self { task }
    }
}

impl Drop for MarketRuleRefreshHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub async fn refresh_market_rules(store: &MarketRuleStore, client: &Client) {
    let watched = store.watched_markets();
    if watched.is_empty() {
        return;
    }

    let mut next_rules = store.snapshot().rules.clone();
    let mut refreshed = 0_usize;

    for exchange in [Exchange::BinanceUsdM, Exchange::BybitLinear] {
        let symbols = watched
            .iter()
            .filter(|market| market.exchange == exchange)
            .map(|market| market.symbol.clone())
            .collect::<BTreeSet<_>>();
        if symbols.is_empty() {
            continue;
        }

        match fetch_exchange_rules(client, exchange, &symbols).await {
            Ok(rules) => {
                refreshed += rules.len();
                for rule in rules {
                    info!("market rule updated {:?}", rule);
                    next_rules.insert(rule.market.clone(), rule);
                }
            }
            Err(error) => warn!("market rule refresh exchange={} failed: {}", exchange, error),
        }
    }

    next_rules.retain(|market, _| watched.contains(market));
    store.replace_rules(next_rules);
    info!(
        "market rules refreshed watched={} refreshed={} cached={}",
        watched.len(),
        refreshed,
        store.snapshot().len()
    );
}

async fn fetch_exchange_rules(
    client: &Client,
    exchange: Exchange,
    symbols: &BTreeSet<String>,
) -> Result<Vec<MarketRule>, String> {
    match exchange {
        Exchange::BinanceUsdM => fetch_binance_usd_m_rules(client, symbols).await,
        Exchange::BybitLinear => fetch_bybit_linear_rules(client, symbols).await,
    }
}

async fn fetch_binance_usd_m_rules(
    client: &Client,
    symbols: &BTreeSet<String>,
) -> Result<Vec<MarketRule>, String> {
    let response = client
        .get(BINANCE_USD_M_EXCHANGE_INFO)
        .send()
        .await
        .map_err(|error| format!("request failed: {error}"))?
        .error_for_status()
        .map_err(|error| format!("bad status: {error}"))?
        .json::<BinanceExchangeInfo>()
        .await
        .map_err(|error| format!("decode failed: {error}"))?;

    let now = now_ms();
    Ok(response
        .symbols
        .into_iter()
        .filter(|symbol| symbols.contains(&symbol.symbol))
        .map(|symbol| binance_rule(symbol, now))
        .collect())
}

async fn fetch_bybit_linear_rules(
    client: &Client,
    symbols: &BTreeSet<String>,
) -> Result<Vec<MarketRule>, String> {
    let mut cursor = None::<String>;
    let mut instruments = Vec::new();

    loop {
        let mut request = client
            .get(BYBIT_LINEAR_INSTRUMENTS)
            .query(&[("category", "linear"), ("limit", "1000")]);
        if let Some(cursor) = &cursor {
            request = request.query(&[("cursor", cursor)]);
        }

        let response = request
            .send()
            .await
            .map_err(|error| format!("request failed: {error}"))?
            .error_for_status()
            .map_err(|error| format!("bad status: {error}"))?
            .json::<BybitInstrumentResponse>()
            .await
            .map_err(|error| format!("decode failed: {error}"))?;

        if response.ret_code != 0 {
            return Err(format!("ret_code={} ret_msg={}", response.ret_code, response.ret_msg));
        }

        instruments.extend(response.result.list);
        cursor = response.result.next_page_cursor.filter(|value| !value.is_empty());
        if cursor.is_none() {
            break;
        }
    }

    let now = now_ms();
    Ok(instruments
        .into_iter()
        .filter(|instrument| symbols.contains(&instrument.symbol))
        .map(|instrument| bybit_rule(instrument, now))
        .collect())
}

fn binance_rule(symbol: BinanceSymbol, updated_at_ms: u64) -> MarketRule {
    let mut price_tick = None;
    let mut qty_step = None;
    let mut min_qty = None;
    let mut min_notional = None;

    for filter in symbol.filters {
        match filter.filter_type.as_str() {
            "PRICE_FILTER" => price_tick = parse_decimal(filter.tick_size.as_deref()),
            "LOT_SIZE" | "MARKET_LOT_SIZE" => {
                qty_step = qty_step.or_else(|| parse_decimal(filter.step_size.as_deref()));
                min_qty = min_qty.or_else(|| parse_decimal(filter.min_qty.as_deref()));
            }
            "MIN_NOTIONAL" | "NOTIONAL" => {
                min_notional = min_notional.or_else(|| parse_decimal(filter.notional.as_deref()));
                min_notional = min_notional.or_else(|| parse_decimal(filter.min_notional.as_deref()));
            }
            _ => {}
        }
    }

    MarketRule {
        market: MarketKey::new(Exchange::BinanceUsdM, symbol.symbol),
        min_qty,
        min_notional,
        price_tick,
        qty_step,
        price_precision: symbol.price_precision,
        qty_precision: symbol.quantity_precision,
        contract_multiplier: Some(1.0),
        updated_at_ms,
    }
}

fn bybit_rule(instrument: BybitInstrument, updated_at_ms: u64) -> MarketRule {
    let qty_step = instrument.lot_size_filter.qty_step.as_deref();
    MarketRule {
        market: MarketKey::new(Exchange::BybitLinear, instrument.symbol),
        min_qty: parse_decimal(instrument.lot_size_filter.min_order_qty.as_deref()),
        min_notional: parse_decimal(instrument.lot_size_filter.min_notional_value.as_deref()),
        price_tick: parse_decimal(instrument.price_filter.tick_size.as_deref()),
        qty_step: parse_decimal(qty_step),
        price_precision: instrument.price_scale.and_then(|value| value.parse::<u32>().ok()),
        qty_precision: qty_step.and_then(decimal_precision_from_step),
        contract_multiplier: parse_decimal(instrument.contract_size.as_deref()).or(Some(1.0)),
        updated_at_ms,
    }
}

fn parse_decimal(value: Option<&str>) -> Option<f64> {
    value.and_then(|value| value.parse::<f64>().ok())
}

fn decimal_precision_from_step(value: &str) -> Option<u32> {
    let normalized = value.trim().trim_end_matches('0');
    let (_, decimals) = normalized.split_once('.')?;
    Some(decimals.len() as u32)
}

#[derive(Debug, Deserialize)]
struct BinanceExchangeInfo {
    symbols: Vec<BinanceSymbol>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceSymbol {
    symbol: String,
    price_precision: Option<u32>,
    quantity_precision: Option<u32>,
    filters: Vec<BinanceFilter>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceFilter {
    #[serde(rename = "filterType")]
    filter_type: String,
    tick_size: Option<String>,
    step_size: Option<String>,
    min_qty: Option<String>,
    notional: Option<String>,
    min_notional: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitInstrumentResponse {
    #[serde(rename = "retCode")]
    ret_code: i64,
    #[serde(rename = "retMsg")]
    ret_msg: String,
    result: BybitInstrumentResult,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitInstrumentResult {
    list: Vec<BybitInstrument>,
    next_page_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitInstrument {
    symbol: String,
    price_scale: Option<String>,
    price_filter: BybitPriceFilter,
    lot_size_filter: BybitLotSizeFilter,
    contract_size: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitPriceFilter {
    tick_size: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitLotSizeFilter {
    min_order_qty: Option<String>,
    min_notional_value: Option<String>,
    qty_step: Option<String>,
}
