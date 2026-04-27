use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
    thread,
    time::Duration,
};

use arc_swap::ArcSwap;
use crossbeam_channel::{Receiver, Sender, bounded, select, tick};
use serde::Serialize;
use tracing::warn;

use crate::config::{
    ids::{ShardId, StrategyId},
    model::{MarketKey, now_ms},
};

const TELEMETRY_QUEUE_BOUND: usize = 8192;
const MATCH_EVENT_LIMIT: usize = 200;
const SNAPSHOT_PUBLISH_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone, Debug, Serialize)]
pub struct StrategyRuntimeSnapshot {
    pub generated_at_ms: u64,
    pub strategies: Vec<StrategyRuntimeView>,
    pub match_events: Vec<StrategyMatchEvent>,
}

impl StrategyRuntimeSnapshot {
    fn empty() -> Self {
        Self {
            generated_at_ms: now_ms(),
            strategies: Vec::new(),
            match_events: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct StrategyRuntimeView {
    pub strategy_id: StrategyId,
    pub name: String,
    pub enabled: bool,
    pub long_market: MarketKey,
    pub short_market: MarketKey,
    pub open_spread_pct: Option<f64>,
    pub close_spread_pct: Option<f64>,
    pub current_pair_notional: f64,
    pub pending_open_notional: f64,
    pub pending_close_notional: f64,
    pub active_order_count: u32,
    pub last_updated_ms: u64,
}

#[derive(Clone, Debug)]
pub struct StrategyRuntimeDescriptor {
    pub strategy_id: StrategyId,
    pub name: String,
    pub enabled: bool,
    pub long_market: MarketKey,
    pub short_market: MarketKey,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyMatchEventKind {
    SpreadUpdated,
    OpenPlanned,
    ClosePlanned,
}

#[derive(Clone, Debug, Serialize)]
pub struct StrategyMatchEvent {
    pub ts_ms: u64,
    pub shard_id: ShardId,
    pub strategy_id: StrategyId,
    pub kind: StrategyMatchEventKind,
    pub open_spread_pct: Option<f64>,
    pub close_spread_pct: Option<f64>,
    pub notional_usd: Option<f64>,
    pub message: String,
}

#[derive(Clone, Debug)]
pub struct StrategyRuntimeMetrics {
    pub open_spread_pct: Option<f64>,
    pub close_spread_pct: Option<f64>,
    pub current_pair_notional: f64,
    pub pending_open_notional: f64,
    pub pending_close_notional: f64,
    pub active_order_count: u32,
}

#[derive(Clone, Debug)]
pub enum StrategyTelemetryEvent {
    RuntimeLoaded {
        generation: u64,
        strategies: Vec<StrategyRuntimeDescriptor>,
    },
    MetricsUpdated {
        generation: u64,
        shard_id: ShardId,
        strategy_id: StrategyId,
        metrics: StrategyRuntimeMetrics,
        ts_ms: u64,
    },
    OrderPlanned {
        generation: u64,
        event: StrategyMatchEvent,
    },
    Shutdown,
}

#[derive(Debug)]
pub struct StrategyTelemetryHub {
    snapshot: Arc<ArcSwap<StrategyRuntimeSnapshot>>,
    tx: Sender<StrategyTelemetryEvent>,
    worker: Option<thread::JoinHandle<()>>,
}

impl StrategyTelemetryHub {
    pub fn spawn() -> Self {
        let snapshot = Arc::new(ArcSwap::from_pointee(StrategyRuntimeSnapshot::empty()));
        let (tx, rx) = bounded(TELEMETRY_QUEUE_BOUND);
        let worker_snapshot = snapshot.clone();
        let worker = thread::Builder::new()
            .name("strategy-telemetry".to_owned())
            .spawn(move || telemetry_loop(rx, worker_snapshot))
            .ok();

        Self {
            snapshot,
            tx,
            worker,
        }
    }

    pub fn sender(&self) -> Sender<StrategyTelemetryEvent> {
        self.tx.clone()
    }

    pub fn publish_loaded(&self, generation: u64, strategies: Vec<StrategyRuntimeDescriptor>) {
        if let Err(error) = self.tx.try_send(StrategyTelemetryEvent::RuntimeLoaded {
            generation,
            strategies,
        }) {
            warn!("strategy telemetry runtime load event dropped: {}", error);
        }
    }

    pub fn snapshot(&self) -> Arc<StrategyRuntimeSnapshot> {
        self.snapshot.load_full()
    }
}

impl Drop for StrategyTelemetryHub {
    fn drop(&mut self) {
        let _ = self.tx.try_send(StrategyTelemetryEvent::Shutdown);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn telemetry_loop(
    rx: Receiver<StrategyTelemetryEvent>,
    snapshot: Arc<ArcSwap<StrategyRuntimeSnapshot>>,
) {
    let mut generation = 0_u64;
    let mut views = BTreeMap::<StrategyId, StrategyRuntimeView>::new();
    let mut events = VecDeque::<StrategyMatchEvent>::with_capacity(MATCH_EVENT_LIMIT);
    let publish_tick = tick(SNAPSHOT_PUBLISH_INTERVAL);
    let mut dirty = false;

    loop {
        select! {
            recv(rx) -> event => match event {
                Ok(StrategyTelemetryEvent::RuntimeLoaded {
                    generation: next_generation,
                    strategies,
                }) => {
                    generation = next_generation;
                    views.clear();
                    events.clear();
                    for descriptor in strategies {
                        views.insert(
                            descriptor.strategy_id.clone(),
                            StrategyRuntimeView {
                                strategy_id: descriptor.strategy_id,
                                name: descriptor.name,
                                enabled: descriptor.enabled,
                                long_market: descriptor.long_market,
                                short_market: descriptor.short_market,
                                open_spread_pct: None,
                                close_spread_pct: None,
                                current_pair_notional: 0.0,
                                pending_open_notional: 0.0,
                                pending_close_notional: 0.0,
                                active_order_count: 0,
                                last_updated_ms: 0,
                            },
                        );
                    }
                    publish_snapshot(&snapshot, &views, &events);
                    dirty = false;
                }
                Ok(StrategyTelemetryEvent::MetricsUpdated {
                    generation: event_generation,
                    shard_id,
                    strategy_id,
                    metrics,
                    ts_ms,
                }) => {
                    if event_generation != generation {
                        continue;
                    }
                    if let Some(view) = views.get_mut(&strategy_id) {
                        view.open_spread_pct = metrics.open_spread_pct;
                        view.close_spread_pct = metrics.close_spread_pct;
                        view.current_pair_notional = metrics.current_pair_notional;
                        view.pending_open_notional = metrics.pending_open_notional;
                        view.pending_close_notional = metrics.pending_close_notional;
                        view.active_order_count = metrics.active_order_count;
                        view.last_updated_ms = ts_ms;
                        let _ = shard_id;
                        dirty = true;
                    }
                }
                Ok(StrategyTelemetryEvent::OrderPlanned {
                    generation: event_generation,
                    event,
                }) => {
                    if event_generation != generation {
                        continue;
                    }
                    push_event(&mut events, event);
                    dirty = true;
                }
                Ok(StrategyTelemetryEvent::Shutdown) => {
                    if dirty {
                        publish_snapshot(&snapshot, &views, &events);
                    }
                    break;
                }
                Err(_) => break,
            },
            recv(publish_tick) -> _ => {
                if dirty {
                    publish_snapshot(&snapshot, &views, &events);
                    dirty = false;
                }
            },
        }
    }
}

fn push_event(events: &mut VecDeque<StrategyMatchEvent>, event: StrategyMatchEvent) {
    if events.len() == MATCH_EVENT_LIMIT {
        events.pop_front();
    }
    events.push_back(event);
}

fn publish_snapshot(
    snapshot: &ArcSwap<StrategyRuntimeSnapshot>,
    views: &BTreeMap<StrategyId, StrategyRuntimeView>,
    events: &VecDeque<StrategyMatchEvent>,
) {
    snapshot.store(Arc::new(StrategyRuntimeSnapshot {
        generated_at_ms: now_ms(),
        strategies: views.values().cloned().collect(),
        match_events: events.iter().cloned().collect(),
    }));
}
