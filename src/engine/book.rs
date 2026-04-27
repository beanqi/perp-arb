mod codec;

use std::{cmp::Ordering, time::Instant};

use serde::{Deserialize, Serialize};

use crate::config::model::{DepthMode, MarketKey};

pub use codec::{decode_raw_depth, decode_raw_depth_with_timing};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: f64,
    pub qty: f64,
}

#[derive(Clone, Debug)]
pub struct LocalBookState {
    pub market: MarketKey,
    pub mode: DepthMode,
    pub has_snapshot: bool,
    pub last_sequence: Option<u64>,
    // bids 升序、asks 降序，让买一/卖一都在数组尾部，减少热档位增量更新时的搬移。
    bids_asc: Vec<PriceLevel>,
    asks_desc: Vec<PriceLevel>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RawDepthMessage {
    Snapshot {
        market: MarketKey,
        sequence: Option<u64>,
        bids: Vec<PriceLevel>,
        asks: Vec<PriceLevel>,
    },
    Delta {
        market: MarketKey,
        first_sequence: Option<u64>,
        previous_sequence: Option<u64>,
        sequence: Option<u64>,
        bids: Vec<PriceLevel>,
        asks: Vec<PriceLevel>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BookApplyResult {
    Applied,
    IgnoredStale,
    WaitingForSnapshot,
    GapDetected,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct DepthDecodeTiming {
    pub deserialize_us: u128,
    pub normalize_us: u128,
}

#[derive(Clone, Debug)]
pub struct DecodedDepthMessage {
    pub message: RawDepthMessage,
    pub timing: DepthDecodeTiming,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct BookApplyTiming {
    pub total_us: u128,
    pub validation_us: u128,
    pub sort_bids_us: u128,
    pub sort_asks_us: u128,
    pub merge_bids_us: u128,
    pub merge_asks_us: u128,
}

#[derive(Clone, Copy, Debug)]
pub struct TimedBookApplyResult {
    pub result: BookApplyResult,
    pub timing: BookApplyTiming,
}

impl RawDepthMessage {
    pub fn market(&self) -> &MarketKey {
        match self {
            Self::Snapshot { market, .. } | Self::Delta { market, .. } => market,
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::Snapshot { .. } => "snapshot",
            Self::Delta { .. } => "delta",
        }
    }

    pub fn level_counts(&self) -> (usize, usize) {
        match self {
            Self::Snapshot { bids, asks, .. } | Self::Delta { bids, asks, .. } => {
                (bids.len(), asks.len())
            }
        }
    }
}

impl LocalBookState {
    pub fn new(market: MarketKey, mode: DepthMode) -> Self {
        Self {
            market,
            mode,
            has_snapshot: false,
            last_sequence: None,
            bids_asc: Vec::new(),
            asks_desc: Vec::new(),
        }
    }

    pub fn apply(&mut self, message: RawDepthMessage) -> BookApplyResult {
        self.apply_timed(message).result
    }

    pub fn apply_timed(&mut self, message: RawDepthMessage) -> TimedBookApplyResult {
        let total_started_at = Instant::now();
        let mut timing = BookApplyTiming::default();
        match message {
            RawDepthMessage::Snapshot {
                sequence,
                mut bids,
                mut asks,
                ..
            } => {
                let sort_bids_started_at = Instant::now();
                sort_asc(&mut bids);
                timing.sort_bids_us = sort_bids_started_at.elapsed().as_micros();
                let sort_asks_started_at = Instant::now();
                sort_desc(&mut asks);
                timing.sort_asks_us = sort_asks_started_at.elapsed().as_micros();
                self.bids_asc = bids;
                self.asks_desc = asks;
                self.has_snapshot = true;
                self.last_sequence = sequence;
                timing.total_us = total_started_at.elapsed().as_micros();
                TimedBookApplyResult {
                    result: BookApplyResult::Applied,
                    timing,
                }
            }
            RawDepthMessage::Delta {
                first_sequence,
                previous_sequence,
                sequence,
                bids,
                asks,
                ..
            } => {
                let validation_started_at = Instant::now();
                if self.mode == DepthMode::SnapshotThenIncremental && !self.has_snapshot {
                    timing.validation_us = validation_started_at.elapsed().as_micros();
                    timing.total_us = total_started_at.elapsed().as_micros();
                    return TimedBookApplyResult {
                        result: BookApplyResult::WaitingForSnapshot,
                        timing,
                    };
                }

                if let Some(last) = self.last_sequence {
                    if let Some(current) = sequence {
                        if current <= last {
                            timing.validation_us = validation_started_at.elapsed().as_micros();
                            timing.total_us = total_started_at.elapsed().as_micros();
                            return TimedBookApplyResult {
                                result: BookApplyResult::IgnoredStale,
                                timing,
                            };
                        }
                    }

                    if let Some(previous) = previous_sequence {
                        let bridges_snapshot = first_sequence
                            .is_some_and(|first| first <= last + 1)
                            && sequence.is_some_and(|current| current >= last + 1);
                        if previous != last && !bridges_snapshot {
                            self.clear_for_rebuild();
                            timing.validation_us = validation_started_at.elapsed().as_micros();
                            timing.total_us = total_started_at.elapsed().as_micros();
                            return TimedBookApplyResult {
                                result: BookApplyResult::GapDetected,
                                timing,
                            };
                        }
                    } else if let Some(first) = first_sequence {
                        if first > last + 1 {
                            self.clear_for_rebuild();
                            timing.validation_us = validation_started_at.elapsed().as_micros();
                            timing.total_us = total_started_at.elapsed().as_micros();
                            return TimedBookApplyResult {
                                result: BookApplyResult::GapDetected,
                                timing,
                            };
                        }
                    }
                }
                timing.validation_us = validation_started_at.elapsed().as_micros();

                let merge_bids_started_at = Instant::now();
                apply_levels(&mut self.bids_asc, bids, compare_price_asc);
                timing.merge_bids_us = merge_bids_started_at.elapsed().as_micros();
                let merge_asks_started_at = Instant::now();
                apply_levels(&mut self.asks_desc, asks, compare_price_desc);
                timing.merge_asks_us = merge_asks_started_at.elapsed().as_micros();
                self.has_snapshot = true;
                self.last_sequence = sequence.or(self.last_sequence);
                timing.total_us = total_started_at.elapsed().as_micros();
                TimedBookApplyResult {
                    result: BookApplyResult::Applied,
                    timing,
                }
            }
        }
    }

    pub fn best_bid(&self) -> Option<&PriceLevel> {
        self.bids_asc.last()
    }

    pub fn best_ask(&self) -> Option<&PriceLevel> {
        self.asks_desc.last()
    }

    pub fn bids_asc(&self) -> &[PriceLevel] {
        &self.bids_asc
    }

    pub fn asks_desc(&self) -> &[PriceLevel] {
        &self.asks_desc
    }

    pub fn level_counts(&self) -> (usize, usize) {
        (self.bids_asc.len(), self.asks_desc.len())
    }

    fn clear_for_rebuild(&mut self) {
        self.has_snapshot = false;
        self.last_sequence = None;
        self.bids_asc.clear();
        self.asks_desc.clear();
    }
}

fn sort_asc(levels: &mut [PriceLevel]) {
    levels.sort_by(|left, right| compare_price_asc(left.price, right.price));
}

fn sort_desc(levels: &mut [PriceLevel]) {
    levels.sort_by(|left, right| compare_price_desc(left.price, right.price));
}

fn apply_levels(
    book_side: &mut Vec<PriceLevel>,
    updates: Vec<PriceLevel>,
    compare_price: fn(f64, f64) -> Ordering,
) {
    for update in updates {
        match book_side.binary_search_by(|level| compare_price(level.price, update.price)) {
            Ok(index) if update.qty == 0.0 => {
                book_side.remove(index);
            }
            Ok(index) => {
                book_side[index].qty = update.qty;
            }
            Err(index) if update.qty != 0.0 => {
                book_side.insert(index, update);
            }
            Err(_) => {}
        }
    }
}

fn compare_price_asc(left: f64, right: f64) -> Ordering {
    left.partial_cmp(&right).unwrap_or(Ordering::Equal)
}

fn compare_price_desc(left: f64, right: f64) -> Ordering {
    right.partial_cmp(&left).unwrap_or(Ordering::Equal)
}
