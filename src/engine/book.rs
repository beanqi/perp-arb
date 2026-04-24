mod codec;

use std::cmp::Ordering;

use crate::config::model::{DepthMode, MarketKey};

pub use codec::decode_raw_depth;

#[derive(Clone, Debug)]
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
    bids_desc: Vec<PriceLevel>,
    asks_desc: Vec<PriceLevel>,
}

#[derive(Clone, Debug)]
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

impl RawDepthMessage {
    pub fn market(&self) -> &MarketKey {
        match self {
            Self::Snapshot { market, .. } | Self::Delta { market, .. } => market,
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
            bids_desc: Vec::new(),
            asks_desc: Vec::new(),
        }
    }

    pub fn apply(&mut self, message: RawDepthMessage) -> BookApplyResult {
        match message {
            RawDepthMessage::Snapshot {
                sequence,
                mut bids,
                mut asks,
                ..
            } => {
                sort_desc(&mut bids);
                sort_desc(&mut asks);
                self.bids_desc = bids;
                self.asks_desc = asks;
                self.has_snapshot = true;
                self.last_sequence = sequence;
                BookApplyResult::Applied
            }
            RawDepthMessage::Delta {
                first_sequence,
                previous_sequence,
                sequence,
                bids,
                asks,
                ..
            } => {
                if self.mode == DepthMode::SnapshotThenIncremental && !self.has_snapshot {
                    return BookApplyResult::WaitingForSnapshot;
                }

                if let Some(last) = self.last_sequence {
                    if let Some(current) = sequence {
                        if current <= last {
                            return BookApplyResult::IgnoredStale;
                        }
                    }

                    if let Some(previous) = previous_sequence {
                        let bridges_snapshot = first_sequence
                            .is_some_and(|first| first <= last + 1)
                            && sequence.is_some_and(|current| current >= last + 1);
                        if previous != last && !bridges_snapshot {
                            self.clear_for_rebuild();
                            return BookApplyResult::GapDetected;
                        }
                    } else if let Some(first) = first_sequence {
                        if first > last + 1 {
                            self.clear_for_rebuild();
                            return BookApplyResult::GapDetected;
                        }
                    }
                }

                apply_levels(&mut self.bids_desc, bids);
                apply_levels(&mut self.asks_desc, asks);
                self.has_snapshot = true;
                self.last_sequence = sequence.or(self.last_sequence);
                BookApplyResult::Applied
            }
        }
    }

    pub fn best_bid(&self) -> Option<&PriceLevel> {
        self.bids_desc.first()
    }

    pub fn best_ask(&self) -> Option<&PriceLevel> {
        self.asks_desc.last()
    }

    pub fn level_counts(&self) -> (usize, usize) {
        (self.bids_desc.len(), self.asks_desc.len())
    }

    fn clear_for_rebuild(&mut self) {
        self.has_snapshot = false;
        self.last_sequence = None;
        self.bids_desc.clear();
        self.asks_desc.clear();
    }
}

fn sort_desc(levels: &mut [PriceLevel]) {
    levels.sort_by(|left, right| compare_price_desc(left.price, right.price));
}

fn apply_levels(book_side: &mut Vec<PriceLevel>, updates: Vec<PriceLevel>) {
    for update in updates {
        match book_side.binary_search_by(|level| compare_price_desc(level.price, update.price)) {
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

fn compare_price_desc(left: f64, right: f64) -> Ordering {
    right.partial_cmp(&left).unwrap_or(Ordering::Equal)
}
