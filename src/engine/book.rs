use crate::config::model::MarketKey;

#[derive(Clone, Debug, Default)]
pub struct LocalBookState {
    pub market: Option<MarketKey>,
    pub has_snapshot: bool,
    pub last_sequence: Option<u64>,
}
