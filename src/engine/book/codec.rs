use std::{fmt, time::Instant};

use serde::{
    Deserialize, Deserializer,
    de::{self, Visitor},
};

use crate::config::model::{Exchange, MarketKey};

use super::{DecodedDepthMessage, DepthDecodeTiming, PriceLevel, RawDepthMessage};

pub fn decode_raw_depth(exchange: Exchange, payload: &[u8]) -> Option<RawDepthMessage> {
    decode_raw_depth_with_timing(exchange, payload).map(|decoded| decoded.message)
}

pub fn decode_raw_depth_with_timing(
    exchange: Exchange,
    payload: &[u8],
) -> Option<DecodedDepthMessage> {
    match exchange {
        Exchange::BinanceUsdM => decode_binance_depth(payload),
        Exchange::BybitLinear => decode_bybit_depth(payload),
    }
}

fn decode_binance_depth(payload: &[u8]) -> Option<DecodedDepthMessage> {
    let deserialize_started_at = Instant::now();
    let envelope = sonic_rs::from_slice::<BinanceDepthEnvelope<'_>>(payload).ok()?;
    let deserialize_us = deserialize_started_at.elapsed().as_micros();
    let normalize_started_at = Instant::now();
    let data = match envelope {
        BinanceDepthEnvelope::Combined { data } | BinanceDepthEnvelope::Direct(data) => data,
    };
    let market = MarketKey::new(Exchange::BinanceUsdM, data.symbol);

    let message = if data.event == Some(BinanceDepthEvent::DepthUpdate) {
        RawDepthMessage::Delta {
            market,
            first_sequence: data.first_sequence,
            previous_sequence: data.previous_sequence,
            sequence: data.sequence,
            bids: parse_levels(data.delta_bids?),
            asks: parse_levels(data.delta_asks?),
        }
    } else {
        RawDepthMessage::Snapshot {
            market,
            sequence: data.last_update_id,
            bids: parse_levels(data.snapshot_bids?),
            asks: parse_levels(data.snapshot_asks?),
        }
    };

    Some(DecodedDepthMessage {
        message,
        timing: DepthDecodeTiming {
            deserialize_us,
            normalize_us: normalize_started_at.elapsed().as_micros(),
        },
    })
}

fn decode_bybit_depth(payload: &[u8]) -> Option<DecodedDepthMessage> {
    let deserialize_started_at = Instant::now();
    let envelope = sonic_rs::from_slice::<BybitDepthEnvelope<'_>>(payload).ok()?;
    let deserialize_us = deserialize_started_at.elapsed().as_micros();
    let normalize_started_at = Instant::now();
    let symbol = envelope
        .data
        .symbol
        .or_else(|| symbol_from_topic(envelope.topic))?;
    let market = MarketKey::new(Exchange::BybitLinear, symbol);
    let sequence = envelope.data.sequence.or(envelope.data.seq);
    let bids = parse_levels(envelope.data.bids?);
    let asks = parse_levels(envelope.data.asks?);

    let message = if envelope.message_type == Some(BybitMessageType::Delta) {
        RawDepthMessage::Delta {
            market,
            first_sequence: sequence,
            previous_sequence: None,
            sequence,
            bids,
            asks,
        }
    } else {
        RawDepthMessage::Snapshot {
            market,
            sequence,
            bids,
            asks,
        }
    };

    Some(DecodedDepthMessage {
        message,
        timing: DepthDecodeTiming {
            deserialize_us,
            normalize_us: normalize_started_at.elapsed().as_micros(),
        },
    })
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum BinanceDepthEnvelope<'a> {
    Combined {
        #[serde(borrow)]
        data: BinanceDepthData<'a>,
    },
    Direct(#[serde(borrow)] BinanceDepthData<'a>),
}

#[derive(Debug, Deserialize)]
struct BinanceDepthData<'a> {
    #[serde(rename = "e")]
    event: Option<BinanceDepthEvent>,
    #[serde(rename = "s")]
    symbol: &'a str,
    #[serde(rename = "U")]
    first_sequence: Option<u64>,
    #[serde(rename = "pu")]
    previous_sequence: Option<u64>,
    #[serde(rename = "u")]
    sequence: Option<u64>,
    #[serde(rename = "b")]
    delta_bids: Option<Vec<[JsonF64; 2]>>,
    #[serde(rename = "a")]
    delta_asks: Option<Vec<[JsonF64; 2]>>,
    #[serde(rename = "lastUpdateId")]
    last_update_id: Option<u64>,
    #[serde(rename = "bids")]
    snapshot_bids: Option<Vec<[JsonF64; 2]>>,
    #[serde(rename = "asks")]
    snapshot_asks: Option<Vec<[JsonF64; 2]>>,
}

#[derive(Debug, Deserialize)]
struct BybitDepthEnvelope<'a> {
    #[serde(rename = "type")]
    message_type: Option<BybitMessageType>,
    topic: Option<&'a str>,
    #[serde(borrow)]
    data: BybitDepthData<'a>,
}

#[derive(Debug, Deserialize)]
struct BybitDepthData<'a> {
    #[serde(rename = "s")]
    symbol: Option<&'a str>,
    #[serde(rename = "u")]
    sequence: Option<u64>,
    seq: Option<u64>,
    #[serde(rename = "b")]
    bids: Option<Vec<[JsonF64; 2]>>,
    #[serde(rename = "a")]
    asks: Option<Vec<[JsonF64; 2]>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BinanceDepthEvent {
    DepthUpdate,
    Other,
}

impl<'de> Deserialize<'de> for BinanceDepthEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(BinanceDepthEventVisitor)
    }
}

struct BinanceDepthEventVisitor;

impl Visitor<'_> for BinanceDepthEventVisitor {
    type Value = BinanceDepthEvent;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a Binance event type")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(if value == "depthUpdate" {
            BinanceDepthEvent::DepthUpdate
        } else {
            BinanceDepthEvent::Other
        })
    }

    fn visit_borrowed_str<E>(self, value: &'_ str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_str(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BybitMessageType {
    Delta,
    Other,
}

impl<'de> Deserialize<'de> for BybitMessageType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(BybitMessageTypeVisitor)
    }
}

struct BybitMessageTypeVisitor;

impl Visitor<'_> for BybitMessageTypeVisitor {
    type Value = BybitMessageType;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a Bybit message type")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(if value == "delta" {
            BybitMessageType::Delta
        } else {
            BybitMessageType::Other
        })
    }

    fn visit_borrowed_str<E>(self, value: &'_ str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_str(value)
    }
}

#[derive(Clone, Copy, Debug)]
struct JsonF64(f64);

impl<'de> Deserialize<'de> for JsonF64 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(JsonF64Visitor)
    }
}

struct JsonF64Visitor;

impl Visitor<'_> for JsonF64Visitor {
    type Value = JsonF64;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON number or numeric string")
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E> {
        Ok(JsonF64(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(JsonF64(value as f64))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(JsonF64(value as f64))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        value.parse::<f64>().map(JsonF64).map_err(E::custom)
    }

    fn visit_borrowed_str<E>(self, value: &'_ str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_str(value)
    }
}

fn parse_levels(levels: Vec<[JsonF64; 2]>) -> Vec<PriceLevel> {
    levels
        .into_iter()
        .map(|[price, qty]| PriceLevel {
            price: price.0,
            qty: qty.0,
        })
        .collect()
}

fn symbol_from_topic(topic: Option<&str>) -> Option<&str> {
    topic?.rsplit('.').next()
}
