use serde_json::Value;

use crate::config::model::{Exchange, MarketKey};

use super::{PriceLevel, RawDepthMessage};

pub fn decode_raw_depth(exchange: Exchange, payload: &[u8]) -> Option<RawDepthMessage> {
    let value = serde_json::from_slice::<Value>(payload).ok()?;
    match exchange {
        Exchange::BinanceUsdM => decode_binance_depth(&value),
        Exchange::BybitLinear => decode_bybit_depth(&value),
    }
}

fn decode_binance_depth(value: &Value) -> Option<RawDepthMessage> {
    let data = value.get("data").unwrap_or(value);
    let symbol = data.get("s")?.as_str()?.to_owned();
    let market = MarketKey::new(Exchange::BinanceUsdM, symbol);

    if data.get("e").and_then(Value::as_str) == Some("depthUpdate") {
        return Some(RawDepthMessage::Delta {
            market,
            first_sequence: data.get("U").and_then(Value::as_u64),
            previous_sequence: data.get("pu").and_then(Value::as_u64),
            sequence: data.get("u").and_then(Value::as_u64),
            bids: parse_levels(data.get("b")?),
            asks: parse_levels(data.get("a")?),
        });
    }

    Some(RawDepthMessage::Snapshot {
        market,
        sequence: data.get("lastUpdateId").and_then(Value::as_u64),
        bids: parse_levels(data.get("bids")?),
        asks: parse_levels(data.get("asks")?),
    })
}

fn decode_bybit_depth(value: &Value) -> Option<RawDepthMessage> {
    let data = value.get("data")?;
    let symbol = data
        .get("s")
        .and_then(Value::as_str)
        .or_else(|| symbol_from_topic(value.get("topic").and_then(Value::as_str)))?
        .to_owned();
    let market = MarketKey::new(Exchange::BybitLinear, symbol);
    let sequence = data
        .get("u")
        .and_then(Value::as_u64)
        .or_else(|| data.get("seq").and_then(Value::as_u64));
    let message_type = value.get("type").and_then(Value::as_str);
    let bids = parse_levels(data.get("b")?);
    let asks = parse_levels(data.get("a")?);

    if message_type == Some("delta") {
        Some(RawDepthMessage::Delta {
            market,
            first_sequence: sequence,
            previous_sequence: None,
            sequence,
            bids,
            asks,
        })
    } else {
        Some(RawDepthMessage::Snapshot {
            market,
            sequence,
            bids,
            asks,
        })
    }
}

fn symbol_from_topic(topic: Option<&str>) -> Option<&str> {
    topic?.rsplit('.').next()
}

fn parse_levels(value: &Value) -> Vec<PriceLevel> {
    value
        .as_array()
        .into_iter()
        .flat_map(|levels| levels.iter())
        .filter_map(parse_level)
        .collect()
}

fn parse_level(value: &Value) -> Option<PriceLevel> {
    let values = value.as_array()?;
    Some(PriceLevel {
        price: parse_number(values.first()?)?,
        qty: parse_number(values.get(1)?)?,
    })
}

fn parse_number(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|text| text.parse::<f64>().ok()))
}
