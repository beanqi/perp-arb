use std::fmt;

use crate::{
    engine::types::{Side, TradeCommand},
    market_rules::MarketRule,
};

const ROUNDING_EPS: f64 = 1e-12;

#[derive(Clone, Debug)]
pub struct TradeExecRuntime {
    pub command_queue_bound: usize,
}

impl Default for TradeExecRuntime {
    fn default() -> Self {
        Self {
            command_queue_bound: 1024,
        }
    }
}

impl TradeExecRuntime {
    pub fn accepts(&self, _: &TradeCommand) -> bool {
        true
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PreparedPlaceOrder {
    pub qty: f64,
    pub limit_price: Option<f64>,
    pub notional_usd: Option<f64>,
}

#[derive(Clone, Copy, Debug)]
pub enum PlaceOrderReject {
    InvalidQty,
    InvalidLimitPrice,
    BelowMinQty {
        qty: f64,
        min_qty: f64,
    },
    BelowMinNotional {
        notional_usd: f64,
        min_notional: f64,
    },
}

impl fmt::Display for PlaceOrderReject {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidQty => formatter.write_str("invalid qty"),
            Self::InvalidLimitPrice => formatter.write_str("invalid limit price"),
            Self::BelowMinQty { qty, min_qty } => {
                write!(formatter, "qty {qty} below min_qty {min_qty}")
            }
            Self::BelowMinNotional {
                notional_usd,
                min_notional,
            } => write!(
                formatter,
                "notional {notional_usd} below min_notional {min_notional}"
            ),
        }
    }
}

pub fn prepare_place_order(
    qty: f64,
    limit_price: Option<f64>,
    side: &Side,
    rule: &MarketRule,
) -> Result<PreparedPlaceOrder, PlaceOrderReject> {
    let qty = align_qty(qty, rule)?;
    if let Some(min_qty) = rule.min_qty
        && qty < min_qty
    {
        return Err(PlaceOrderReject::BelowMinQty { qty, min_qty });
    }

    let limit_price = limit_price
        .map(|price| align_limit_price(price, side, rule))
        .transpose()?;
    let notional_usd = limit_price.map(|price| qty * price);
    if let (Some(notional_usd), Some(min_notional)) = (notional_usd, rule.min_notional)
        && notional_usd < min_notional
    {
        return Err(PlaceOrderReject::BelowMinNotional {
            notional_usd,
            min_notional,
        });
    }

    Ok(PreparedPlaceOrder {
        qty,
        limit_price,
        notional_usd,
    })
}

fn align_qty(qty: f64, rule: &MarketRule) -> Result<f64, PlaceOrderReject> {
    if !qty.is_finite() || qty <= 0.0 {
        return Err(PlaceOrderReject::InvalidQty);
    }

    let mut aligned = if let Some(step) = rule.qty_step.filter(|step| *step > 0.0) {
        floor_to_step(qty, step)
    } else {
        qty
    };
    if let Some(precision) = rule.qty_precision {
        aligned = floor_to_precision(aligned, precision);
    }

    if aligned.is_finite() && aligned > 0.0 {
        Ok(aligned)
    } else {
        Err(PlaceOrderReject::InvalidQty)
    }
}

fn align_limit_price(price: f64, side: &Side, rule: &MarketRule) -> Result<f64, PlaceOrderReject> {
    if !price.is_finite() || price <= 0.0 {
        return Err(PlaceOrderReject::InvalidLimitPrice);
    }

    let align_up = matches!(side, Side::Sell);
    let mut aligned = if let Some(tick) = rule.price_tick.filter(|tick| *tick > 0.0) {
        if align_up {
            ceil_to_step(price, tick)
        } else {
            floor_to_step(price, tick)
        }
    } else {
        price
    };
    if let Some(precision) = rule.price_precision {
        aligned = if align_up {
            ceil_to_precision(aligned, precision)
        } else {
            floor_to_precision(aligned, precision)
        };
    }

    if aligned.is_finite() && aligned > 0.0 {
        Ok(aligned)
    } else {
        Err(PlaceOrderReject::InvalidLimitPrice)
    }
}

fn floor_to_step(value: f64, step: f64) -> f64 {
    ((value / step) + ROUNDING_EPS).floor() * step
}

fn ceil_to_step(value: f64, step: f64) -> f64 {
    ((value / step) - ROUNDING_EPS).ceil() * step
}

fn floor_to_precision(value: f64, precision: u32) -> f64 {
    let scale = 10_f64.powi(precision as i32);
    (value * scale + ROUNDING_EPS).floor() / scale
}

fn ceil_to_precision(value: f64, precision: u32) -> f64 {
    let scale = 10_f64.powi(precision as i32);
    (value * scale - ROUNDING_EPS).ceil() / scale
}
