use std::collections::HashMap;

use crate::{
    config::{
        crypto::mask_api_key,
        ids::{AccountId, StrategyId},
        model::{
            AccountRecord, AccountUpsertRequest, AccountValidationStatus, Exchange, SpreadLevel, StrategyLeg,
            StrategyRecord, StrategyUpsertRequest, ValidatedAccountInput, ValidatedStrategy,
        },
    },
    error::{AppError, AppResult},
};

pub fn validate_strategy_request(
    request: &StrategyUpsertRequest,
    accounts: &[AccountRecord],
) -> AppResult<ValidatedStrategy> {
    let id = StrategyId::new(validate_identifier(request.id.as_str(), "strategy id")?);
    let name = validate_name(&request.name, "strategy name")?;
    let accounts_by_id = accounts
        .iter()
        .map(|account| (account.id.clone(), account))
        .collect::<HashMap<_, _>>();
    let long_leg = validate_leg(&request.long_leg, "long_leg", &accounts_by_id)?;
    let short_leg = validate_leg(&request.short_leg, "short_leg", &accounts_by_id)?;

    if long_leg.exchange == short_leg.exchange {
        return Err(AppError::Validation(
            "V1 先按跨交易所套利约束，long_leg.exchange 与 short_leg.exchange 不能相同".to_owned(),
        ));
    }

    let same_leg = long_leg.exchange == short_leg.exchange
        && long_leg.symbol == short_leg.symbol
        && long_leg.account_id == short_leg.account_id;
    if same_leg {
        return Err(AppError::Validation(
            "long_leg 与 short_leg 不能完全相同".to_owned(),
        ));
    }

    if request.enabled {
        ensure_account_enabled(&long_leg.account_id, &accounts_by_id, &id)?;
        ensure_account_enabled(&short_leg.account_id, &accounts_by_id, &id)?;
    }

    let open_levels = validate_levels(&request.open_levels, "open_levels")?;
    let close_levels = validate_levels(&request.close_levels, "close_levels")?;

    if request.max_total_notional <= 0.0 {
        return Err(AppError::Validation(
            "max_total_notional 必须大于 0".to_owned(),
        ));
    }
    if request.max_open_orders == 0 {
        return Err(AppError::Validation(
            "max_open_orders 必须大于 0".to_owned(),
        ));
    }
    if request.stale_order_query_ms == 0 {
        return Err(AppError::Validation(
            "stale_order_query_ms 必须大于 0".to_owned(),
        ));
    }

    Ok(ValidatedStrategy {
        id,
        name,
        enabled: request.enabled,
        long_leg,
        short_leg,
        open_levels,
        close_levels,
        max_total_notional: request.max_total_notional,
        max_open_orders: request.max_open_orders,
        stale_order_query_ms: request.stale_order_query_ms,
    })
}

pub fn validate_existing_strategies(strategies: &[StrategyRecord], accounts: &[AccountRecord]) -> AppResult<()> {
    for strategy in strategies {
        let request = StrategyUpsertRequest {
            id: strategy.id.clone(),
            name: strategy.name.clone(),
            enabled: strategy.enabled,
            long_leg: strategy.long_leg.clone(),
            short_leg: strategy.short_leg.clone(),
            open_levels: strategy.open_levels.clone(),
            close_levels: strategy.close_levels.clone(),
            max_total_notional: strategy.max_total_notional,
            max_open_orders: strategy.max_open_orders,
            stale_order_query_ms: strategy.stale_order_query_ms,
        };

        validate_strategy_request(&request, accounts)?;
    }

    Ok(())
}

pub fn validate_account_request(request: &AccountUpsertRequest) -> AppResult<ValidatedAccountInput> {
    let id = AccountId::new(validate_identifier(request.id.as_str(), "account id")?);
    let name = validate_name(&request.name, "account name")?;
    let api_key = validate_secret_field(&request.api_key, "api_key")?;
    let api_secret = validate_secret_field(&request.api_secret, "api_secret")?;
    let passphrase = request
        .passphrase
        .as_ref()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());

    Ok(ValidatedAccountInput {
        id,
        name,
        exchange: request.exchange,
        enabled: request.enabled,
        api_key: api_key.clone(),
        api_secret,
        passphrase,
        masked_api_key: mask_api_key(&api_key),
        validation_status: AccountValidationStatus::DryRunOk,
    })
}

fn ensure_account_enabled(
    account_id: &AccountId,
    accounts_by_id: &HashMap<AccountId, &AccountRecord>,
    strategy_id: &StrategyId,
) -> AppResult<()> {
    let account = accounts_by_id.get(account_id).ok_or_else(|| {
        AppError::Validation(format!(
            "strategy `{strategy_id}` 引用了不存在的 account_id `{account_id}`"
        ))
    })?;

    if !account.enabled {
        return Err(AppError::Validation(format!(
            "strategy `{strategy_id}` 引用的 account `{account_id}` 当前未启用"
        )));
    }

    Ok(())
}

fn validate_leg(
    leg: &StrategyLeg,
    label: &str,
    accounts_by_id: &HashMap<AccountId, &AccountRecord>,
) -> AppResult<StrategyLeg> {
    let account_id = AccountId::new(validate_identifier(leg.account_id.as_str(), &format!("{label}.account_id"))?);
    let symbol = validate_symbol(&leg.symbol, &format!("{label}.symbol"))?;
    let account = accounts_by_id.get(&account_id).ok_or_else(|| {
        AppError::Validation(format!(
            "{label}.account_id `{account_id}` 不存在，保存策略前请先创建账户"
        ))
    })?;

    if account.exchange != leg.exchange {
        return Err(AppError::Validation(format!(
            "{label}.exchange 与 account `{account_id}` 的 exchange 不匹配"
        )));
    }

    Ok(StrategyLeg {
        exchange: match leg.exchange {
            Exchange::BinanceUsdM => Exchange::BinanceUsdM,
            Exchange::BybitLinear => Exchange::BybitLinear,
        },
        symbol,
        account_id,
    })
}

fn validate_levels(levels: &[SpreadLevel], label: &str) -> AppResult<Vec<SpreadLevel>> {
    if levels.is_empty() {
        return Err(AppError::Validation(format!("{label} 不能为空")));
    }

    let mut previous = None::<f64>;
    let mut normalized = Vec::with_capacity(levels.len());
    for (index, level) in levels.iter().enumerate() {
        if !level.spread_pct.is_finite() || level.spread_pct <= 0.0 {
            return Err(AppError::Validation(format!(
                "{label}[{index}].spread_pct 必须是大于 0 的有限数值"
            )));
        }
        if !level.notional_usd.is_finite() || level.notional_usd <= 0.0 {
            return Err(AppError::Validation(format!(
                "{label}[{index}].notional_usd 必须是大于 0 的有限数值"
            )));
        }
        if let Some(previous_spread) = previous {
            if level.spread_pct <= previous_spread {
                return Err(AppError::Validation(format!(
                    "{label} 必须按 spread_pct 严格递增"
                )));
            }
        }
        previous = Some(level.spread_pct);
        normalized.push(level.clone());
    }

    Ok(normalized)
}

fn validate_identifier(value: &str, field: &str) -> AppResult<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(AppError::Validation(format!("{field} 不能为空")));
    }
    if !trimmed
        .chars()
        .all(|char| char.is_ascii_alphanumeric() || matches!(char, '_' | '-'))
    {
        return Err(AppError::Validation(format!(
            "{field} 只能包含字母、数字、`_`、`-`"
        )));
    }

    Ok(trimmed.to_owned())
}

fn validate_name(value: &str, field: &str) -> AppResult<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(AppError::Validation(format!("{field} 不能为空")));
    }
    Ok(trimmed.to_owned())
}

fn validate_secret_field(value: &str, field: &str) -> AppResult<String> {
    let trimmed = value.trim();
    if trimmed.len() < 4 {
        return Err(AppError::Validation(format!("{field} 至少需要 4 个字符")));
    }
    Ok(trimmed.to_owned())
}

fn validate_symbol(value: &str, field: &str) -> AppResult<String> {
    let normalized = value.trim().to_ascii_uppercase();
    if normalized.is_empty() {
        return Err(AppError::Validation(format!("{field} 不能为空")));
    }
    if !normalized
        .chars()
        .all(|char| char.is_ascii_alphanumeric() || matches!(char, '_' | '-' | '.'))
    {
        return Err(AppError::Validation(format!(
            "{field} 只能包含字母、数字、`_`、`-`、`.`"
        )));
    }
    Ok(normalized)
}
