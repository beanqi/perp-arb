use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, State},
    response::Html,
    routing::{get, post},
};
use serde::Serialize;

use crate::{
    app::AppContext,
    config::{
        ids::StrategyId,
        model::{AccountUpsertRequest, StrategyToggleRequest, StrategyUpsertRequest},
    },
    error::AppResult,
    web_ui,
};

pub fn router(state: Arc<AppContext>) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/api/health", get(health))
        .route(
            "/api/strategies",
            get(list_strategies).post(upsert_strategy),
        )
        .route(
            "/api/strategies/{strategy_id}/enabled",
            post(set_strategy_enabled),
        )
        .route("/api/accounts", get(list_accounts).post(upsert_account))
        .route("/api/runtime/status", get(runtime_status))
        .route("/api/runtime/plan", get(runtime_plan))
        .route("/api/runtime/strategies", get(strategy_runtime))
        .route("/api/positions", get(positions))
        .route("/api/balances", get(balances))
        .route("/api/orders", get(active_orders))
        .with_state(state)
}

async fn index() -> Html<&'static str> {
    Html(web_ui::index_html())
}

#[derive(Serialize)]
struct HealthResponse {
    ok: bool,
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { ok: true })
}

async fn list_strategies(State(state): State<Arc<AppContext>>) -> AppResult<Json<Vec<crate::config::model::StrategyRecord>>> {
    Ok(Json(state.list_strategies()?))
}

async fn upsert_strategy(
    State(state): State<Arc<AppContext>>,
    Json(request): Json<StrategyUpsertRequest>,
) -> AppResult<Json<crate::config::model::StrategyRecord>> {
    Ok(Json(state.upsert_strategy(request)?))
}

async fn set_strategy_enabled(
    State(state): State<Arc<AppContext>>,
    Path(strategy_id): Path<String>,
    Json(request): Json<StrategyToggleRequest>,
) -> AppResult<Json<crate::config::model::StrategyRecord>> {
    Ok(Json(
        state.set_strategy_enabled(StrategyId::new(strategy_id), request)?,
    ))
}

async fn list_accounts(State(state): State<Arc<AppContext>>) -> AppResult<Json<Vec<crate::config::model::AccountView>>> {
    Ok(Json(state.list_accounts()?))
}

async fn upsert_account(
    State(state): State<Arc<AppContext>>,
    Json(request): Json<AccountUpsertRequest>,
) -> AppResult<Json<crate::config::model::AccountView>> {
    Ok(Json(state.upsert_account(request)?))
}

async fn runtime_status(State(state): State<Arc<AppContext>>) -> AppResult<Json<crate::engine::shard::RuntimeStatusView>> {
    Ok(Json(state.runtime_status()?))
}

async fn runtime_plan(State(state): State<Arc<AppContext>>) -> AppResult<Json<crate::config::planner::RuntimePlan>> {
    Ok(Json(state.runtime_plan()?))
}

async fn strategy_runtime(
    State(state): State<Arc<AppContext>>,
) -> Json<crate::engine::telemetry::StrategyRuntimeSnapshot> {
    Json(state.strategy_runtime())
}

async fn positions(State(state): State<Arc<AppContext>>) -> AppResult<Json<Vec<crate::config::model::PositionView>>> {
    Ok(Json(state.positions()?))
}

async fn balances(State(state): State<Arc<AppContext>>) -> AppResult<Json<Vec<crate::config::model::BalanceView>>> {
    Ok(Json(state.balances()?))
}

async fn active_orders(State(state): State<Arc<AppContext>>) -> AppResult<Json<Vec<crate::config::model::ActiveOrderView>>> {
    Ok(Json(state.active_orders()?))
}
