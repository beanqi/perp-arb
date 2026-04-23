use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use tokio::net::TcpListener;

use crate::{
    admin_api,
    config::{
        crypto::CryptoContext,
        ids::StrategyId,
        model::{
            AccountUpsertRequest, AccountView, ActiveOrderView, BalanceView, PositionView, StrategyRecord,
            StrategyToggleRequest, StrategyUpsertRequest,
        },
        persist::FileStore,
        planner::RuntimePlan,
    },
    engine::shard::{RuntimeManager, RuntimeStatusView},
    error::{AppError, AppResult},
};

#[derive(Clone, Debug)]
pub struct AppSettings {
    pub bind_addr: SocketAddr,
    pub store_path: PathBuf,
    pub master_key_env: String,
}

impl Default for AppSettings {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:3000".parse().expect("static socket address is valid"),
            store_path: PathBuf::from("data/store.json"),
            master_key_env: "PERP_ARB_MASTER_KEY".to_owned(),
        }
    }
}

impl AppSettings {
    pub fn from_env() -> AppResult<Self> {
        let defaults = Self::default();
        let bind_addr = std::env::var("PERP_ARB_BIND_ADDR")
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| defaults.bind_addr.to_string())
            .parse()?;
        let store_path = std::env::var("PERP_ARB_STORE_PATH")
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .unwrap_or(defaults.store_path);
        let master_key_env = std::env::var("PERP_ARB_MASTER_KEY_ENV")
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .unwrap_or(defaults.master_key_env);

        Ok(Self {
            bind_addr,
            store_path,
            master_key_env,
        })
    }
}

#[derive(Debug)]
pub struct AppContext {
    store: FileStore,
    runtime: RuntimeManager,
    update_lock: Mutex<()>,
}

impl AppContext {
    pub fn bootstrap(settings: &AppSettings) -> AppResult<Arc<Self>> {
        let crypto = CryptoContext::from_env(&settings.master_key_env);
        let store = FileStore::open(settings.store_path.clone(), crypto)?;
        if !store.is_crypto_configured() {
            println!(
                "warning: {} is not set; account writes and runtime loads that require secret decryption will fail",
                settings.master_key_env
            );
        }

        let context = Arc::new(Self {
            store,
            runtime: RuntimeManager::new(),
            update_lock: Mutex::new(()),
        });

        let runtime_catalog = context.store.runtime_catalog()?;
        context.runtime.reload(runtime_catalog)?;
        Ok(context)
    }

    pub fn list_strategies(&self) -> AppResult<Vec<StrategyRecord>> {
        self.store.list_strategies()
    }

    pub fn list_accounts(&self) -> AppResult<Vec<AccountView>> {
        self.store.list_accounts()
    }

    pub fn upsert_strategy(&self, request: StrategyUpsertRequest) -> AppResult<StrategyRecord> {
        let _guard = self
            .update_lock
            .lock()
            .map_err(|_| AppError::lock("app update"))?;
        let update = self.store.upsert_strategy(request)?;
        let status = self.runtime.reload(update.runtime_catalog)?;
        println!("{}", status.rendered_plan);
        Ok(update.entity)
    }

    pub fn set_strategy_enabled(
        &self,
        strategy_id: StrategyId,
        request: StrategyToggleRequest,
    ) -> AppResult<StrategyRecord> {
        let _guard = self
            .update_lock
            .lock()
            .map_err(|_| AppError::lock("app update"))?;
        let update = self.store.set_strategy_enabled(&strategy_id, request.enabled)?;
        let status = self.runtime.reload(update.runtime_catalog)?;
        println!("{}", status.rendered_plan);
        Ok(update.entity)
    }

    pub fn upsert_account(&self, request: AccountUpsertRequest) -> AppResult<AccountView> {
        let _guard = self
            .update_lock
            .lock()
            .map_err(|_| AppError::lock("app update"))?;
        let update = self.store.upsert_account(request)?;
        let status = self.runtime.reload(update.runtime_catalog)?;
        println!("{}", status.rendered_plan);
        Ok(update.entity)
    }

    pub fn runtime_status(&self) -> AppResult<RuntimeStatusView> {
        self.runtime.status()
    }

    pub fn runtime_plan(&self) -> AppResult<RuntimePlan> {
        Ok(self.runtime_status()?.plan)
    }

    pub fn positions(&self) -> AppResult<Vec<PositionView>> {
        self.runtime.positions()
    }

    pub fn balances(&self) -> AppResult<Vec<BalanceView>> {
        self.runtime.balances()
    }

    pub fn active_orders(&self) -> AppResult<Vec<ActiveOrderView>> {
        self.runtime.active_orders()
    }
}

pub async fn run(settings: AppSettings) -> AppResult<()> {
    let context = AppContext::bootstrap(&settings)?;
    println!("{}", context.runtime_status()?.rendered_plan);
    let router = admin_api::router(context);
    let listener = TcpListener::bind(settings.bind_addr).await.map_err(|error| {
        AppError::Internal(format!("failed to bind {}: {error}", settings.bind_addr))
    })?;
    println!("admin api listening on http://{}", settings.bind_addr);

    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|error| AppError::Internal(format!("server exited with error: {error}")))
}

pub fn print_plan(settings: AppSettings) -> AppResult<()> {
    let context = AppContext::bootstrap(&settings)?;
    println!("{}", context.runtime_status()?.rendered_plan);
    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
