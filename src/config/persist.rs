use std::{
    collections::BTreeSet,
    fs,
    path::PathBuf,
    sync::{Mutex, MutexGuard},
};

use crate::{
    config::{
        crypto::CryptoContext,
        ids::StrategyId,
        model::{
            AccountCredentials, AccountRecord, AccountUpsertRequest, AccountView, EncryptedAccountSecrets,
            PersistedStore, RuntimeAccount, RuntimeCatalog, StrategyRecord, StrategyUpsertRequest, now_ms,
        },
        planner::{RuntimePlan, build_runtime_plan},
        validate::{validate_account_request, validate_existing_strategies, validate_strategy_request},
    },
    error::{AppError, AppResult},
};

#[derive(Debug)]
pub struct StoreUpdate<T> {
    pub entity: T,
    pub runtime_catalog: RuntimeCatalog,
}

#[derive(Debug)]
pub struct FileStore {
    path: PathBuf,
    crypto: CryptoContext,
    inner: Mutex<PersistedStore>,
}

impl FileStore {
    pub fn open(path: PathBuf, crypto: CryptoContext) -> AppResult<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let store = if path.exists() {
            let raw = fs::read_to_string(&path)?;
            if raw.trim().is_empty() {
                PersistedStore::default()
            } else {
                serde_json::from_str(&raw)?
            }
        } else {
            PersistedStore::default()
        };

        Ok(Self {
            path,
            crypto,
            inner: Mutex::new(store),
        })
    }

    pub fn is_crypto_configured(&self) -> bool {
        self.crypto.is_configured()
    }

    pub fn list_strategies(&self) -> AppResult<Vec<StrategyRecord>> {
        let mut strategies = self.lock_store()?.strategies.clone();
        strategies.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(strategies)
    }

    pub fn list_accounts(&self) -> AppResult<Vec<AccountView>> {
        let mut accounts = self
            .lock_store()?
            .accounts
            .iter()
            .map(AccountView::from_record)
            .collect::<Vec<_>>();
        accounts.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(accounts)
    }

    pub fn upsert_strategy(&self, request: StrategyUpsertRequest) -> AppResult<StoreUpdate<StrategyRecord>> {
        let mut store = self.lock_store()?;
        let mut next = store.clone();
        let validated = validate_strategy_request(&request, &next.accounts)?;
        let now = now_ms();

        let strategy = if let Some(existing) = next.strategies.iter_mut().find(|item| item.id == validated.id) {
            let created_at_ms = existing.created_at_ms;
            *existing = validated.into_record(created_at_ms, now);
            existing.clone()
        } else {
            let record = validated.into_record(now, now);
            next.strategies.push(record.clone());
            record
        };

        next.strategies.sort_by(|left, right| left.id.cmp(&right.id));
        next.revision += 1;
        let runtime_catalog = self.runtime_catalog_from_store(&next)?;
        self.persist_store(&next)?;
        *store = next;

        Ok(StoreUpdate {
            entity: strategy,
            runtime_catalog,
        })
    }

    pub fn set_strategy_enabled(
        &self,
        strategy_id: &StrategyId,
        enabled: bool,
    ) -> AppResult<StoreUpdate<StrategyRecord>> {
        let mut store = self.lock_store()?;
        let mut next = store.clone();
        let index = next
            .strategies
            .iter()
            .position(|item| &item.id == strategy_id)
            .ok_or_else(|| AppError::NotFound(format!("strategy `{strategy_id}` 不存在")))?;
        let current = next.strategies[index].clone();
        let request = StrategyUpsertRequest {
            id: current.id.clone(),
            name: current.name.clone(),
            enabled,
            long_leg: current.long_leg.clone(),
            short_leg: current.short_leg.clone(),
            open_levels: current.open_levels.clone(),
            close_levels: current.close_levels.clone(),
            max_total_notional: current.max_total_notional,
            max_open_orders: current.max_open_orders,
            stale_order_query_ms: current.stale_order_query_ms,
        };
        let validated = validate_strategy_request(&request, &next.accounts)?;
        let updated = validated.into_record(current.created_at_ms, now_ms());
        next.strategies[index] = updated.clone();
        next.revision += 1;
        let runtime_catalog = self.runtime_catalog_from_store(&next)?;
        self.persist_store(&next)?;
        *store = next;

        Ok(StoreUpdate {
            entity: updated,
            runtime_catalog,
        })
    }

    pub fn upsert_account(&self, request: AccountUpsertRequest) -> AppResult<StoreUpdate<AccountView>> {
        let mut store = self.lock_store()?;
        let mut next = store.clone();
        let validated = validate_account_request(&request)?;
        let now = now_ms();
        let credentials = EncryptedAccountSecrets {
            api_key: self.crypto.encrypt_string(&validated.api_key)?,
            api_secret: self.crypto.encrypt_string(&validated.api_secret)?,
            passphrase: validated
                .passphrase
                .as_ref()
                .map(|value| self.crypto.encrypt_string(value))
                .transpose()?,
        };

        let new_record = if let Some(existing) = next.accounts.iter().find(|item| item.id == validated.id) {
            AccountRecord::from_validated(validated, credentials, existing.created_at_ms, now)
        } else {
            AccountRecord::from_validated(validated, credentials, now, now)
        };

        if let Some(existing) = next.accounts.iter_mut().find(|item| item.id == new_record.id) {
            *existing = new_record.clone();
        } else {
            next.accounts.push(new_record.clone());
        }

        validate_existing_strategies(&next.strategies, &next.accounts)?;
        next.accounts.sort_by(|left, right| left.id.cmp(&right.id));
        next.revision += 1;
        let runtime_catalog = self.runtime_catalog_from_store(&next)?;
        self.persist_store(&next)?;
        *store = next;

        Ok(StoreUpdate {
            entity: AccountView::from_record(&new_record),
            runtime_catalog,
        })
    }

    pub fn runtime_catalog(&self) -> AppResult<RuntimeCatalog> {
        let store = self.lock_store()?;
        self.runtime_catalog_from_store(&store)
    }

    pub fn runtime_plan_without_credentials(&self) -> AppResult<RuntimePlan> {
        let store = self.lock_store()?;
        let enabled_strategies = store
            .strategies
            .iter()
            .filter(|strategy| strategy.enabled)
            .cloned()
            .collect::<Vec<_>>();
        Ok(build_runtime_plan(&enabled_strategies))
    }

    fn runtime_catalog_from_store(&self, store: &PersistedStore) -> AppResult<RuntimeCatalog> {
        let enabled_strategies = store
            .strategies
            .iter()
            .filter(|strategy| strategy.enabled)
            .cloned()
            .collect::<Vec<_>>();
        let referenced_account_ids = enabled_strategies
            .iter()
            .flat_map(|strategy| {
                [
                    strategy.long_leg.account_id.clone(),
                    strategy.short_leg.account_id.clone(),
                ]
            })
            .collect::<BTreeSet<_>>();
        let referenced_accounts = store
            .accounts
            .iter()
            .filter(|account| referenced_account_ids.contains(&account.id))
            .map(|account| {
                Ok(RuntimeAccount {
                    id: account.id.clone(),
                    name: account.name.clone(),
                    exchange: account.exchange,
                    credentials: AccountCredentials {
                        api_key: self.crypto.decrypt_string(&account.credentials.api_key)?,
                        api_secret: self.crypto.decrypt_string(&account.credentials.api_secret)?,
                        passphrase: account
                            .credentials
                            .passphrase
                            .as_ref()
                            .map(|value| self.crypto.decrypt_string(value))
                            .transpose()?,
                    },
                })
            })
            .collect::<AppResult<Vec<_>>>()?;

        Ok(RuntimeCatalog {
            store_revision: store.revision,
            enabled_strategies,
            referenced_accounts,
        })
    }

    fn persist_store(&self, store: &PersistedStore) -> AppResult<()> {
        let raw = serde_json::to_string_pretty(store)?;
        let temp_path = self.path.with_extension("tmp");
        fs::write(&temp_path, raw)?;
        fs::rename(temp_path, &self.path)?;
        Ok(())
    }

    fn lock_store(&self) -> AppResult<MutexGuard<'_, PersistedStore>> {
        self.inner.lock().map_err(|_| AppError::lock("config store"))
    }
}
