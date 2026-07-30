//! Named [`SchedulerStore`] registration at host boot.
//!
//! Register one or more backends under logical names before constructing
//! `Chronon` in `chronon-runtime`. Use [`StoreRouter::register_global`] with
//! [`DEFAULT_STORE_NAME`] for single-store setups.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use parking_lot::RwLock;

use crate::error::{ChrononError, Result};
use crate::store::SchedulerStore;

/// Default logical store name when hosts register a single backend.
pub const DEFAULT_STORE_NAME: &str = "default";

static GLOBAL_ROUTER: OnceLock<RwLock<StoreRouter>> = OnceLock::new();

fn global_router() -> &'static RwLock<StoreRouter> {
    GLOBAL_ROUTER.get_or_init(|| RwLock::new(StoreRouter::new()))
}

/// Registers named [`SchedulerStore`] backends at host boot.
///
/// Use for multi-store hosts or a single default via [`DEFAULT_STORE_NAME`]. Typical embedded
/// flow:
///
/// 1. [`Self::register_global`] (or `install_default_mem_store` from the mem backend).
/// 2. `ChrononBuilder::scheduler_store_from_global()`.
///
/// Prefer passing a store directly to `ChrononBuilder::scheduler_store` in coordinator–worker
/// or remote-HTTP setups when each binary already shares connection URLs — the global router
/// is optional convenience for single-process boots. Multi-process and remote setups still
/// require a shared durable database.
///
/// Thread-safe when accessed through [`Self::register_global`] / [`default_store_from_global`];
/// direct mutation requires exclusive access to the router instance.
///
/// # Examples
///
/// ```
/// # use std::sync::Arc;
/// # use chronon_core::{SchedulerStore, StoreRouter, DEFAULT_STORE_NAME};
/// # fn demo(store: Arc<dyn SchedulerStore>) {
/// StoreRouter::register_global(DEFAULT_STORE_NAME, store);
/// # }
/// ```
///
/// Runnable end-to-end: `cargo run -p uf-chronon --example store_router_boot --features mem`.
#[derive(Default)]
pub struct StoreRouter {
    stores: HashMap<String, Arc<dyn SchedulerStore>>,
}

impl StoreRouter {
    /// Create an empty router (no stores registered).
    pub fn new() -> Self {
        Self {
            stores: HashMap::new(),
        }
    }

    /// Register a store under a logical name (overwrites any previous entry).
    pub fn register(&mut self, name: impl Into<String>, store: Arc<dyn SchedulerStore>) {
        self.stores.insert(name.into(), store);
    }

    /// Resolve a registered store by name.
    pub fn get(&self, name: &str) -> Option<Arc<dyn SchedulerStore>> {
        self.stores.get(name).cloned()
    }

    /// Replace the process-global router (typically once at startup).
    ///
    /// Subsequent calls are ignored; the first successful install wins.
    pub fn install_global(router: Self) {
        let _ = GLOBAL_ROUTER.set(RwLock::new(router));
    }

    /// Register a store on the process-global router.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use chronon_core::{SchedulerStore, StoreRouter, DEFAULT_STORE_NAME};
    /// # fn demo(store: Arc<dyn SchedulerStore>) {
    /// StoreRouter::register_global(DEFAULT_STORE_NAME, store);
    /// # }
    /// ```
    pub fn register_global(name: impl Into<String>, store: Arc<dyn SchedulerStore>) {
        global_router().write().register(name, store);
    }
}

/// Resolves the default store from the global router.
///
/// Returns [`ChrononError::StorageError`] when no store is registered under
/// [`DEFAULT_STORE_NAME`].
pub fn default_store_from_global() -> Result<Arc<dyn SchedulerStore>> {
    global_router()
        .read()
        .get(DEFAULT_STORE_NAME)
        .ok_or_else(|| ChrononError::storage("no default SchedulerStore registered"))
}
