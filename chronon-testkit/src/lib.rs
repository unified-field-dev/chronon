//! Matrix bootstrap and declarative scenarios for Chronon e2e and bench drivers.
//!
//! Test-only layer for exercising scheduler correctness across storage, deployment, topology,
//! and telemetry matrix slices. Not imported by production crates.
//!
//! # Entry points
//!
//! - [`MatrixSpec`] — matrix dimensions (`ci_mem_embedded`, etc.)
//! - [`BootstrapSession`] — install store (mem, sqlite, postgres, postgres-redis) and spawn runtimes
//! - [`ScenarioSpec`] / [`ScenarioRunner`] — declarative steps shared by e2e and bench
//! - [`seed_due_cron_jobs`], [`NOOP_SCRIPT`], [`COUNTING_SCRIPT`], [`SLEEP_SCRIPT`] — seed jobs and probe scripts
//!
//! PR CI runs **mem + sqlite** matrix rows; postgres and postgres-redis run on tag CI
//! ([`ci.yml`](../.github/workflows/ci.yml) `e2e-durable` job). Built-in probe scripts
//! ([`NOOP_SCRIPT`], [`COUNTING_SCRIPT`], [`SLEEP_SCRIPT`]) register on every Chronon build. Partition count
//! is fixed to four via `CHRONON_NUM_PARTITIONS` during bootstrap.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod bootstrap;
mod catalog;
mod distributed;
mod fixtures;
mod macros;
mod matrix;
mod runner;
mod runner_steps;
mod runner_types;
mod scenario;
mod shared_store;
mod store_contract;

pub use bootstrap::{BootstrapSession, EmbeddedHandle, SplitHandle};
pub use catalog::{
    coordinator_catalog, e2e_storage_backends, embedded_catalog, mem_coordinator_catalog,
    mem_embedded_catalog, run_catalog_entry, run_catalog_entry_by_id, CatalogDeployment,
    CatalogEntry, PathKind,
};
pub use distributed::{
    coordinator_failover_postgres_redis_smoke, coordinator_leader_exclusive_smoke,
    distributed_store_available, dual_worker_claim_exclusive_smoke,
    dual_worker_wrong_pool_idle_smoke, postgres_redis_hybrid_claim_roundtrip_smoke,
};
pub use fixtures::{
    counting_probe_total, parse_sleep_ms, reset_counting_probe, seed_due_cron_jobs,
    smoke_actor_json, upsert_future_cron_job, upsert_immediate_cron_job,
    upsert_immediate_run_once_job, upsert_manual_job, wait_for_run_terminal, COUNTING_SCRIPT,
    FAIL_SCRIPT, MAX_SLEEP_MS, NOOP_SCRIPT, SLEEP_SCRIPT,
};
pub use matrix::{DeploymentKind, MatrixSpec, StorageAdapter, TelemetryAdapter, Topology};
pub use runner::{ScenarioResult, ScenarioRunner};
pub use runner_types::{RunMode, StepTiming};
pub use scenario::{ScenarioSpec, ScenarioStep, ScriptProbeKind};
pub use shared_store::{extended_store_available, extended_store_skip_reason};
pub use store_contract::run_store_contract;

pub use paste;

#[cfg(test)]
mod shared_store_tests {
    use super::{extended_store_available, extended_store_skip_reason, StorageAdapter};

    #[test]
    fn mem_store_always_available() {
        assert!(extended_store_available(StorageAdapter::Mem));
        assert!(extended_store_skip_reason(StorageAdapter::Mem).is_none());
    }

    #[test]
    fn sqlite_always_available() {
        assert!(extended_store_available(StorageAdapter::Sqlite));
        assert!(extended_store_skip_reason(StorageAdapter::Sqlite).is_none());
    }

    #[test]
    fn postgres_requires_env() {
        if std::env::var("CHRONON_POSTGRES_URL").is_ok() {
            assert!(extended_store_available(StorageAdapter::Postgres));
        } else {
            assert!(!extended_store_available(StorageAdapter::Postgres));
            assert!(extended_store_skip_reason(StorageAdapter::Postgres).is_some());
        }
    }
}
