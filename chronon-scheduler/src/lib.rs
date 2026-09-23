//! Coordinator tick loop: due-job discovery, claims, and run enqueue.
//!
//! Parses cron expressions, discovers due jobs, claims partition slices, and enqueues runs.
//! Workers in `chronon-runtime` claim and execute runs separately from the tick loop.
//!
//! # Documentation map
//!
//! - **Fluent job construction (preferred)** — [`JobBuilder`]
//! - **Embedded / coordinator ticks** — [`Scheduler`], [`run_coordinator_tick_loop`]
//! - **Cron parsing** — [`CronExpr`]
//! - **Horizontal scale-out** — [`PartitionAssigner`], [`try_acquire_leader`], [`LeaderElector`]
//! - **Env tuning** — `num_partitions_from_env`, `tick_interval_ms_from_env`, and related helpers (see table below)
//!
//! # Environment variables
//!
//! | Variable | Default | Purpose |
//! |----------|---------|---------|
//! | `CHRONON_NUM_PARTITIONS` | 64 | Partition count for due-job sharding |
//! | `CHRONON_TICK_INTERVAL_MS` | 250 | Sleep between ticks when idle |
//! | `CHRONON_TICK_BATCH_LIMIT` | 500 | Max due jobs processed per tick |
//! | `CHRONON_JOB_CLAIM_LEASE_TTL_S` | 5 | Tick claim lease on a job row |
//! | `CHRONON_PARTITION_LEASE_TTL_S` | 30 | Partition ownership lease |
//! | `CHRONON_PARTITION_LEASE_RENEW_S` | 5 | Partition lease renew interval |
//! | `CHRONON_RUN_LEASE_TTL_S` | 300 | Worker claim lease on a run |
//! | `CHRONON_RUN_LEASE_RENEW_S` | 1 | Worker lease renew interval |
//! | `CHRONON_WORKER_POOL` | `"general"` | Default worker pool id |
//! | `CHRONON_WORKER_CONCURRENCY` | 4 | Concurrent run tasks per worker loop |
//! | `CHRONON_LEADER_TTL_S` | 30 | Scheduler leader lease (see [`try_acquire_leader`]) |
//! | `CHRONON_LEADER_RENEW_S` | 5 | Leader election campaign/renew interval (see [`LeaderElector`]) |
//! | `CHRONON_DISABLE_COORDINATOR` | — | Set to `1` or `true` to pause coordinator ticks |
//! | `CHRONON_DISABLE_WORKER` | — | Set to `1` or `true` to pause worker claiming |
//!
//! `ChrononBuilder::tick_interval_ms` (in `chronon-runtime`) overrides
//! `CHRONON_TICK_INTERVAL_MS` when set at build time. Partition count is env-only.
//!
//! # Notes
//!
//! Embedded mode assigns all partitions locally and skips distributed lease churn.
//! Coordinator ticks enqueue runs only; they do not execute scripts.

mod cron;
mod env;
mod job_builder;
mod leader;
mod partition_assigner;
mod partitioning;
mod tick;
mod tick_loop;

pub use cron::CronExpr;
pub use job_builder::JobBuilder;
pub use leader::{
    am_i_leader, current_leader, renew_leader_lease, try_acquire_leader, LeaderElector,
};
pub use partition_assigner::PartitionAssigner;
pub use partitioning::{
    job_claim_lease_ttl_secs, job_execution_pool_id, num_partitions_from_env,
    partition_hash_i64_for_job_id, run_worker_lease_renew_secs, run_worker_lease_ttl_secs,
    tick_batch_limit_from_env, tick_interval_ms_from_env, worker_concurrency_from_env,
    worker_pool_from_env, DEFAULT_POOL,
};
pub use tick::Scheduler;
pub use tick_loop::{run_coordinator_tick_loop, run_one_tick, TickResult};

use std::sync::Arc;

use chronon_core::store::SchedulerStore;
use chronon_telemetry::TelemetrySink;

/// Scheduler configuration.
///
/// Defaults are loaded from environment helpers such as [`tick_interval_ms_from_env`]; override fields
/// before constructing [`Scheduler`].
pub struct SchedulerConfig {
    /// Sleep between coordinator ticks when no due jobs remain, in milliseconds.
    pub tick_interval_ms: u64,
    /// Unique coordinator instance id used for leader election and job claims.
    pub instance_id: String,
    /// Partition count for sharding due-job queries across coordinators.
    pub num_partitions: u32,
    /// When `true`, assign all partitions locally without distributed lease churn.
    pub embedded: bool,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            tick_interval_ms: partitioning::tick_interval_ms_from_env(),
            instance_id: uuid::Uuid::new_v4().to_string(),
            num_partitions: partitioning::num_partitions_from_env(),
            embedded: true,
        }
    }
}

/// Handle for a running scheduler loop.
///
/// Snapshot of the components passed into [`run_coordinator_tick_loop`] after boot.
pub struct SchedulerHandle {
    /// Tick timing and instance identity used by the coordinator loop.
    pub config: SchedulerConfig,
    /// Persistent job and run store backing ticks.
    pub store: Arc<dyn SchedulerStore>,
    /// Metrics and structured scheduler events.
    pub telemetry: Arc<dyn TelemetrySink>,
    /// Partition lease owner for this coordinator instance.
    pub assigner: Arc<PartitionAssigner>,
}
