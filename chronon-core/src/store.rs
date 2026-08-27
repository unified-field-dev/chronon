//! Scheduler persistence port for jobs, runs, revisions, and coordinator metadata.

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::error::Result;
use crate::models::{
    Job, JobRevision, PartitionAssignment, Run, RunStatus, SchedulerLeader, Script, Worker,
};

/// Async persistence port for jobs, runs, revisions, and coordinator metadata.
///
/// Hosts provide one implementation per storage substrate. The scheduler, executor, and
/// HTTP API call these methods; implementations must be `Send + Sync` for shared use across
/// Tokio tasks.
///
/// | Adapter | Public crate feature | Topology fit |
/// |---------|----------------|--------------|
/// | `InMemorySchedulerStore` | `mem` | Embedded local / tests (not multi-process) |
/// | `SqliteSchedulerStore` | `sqlite` | Embedded single-host durable |
/// | `PostgresSchedulerStore` | `postgres` | Coordinator–worker shared durable |
/// | `PostgresRedisSchedulerStore` | `postgres,redis` | Coordinator–worker production claim path |
///
/// Inject via `ChrononBuilder::scheduler_store` (or [`crate::StoreRouter`] +
/// `scheduler_store_from_global`). Custom adapters implement this trait in a separate crate.
///
/// # Contract
///
/// - Job rows are keyed by [`Job::job_id`]; [`Job::job_name`] is unique per deployment.
/// - Run claims must be atomic: at most one worker holds a claimed run at a time.
/// - Tick claims prevent duplicate enqueue when coordinators race on the same job.
/// - Leader election uses a singleton row with TTL renewal semantics.
///
/// # Examples
///
/// Trait-object usage (pass any adapter that implements the port):
///
/// ```
/// use std::sync::Arc;
/// use chronon_core::{Job, SchedulerStore};
///
/// async fn seed(store: Arc<dyn SchedulerStore>) -> chronon_core::Result<()> {
///     store.upsert_job(&Job::new("demo", "noop")).await?;
///     assert_eq!(store.list_jobs().await?.len(), 1);
///     Ok(())
/// }
/// ```
///
/// Concrete adapters and runnable boots live in `chronon-backend-mem` / `-sqlite` /
/// `-postgres` / `-redis` and the `uf-chronon` examples (`sqlite_boot`, `postgres_boot`, …).
#[async_trait]
pub trait SchedulerStore: Send + Sync {
    // --- Jobs ---

    /// Insert or replace a job row keyed by [`Job::job_id`].
    ///
    /// # Contract
    ///
    /// Replaces the full row; callers must send complete job state on update.
    async fn upsert_job(&self, job: &Job) -> Result<()>;

    /// Look up a job by primary key.
    async fn get_job(&self, job_id: &str) -> Result<Option<Job>>;

    /// Look up a job by unique [`Job::job_name`].
    async fn get_job_by_name(&self, job_name: &str) -> Result<Option<Job>>;

    /// Return all jobs (admin / list API).
    async fn list_jobs(&self) -> Result<Vec<Job>>;

    /// Jobs with `next_run_at <= before` and enabled scheduling (tick discovery).
    ///
    /// # Contract
    ///
    /// Returns only enabled jobs whose `next_run_at` is set and `<= before`.
    async fn list_due_jobs(&self, before: DateTime<Utc>) -> Result<Vec<Job>>;

    /// Disable automatic scheduling without deleting the job.
    async fn pause_job(&self, job_id: &str) -> Result<()>;

    /// Re-enable automatic scheduling after [`Self::pause_job`].
    async fn resume_job(&self, job_id: &str) -> Result<()>;

    // --- Runs ---

    /// Persist a new run row (typically `RunStatus::Queued`).
    ///
    /// # Contract
    ///
    /// `run_id` must be unique; duplicate inserts are a backend error.
    async fn create_run(&self, run: &Run) -> Result<()>;

    /// Replace an existing run row (status transitions, lease renewal, completion).
    async fn update_run(&self, run: &Run) -> Result<()>;

    /// Look up a run by [`Run::run_id`].
    async fn get_run(&self, run_id: &str) -> Result<Option<Run>>;

    /// Recent runs for one job, newest first, capped by `limit`.
    async fn list_runs_for_job(&self, job_id: &str, limit: usize) -> Result<Vec<Run>>;

    /// Paginated run listing with optional job and status filters (HTTP list API).
    async fn list_runs_filtered(
        &self,
        job_id: Option<&str>,
        status: Option<RunStatus>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Run>>;

    /// Claim the next queued run for a worker pool.
    ///
    /// # Contract
    ///
    /// Atomically selects a `Queued` run matching `pool_id`, sets `claimed_by`, and writes
    /// `claim_lease_until = now + lease_ttl_secs`. Returns `None` when the pool queue is empty.
    /// At most one worker may hold a given run claim at a time.
    async fn claim_next_queued(
        &self,
        pool_id: &str,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_ttl_secs: i64,
    ) -> Result<Option<Run>>;

    /// Claim a specific queued run by id (postgres-redis hybrid hot path).
    ///
    /// Returns `None` when the run is not queued, pool/lease checks fail, or the row
    /// does not exist.
    async fn claim_run_by_id(
        &self,
        run_id: &str,
        pool_id: &str,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_ttl_secs: i64,
    ) -> Result<Option<Run>>;

    /// Claim multiple queued runs by id in one round trip when the backend supports it.
    ///
    /// Default implementation claims each id sequentially via [`Self::claim_run_by_id`].
    async fn claim_runs_by_ids(
        &self,
        run_ids: &[&str],
        pool_id: &str,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_ttl_secs: i64,
    ) -> Result<Vec<Run>> {
        let mut claimed = Vec::new();
        for run_id in run_ids {
            if let Some(run) = self
                .claim_run_by_id(run_id, pool_id, worker_id, now, lease_ttl_secs)
                .await?
            {
                claimed.push(run);
            }
        }
        Ok(claimed)
    }

    /// Extend the worker lease on a claimed run if `worker_id` still holds the claim.
    ///
    /// Returns `false` when the run is not claimed by this worker or the lease expired.
    async fn renew_run_lease(
        &self,
        run_id: &str,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_ttl_secs: i64,
    ) -> Result<bool>;

    /// Reset `claimed` / `running` runs whose `claim_lease_until` is at or before `now` to
    /// [`RunStatus::Queued`].
    ///
    /// Clears `claimed_by`, `claim_lease_until`, and `started_at`. Returns the reclaimed
    /// `run_id` values so hybrid backends can re-enqueue claim queues.
    async fn reclaim_expired_run_leases(&self, now: DateTime<Utc>) -> Result<Vec<String>>;

    // --- Revisions ---

    /// Append an immutable job revision snapshot (audit / rollback).
    async fn append_revision(&self, revision: &JobRevision) -> Result<()>;

    /// All revisions for a job, typically oldest-first.
    async fn list_revisions(&self, job_id: &str) -> Result<Vec<JobRevision>>;

    // --- Scripts ---

    /// Insert or replace script metadata (name, signature hash).
    async fn upsert_script(&self, script: &Script) -> Result<()>;

    /// Look up persisted script metadata by name.
    async fn get_script(&self, script_name: &str) -> Result<Option<Script>>;

    // --- Run-once coordinator safety ---

    /// Attempt exclusive claim for a run-once job before enqueueing its single run.
    ///
    /// Returns `true` when this `claimed_by` instance acquired the claim; `false` when another
    /// coordinator already holds it or the job already completed.
    async fn try_claim_run_once(
        &self,
        job_id: &str,
        claimed_by: &str,
        now: DateTime<Utc>,
        claim_ttl_secs: i64,
    ) -> Result<bool>;

    /// Mark a run-once job as finished so future ticks skip it.
    async fn mark_run_once_completed(
        &self,
        job_id: &str,
        completed_at: DateTime<Utc>,
    ) -> Result<()>;

    /// Release a run-once claim when enqueue failed or the coordinator shut down cleanly.
    async fn release_run_once_claim(
        &self,
        job_id: &str,
        claimed_by: &str,
        now: DateTime<Utc>,
    ) -> Result<()>;

    // --- Tick / partition coordinator ---

    /// Due job ids owned by this coordinator's partition slice (distributed tick).
    async fn find_due_job_ids_in_partitions(
        &self,
        owned_partitions: &[u32],
        due_until: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<String>>;

    /// Earliest `next_run_at` among jobs in the owned partitions (sleep hint for tick loop).
    async fn min_next_run_at_in_partitions(
        &self,
        owned_partitions: &[u32],
    ) -> Result<Option<DateTime<Utc>>>;

    /// Exclusive short-lived lease on a job row during tick processing.
    ///
    /// Prevents duplicate enqueue when multiple scheduler instances race on the same job.
    async fn claim_job_for_tick(
        &self,
        job_id: &str,
        claim_id: &str,
        now: DateTime<Utc>,
        lease_ttl_secs: i64,
    ) -> Result<bool>;

    /// Release tick claim after enqueue succeeds or the tick aborts.
    async fn release_job_tick_claim(&self, job_id: &str) -> Result<()>;

    /// Persist `next_run_at` (and related job fields) after a successful tick.
    async fn persist_post_tick_job_state(
        &self,
        job_id: &str,
        next_run_at: Option<DateTime<Utc>>,
    ) -> Result<()>;

    // --- Scheduler leader ---

    /// Attempt to become the active scheduler leader (singleton row + TTL).
    async fn try_acquire_leader(&self, instance_id: &str, ttl_secs: i64) -> Result<bool>;

    /// Renew the leader lease while this instance remains leader.
    async fn renew_leader_lease(&self, instance_id: &str, ttl_secs: i64) -> Result<()>;

    /// Current leader row, if any (expired leases may still be returned for diagnostics).
    async fn get_leader(&self) -> Result<Option<SchedulerLeader>>;

    // --- Partitions / workers ---

    /// Upsert partition ownership for coordinator sharding.
    async fn upsert_partition_assignment(&self, assignment: &PartitionAssignment) -> Result<()>;

    /// All partition assignments (rebalance / diagnostics).
    async fn list_partition_assignments(&self) -> Result<Vec<PartitionAssignment>>;

    /// Register or update a worker heartbeat row.
    async fn register_worker(&self, worker: &Worker) -> Result<()>;

    /// Update `last_heartbeat_at` for an existing worker.
    async fn heartbeat_worker(&self, worker_id: &str, at: DateTime<Utc>) -> Result<()>;
}
