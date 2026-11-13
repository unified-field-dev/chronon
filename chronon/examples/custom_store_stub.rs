//! Minimal [`SchedulerStore`] stub: wrap an existing store, override a couple of methods, and
//! delegate the rest — **Extending storage** (custom adapter sketch).
//!
//! ```bash
//! cargo run -p uf-chronon --example custom_store_stub --features mem
//! ```
//!
//! Real adapters (`chronon-backend-sqlite`, `-postgres`, `-redis`) implement every method
//! directly against their substrate. This sketch shows the **decorator** shape instead: wrap
//! any `Arc<dyn SchedulerStore>` and intercept only the calls you care about (validation,
//! auditing, metrics) without reimplementing persistence. See `chronon-core/src/store.rs` for
//! the full contract and `store_router_boot.rs` for the global-router wiring alternative.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use chronon::prelude::*;
use chronon_backend_mem::InMemorySchedulerStore;
use chronon_core::{PartitionAssignment, SchedulerLeader, Worker};

/// Decorator over any [`SchedulerStore`] adding job-name validation and run auditing.
///
/// Delegates every method except [`Self::upsert_job`] (rejects blank job names) and
/// [`Self::create_run`] (traces run creation) — the minimal shape for a custom adapter that
/// only needs to intercept a couple of storage calls.
struct AuditingSchedulerStore {
    inner: Arc<dyn SchedulerStore>,
}

impl AuditingSchedulerStore {
    fn new(inner: Arc<dyn SchedulerStore>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl SchedulerStore for AuditingSchedulerStore {
    async fn upsert_job(&self, job: &Job) -> Result<()> {
        if job.job_name.trim().is_empty() {
            return Err(ChrononError::Internal("job_name must not be blank".into()));
        }
        tracing::info!(job_name = %job.job_name, "auditing_store: upsert_job");
        self.inner.upsert_job(job).await
    }

    async fn get_job(&self, job_id: &str) -> Result<Option<Job>> {
        self.inner.get_job(job_id).await
    }

    async fn get_job_by_name(&self, job_name: &str) -> Result<Option<Job>> {
        self.inner.get_job_by_name(job_name).await
    }

    async fn list_jobs(&self) -> Result<Vec<Job>> {
        self.inner.list_jobs().await
    }

    async fn list_due_jobs(&self, before: DateTime<Utc>) -> Result<Vec<Job>> {
        self.inner.list_due_jobs(before).await
    }

    async fn pause_job(&self, job_id: &str) -> Result<()> {
        self.inner.pause_job(job_id).await
    }

    async fn resume_job(&self, job_id: &str) -> Result<()> {
        self.inner.resume_job(job_id).await
    }

    async fn create_run(&self, run: &Run) -> Result<()> {
        tracing::info!(run_id = %run.run_id, job_id = ?run.job_id, "auditing_store: create_run");
        self.inner.create_run(run).await
    }

    async fn update_run(&self, run: &Run) -> Result<()> {
        self.inner.update_run(run).await
    }

    async fn get_run(&self, run_id: &str) -> Result<Option<Run>> {
        self.inner.get_run(run_id).await
    }

    async fn list_runs_for_job(&self, job_id: &str, limit: usize) -> Result<Vec<Run>> {
        self.inner.list_runs_for_job(job_id, limit).await
    }

    async fn list_runs_filtered(
        &self,
        job_id: Option<&str>,
        status: Option<RunStatus>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Run>> {
        self.inner
            .list_runs_filtered(job_id, status, offset, limit)
            .await
    }

    async fn list_runs_after(
        &self,
        job_id: Option<&str>,
        status: Option<RunStatus>,
        after: Option<&RunKey>,
        limit: usize,
    ) -> Result<Vec<Run>> {
        self.inner
            .list_runs_after(job_id, status, after, limit)
            .await
    }

    async fn claim_next_queued(
        &self,
        pool_id: &str,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_ttl_secs: i64,
    ) -> Result<Option<Run>> {
        self.inner
            .claim_next_queued(pool_id, worker_id, now, lease_ttl_secs)
            .await
    }

    async fn claim_run_by_id(
        &self,
        run_id: &str,
        pool_id: &str,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_ttl_secs: i64,
    ) -> Result<Option<Run>> {
        self.inner
            .claim_run_by_id(run_id, pool_id, worker_id, now, lease_ttl_secs)
            .await
    }

    async fn renew_run_lease(
        &self,
        run_id: &str,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_ttl_secs: i64,
    ) -> Result<bool> {
        self.inner
            .renew_run_lease(run_id, worker_id, now, lease_ttl_secs)
            .await
    }

    async fn reclaim_expired_run_leases(&self, now: DateTime<Utc>) -> Result<Vec<String>> {
        self.inner.reclaim_expired_run_leases(now).await
    }

    async fn append_revision(&self, revision: &JobRevision) -> Result<()> {
        self.inner.append_revision(revision).await
    }

    async fn list_revisions(&self, job_id: &str) -> Result<Vec<JobRevision>> {
        self.inner.list_revisions(job_id).await
    }

    async fn upsert_script(&self, script: &Script) -> Result<()> {
        self.inner.upsert_script(script).await
    }

    async fn get_script(&self, script_name: &str) -> Result<Option<Script>> {
        self.inner.get_script(script_name).await
    }

    async fn try_claim_run_once(
        &self,
        job_id: &str,
        claimed_by: &str,
        now: DateTime<Utc>,
        claim_ttl_secs: i64,
    ) -> Result<bool> {
        self.inner
            .try_claim_run_once(job_id, claimed_by, now, claim_ttl_secs)
            .await
    }

    async fn mark_run_once_completed(
        &self,
        job_id: &str,
        completed_at: DateTime<Utc>,
    ) -> Result<()> {
        self.inner
            .mark_run_once_completed(job_id, completed_at)
            .await
    }

    async fn release_run_once_claim(
        &self,
        job_id: &str,
        claimed_by: &str,
        now: DateTime<Utc>,
    ) -> Result<()> {
        self.inner
            .release_run_once_claim(job_id, claimed_by, now)
            .await
    }

    async fn find_due_job_ids_in_partitions(
        &self,
        owned_partitions: &[u32],
        due_until: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<String>> {
        self.inner
            .find_due_job_ids_in_partitions(owned_partitions, due_until, limit)
            .await
    }

    async fn min_next_run_at_in_partitions(
        &self,
        owned_partitions: &[u32],
    ) -> Result<Option<DateTime<Utc>>> {
        self.inner
            .min_next_run_at_in_partitions(owned_partitions)
            .await
    }

    async fn claim_job_for_tick(
        &self,
        job_id: &str,
        claim_id: &str,
        now: DateTime<Utc>,
        lease_ttl_secs: i64,
    ) -> Result<bool> {
        self.inner
            .claim_job_for_tick(job_id, claim_id, now, lease_ttl_secs)
            .await
    }

    async fn release_job_tick_claim(&self, job_id: &str) -> Result<()> {
        self.inner.release_job_tick_claim(job_id).await
    }

    async fn persist_post_tick_job_state(
        &self,
        job_id: &str,
        next_run_at: Option<DateTime<Utc>>,
    ) -> Result<()> {
        self.inner
            .persist_post_tick_job_state(job_id, next_run_at)
            .await
    }

    async fn try_acquire_leader(&self, instance_id: &str, ttl_secs: i64) -> Result<bool> {
        self.inner.try_acquire_leader(instance_id, ttl_secs).await
    }

    async fn renew_leader_lease(&self, instance_id: &str, ttl_secs: i64) -> Result<()> {
        self.inner.renew_leader_lease(instance_id, ttl_secs).await
    }

    async fn get_leader(&self) -> Result<Option<SchedulerLeader>> {
        self.inner.get_leader().await
    }

    async fn upsert_partition_assignment(&self, assignment: &PartitionAssignment) -> Result<()> {
        self.inner.upsert_partition_assignment(assignment).await
    }

    async fn list_partition_assignments(&self) -> Result<Vec<PartitionAssignment>> {
        self.inner.list_partition_assignments().await
    }

    async fn register_worker(&self, worker: &Worker) -> Result<()> {
        self.inner.register_worker(worker).await
    }

    async fn heartbeat_worker(&self, worker_id: &str, at: DateTime<Utc>) -> Result<()> {
        self.inner.heartbeat_worker(worker_id, at).await
    }
}

#[chronon::script(name = "audited_job")]
#[allow(clippy::unused_async)] // script handlers are always async
async fn audited_job(ctx: Box<dyn ScriptContext>) -> chronon::Result<()> {
    let _ = ctx.label();
    Ok(())
}

#[tokio::main]
async fn main() -> chronon::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();

    let inner: Arc<dyn SchedulerStore> = Arc::new(InMemorySchedulerStore::new());
    let store: Arc<dyn SchedulerStore> = Arc::new(AuditingSchedulerStore::new(inner));

    // Fail-closed proof: blank job_name is rejected before it reaches the wrapped store.
    // (JobBuilder requires a name, so this deliberately uses Job::new + mutate.)
    let mut blank_named = Job::new("placeholder", "audited_job");
    blank_named.job_name = String::new();
    assert!(store.upsert_job(&blank_named).await.is_err());

    let chronon = ChrononBuilder::new()
        .scheduler_store(store)
        .context_factory(Arc::new(JsonScriptContextFactory))
        .embedded()
        .auto_registry()
        .build()?;

    let job = JobBuilder::new(&audited_job())
        .name("audited-job")
        .run_once_at(Utc::now() - Duration::seconds(60))
        .build()?;
    chronon.coordinator_service().upsert_job(job).await?;

    chronon.scheduler.init_partitions().await;
    let tick = chronon.tick_once().await?;
    assert!(tick.enqueued >= 1, "expected at least one enqueued run");

    eprintln!(
        "custom_store_stub: blank job_name rejected; tick enqueued {} run(s) through AuditingSchedulerStore",
        tick.enqueued
    );
    Ok(())
}
