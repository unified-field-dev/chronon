//! PostgreSQL (or any SQL store) plus Redis ready-queue composite.
//!
//! Internal — used by [`PostgresRedisSchedulerStore`](crate::PostgresRedisSchedulerStore).

mod claim;

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use chronon_backend_sql_common::run_pool_key;
use chronon_core::models::{Run, RunKey, RunStatus};
use chronon_core::store::SchedulerStore;
use chronon_core::Result;

use crate::queue::RedisQueueLayer;

/// SQL persistence with Redis-backed run claim ordering.
///
/// Production coordinator–worker store: job/run rows stay in Postgres (or any [`SchedulerStore`]);
/// workers claim through Redis ZSETs for higher throughput. Requires public crate features
/// `postgres` and `redis`. Build with [`Self::new`] after connecting both layers.
///
/// Split examples: [coordinator](index.html#coordinator-binary) /
/// [worker](index.html#worker-binary).
///
/// See [`crate::RedisQueueLayer`] for URL / prefix / cluster env vars
/// (`CHRONON_REDIS_URL`, `CHRONON_REDIS_CLUSTER_URLS`, `CHRONON_REDIS_HASH_TAGS`).
///
/// Runnable: `cargo run -p uf-chronon --example postgres_redis_boot --features postgres,redis`
/// and the `coordinator_daemon` / `worker_daemon` pair.
pub struct PostgresRedisSchedulerStore {
    sql: Arc<dyn SchedulerStore>,
    redis: RedisQueueLayer,
}

impl std::fmt::Debug for PostgresRedisSchedulerStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresRedisSchedulerStore")
            .finish_non_exhaustive()
    }
}

impl PostgresRedisSchedulerStore {
    /// Wrap a SQL store and Redis queue layer.
    ///
    /// `create_run` writes SQL then enqueues Redis for queued runs; `claim_next_queued` pops
    /// Redis then updates SQL lease state.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use std::sync::Arc;
    ///
    /// use chronon_backend_postgres::PostgresSchedulerStore;
    /// use chronon_backend_redis::{PostgresRedisSchedulerStore, RedisQueueLayer};
    /// use chronon_core::store::SchedulerStore;
    ///
    /// # async fn example() -> chronon_core::Result<()> {
    /// let sql: Arc<dyn SchedulerStore> = Arc::new(
    ///     PostgresSchedulerStore::connect("postgres://localhost/chronon").await?,
    /// );
    /// let redis = RedisQueueLayer::connect("redis://127.0.0.1:6379", Some("myapp")).await?;
    /// let store = PostgresRedisSchedulerStore::new(sql, redis);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn new(sql: Arc<dyn SchedulerStore>, redis: RedisQueueLayer) -> Self {
        Self { sql, redis }
    }
}

#[async_trait]
impl SchedulerStore for PostgresRedisSchedulerStore {
    async fn upsert_job(&self, job: &chronon_core::models::Job) -> Result<()> {
        self.sql.upsert_job(job).await
    }

    async fn get_job(&self, job_id: &str) -> Result<Option<chronon_core::models::Job>> {
        self.sql.get_job(job_id).await
    }

    async fn get_job_by_name(&self, job_name: &str) -> Result<Option<chronon_core::models::Job>> {
        self.sql.get_job_by_name(job_name).await
    }

    async fn list_jobs(&self) -> Result<Vec<chronon_core::models::Job>> {
        self.sql.list_jobs().await
    }

    async fn list_due_jobs(&self, before: DateTime<Utc>) -> Result<Vec<chronon_core::models::Job>> {
        self.sql.list_due_jobs(before).await
    }

    async fn pause_job(&self, job_id: &str) -> Result<()> {
        self.sql.pause_job(job_id).await
    }

    async fn delete_job(&self, job_id: &str) -> Result<bool> {
        self.sql.delete_job(job_id).await
    }

    async fn resume_job(&self, job_id: &str) -> Result<()> {
        self.sql.resume_job(job_id).await
    }

    async fn create_run(&self, run: &Run) -> Result<()> {
        self.sql.create_run(run).await?;
        if run.status == RunStatus::Queued {
            let pool = run_pool_key(run.pool_id.as_deref());
            self.redis
                .enqueue_run(pool, &run.run_id, run.scheduled_for)
                .await?;
        }
        Ok(())
    }

    async fn update_run(&self, run: &Run) -> Result<()> {
        self.sql.update_run(run).await
    }

    async fn get_run(&self, run_id: &str) -> Result<Option<Run>> {
        self.sql.get_run(run_id).await
    }

    async fn list_runs_for_job(&self, job_id: &str, limit: usize) -> Result<Vec<Run>> {
        self.sql.list_runs_for_job(job_id, limit).await
    }

    async fn list_runs_filtered(
        &self,
        job_id: Option<&str>,
        status: Option<RunStatus>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Run>> {
        self.sql
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
        self.sql.list_runs_after(job_id, status, after, limit).await
    }

    async fn claim_next_queued(
        &self,
        pool_id: &str,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_ttl_secs: i64,
    ) -> Result<Option<Run>> {
        claim::claim_next_queued(
            &self.sql,
            &self.redis,
            pool_id,
            worker_id,
            now,
            lease_ttl_secs,
        )
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
        self.sql
            .claim_run_by_id(run_id, pool_id, worker_id, now, lease_ttl_secs)
            .await
    }

    async fn claim_runs_by_ids(
        &self,
        run_ids: &[&str],
        pool_id: &str,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_ttl_secs: i64,
    ) -> Result<Vec<Run>> {
        self.sql
            .claim_runs_by_ids(run_ids, pool_id, worker_id, now, lease_ttl_secs)
            .await
    }

    async fn renew_run_lease(
        &self,
        run_id: &str,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_ttl_secs: i64,
    ) -> Result<bool> {
        self.sql
            .renew_run_lease(run_id, worker_id, now, lease_ttl_secs)
            .await
    }

    async fn reclaim_expired_run_leases(&self, now: DateTime<Utc>) -> Result<Vec<String>> {
        let ids = self.sql.reclaim_expired_run_leases(now).await?;
        for run_id in &ids {
            if let Some(run) = self.sql.get_run(run_id).await? {
                let pool = run_pool_key(run.pool_id.as_deref());
                self.redis
                    .enqueue_run(pool, &run.run_id, run.scheduled_for)
                    .await?;
            }
        }
        Ok(ids)
    }

    async fn append_revision(&self, revision: &chronon_core::models::JobRevision) -> Result<()> {
        self.sql.append_revision(revision).await
    }

    async fn list_revisions(&self, job_id: &str) -> Result<Vec<chronon_core::models::JobRevision>> {
        self.sql.list_revisions(job_id).await
    }

    async fn upsert_script(&self, script: &chronon_core::models::Script) -> Result<()> {
        self.sql.upsert_script(script).await
    }

    async fn get_script(&self, script_name: &str) -> Result<Option<chronon_core::models::Script>> {
        self.sql.get_script(script_name).await
    }

    async fn try_claim_run_once(
        &self,
        job_id: &str,
        claimed_by: &str,
        now: DateTime<Utc>,
        claim_ttl_secs: i64,
    ) -> Result<bool> {
        self.sql
            .try_claim_run_once(job_id, claimed_by, now, claim_ttl_secs)
            .await
    }

    async fn mark_run_once_completed(
        &self,
        job_id: &str,
        completed_at: DateTime<Utc>,
    ) -> Result<()> {
        self.sql.mark_run_once_completed(job_id, completed_at).await
    }

    async fn release_run_once_claim(
        &self,
        job_id: &str,
        claimed_by: &str,
        now: DateTime<Utc>,
    ) -> Result<()> {
        self.sql
            .release_run_once_claim(job_id, claimed_by, now)
            .await
    }

    async fn find_due_job_ids_in_partitions(
        &self,
        owned_partitions: &[u32],
        due_until: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<String>> {
        self.sql
            .find_due_job_ids_in_partitions(owned_partitions, due_until, limit)
            .await
    }

    async fn min_next_run_at_in_partitions(
        &self,
        owned_partitions: &[u32],
    ) -> Result<Option<DateTime<Utc>>> {
        self.sql
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
        self.sql
            .claim_job_for_tick(job_id, claim_id, now, lease_ttl_secs)
            .await
    }

    async fn release_job_tick_claim(&self, job_id: &str) -> Result<()> {
        self.sql.release_job_tick_claim(job_id).await
    }

    async fn persist_post_tick_job_state(
        &self,
        job_id: &str,
        next_run_at: Option<DateTime<Utc>>,
    ) -> Result<()> {
        self.sql
            .persist_post_tick_job_state(job_id, next_run_at)
            .await
    }

    async fn try_acquire_leader(&self, instance_id: &str, ttl_secs: i64) -> Result<bool> {
        self.sql.try_acquire_leader(instance_id, ttl_secs).await
    }

    async fn renew_leader_lease(&self, instance_id: &str, ttl_secs: i64) -> Result<()> {
        self.sql.renew_leader_lease(instance_id, ttl_secs).await
    }

    async fn get_leader(&self) -> Result<Option<chronon_core::models::SchedulerLeader>> {
        self.sql.get_leader().await
    }

    async fn upsert_partition_assignment(
        &self,
        assignment: &chronon_core::models::PartitionAssignment,
    ) -> Result<()> {
        self.sql.upsert_partition_assignment(assignment).await
    }

    async fn list_partition_assignments(
        &self,
    ) -> Result<Vec<chronon_core::models::PartitionAssignment>> {
        self.sql.list_partition_assignments().await
    }

    async fn register_worker(&self, worker: &chronon_core::models::Worker) -> Result<()> {
        self.sql.register_worker(worker).await
    }

    async fn heartbeat_worker(&self, worker_id: &str, at: DateTime<Utc>) -> Result<()> {
        self.sql.heartbeat_worker(worker_id, at).await
    }
}
