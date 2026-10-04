//! Job and run CRUD for HTTP handlers and host integration.

use std::sync::Arc;

use chrono::Utc;
use chronon_core::models::{Job, JobRevision, Run, RunStatus, ScheduleKind};
use chronon_core::store::SchedulerStore;
use chronon_core::{ChrononError, Result};
use chronon_scheduler::{job_execution_pool_id, partition_hash_i64_for_job_id, CronExpr};
use serde_json::Value;

/// Job and run CRUD backed by [`SchedulerStore`] — no background loops.
///
/// Use this when the host (or Axum handlers) needs to upsert jobs, pause/resume, list runs,
/// or trigger [`Self::run_now`] without owning scheduler ticks. Obtained from
/// [`crate::Chronon::coordinator_service`] or constructed with [`Self::new`] for HTTP /
/// remote-client API hosts.
///
/// | Method | Role |
/// |--------|------|
/// | [`Self::upsert_job`] | Insert/update; computes partition hash and cron `next_run_at` |
/// | [`Self::run_now`] | Enqueue an immediate run (required for [`ScheduleKind::Manual`](chronon_core::ScheduleKind::Manual)) |
/// | [`Self::list_jobs`] / [`Self::list_runs`] | Admin / HTTP list |
///
/// For remote processes that cannot share the store, use [`crate::RemoteCoordinatorClient`]
/// against a host that mounts `chronon_router` (remote HTTP client topology).
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use chronon_backend_mem::InMemorySchedulerStore;
/// use chronon_core::{Job, ScheduleKind, SchedulerStore};
/// use chronon_runtime::CoordinatorService;
///
/// # #[tokio::main]
/// # async fn main() -> chronon_core::Result<()> {
/// let store = Arc::new(InMemorySchedulerStore::new());
/// let coordinator = CoordinatorService::new(store.clone());
///
/// let mut job = Job::new("manual-job", "noop");
/// job.schedule_kind = ScheduleKind::Manual;
/// coordinator.upsert_job(job.clone()).await?;
///
/// let run_id = coordinator.run_now(&job.job_id).await?;
/// let run = store.get_run(&run_id).await?.expect("queued");
/// assert_eq!(run.status, chronon_core::RunStatus::Queued);
/// # Ok(())
/// # }
/// ```
pub struct CoordinatorService {
    store: Arc<dyn SchedulerStore>,
}

impl CoordinatorService {
    /// Wraps an existing store; does not start background tasks.
    pub fn new(store: Arc<dyn SchedulerStore>) -> Self {
        Self { store }
    }

    /// Underlying store for advanced host queries.
    pub fn store(&self) -> Arc<dyn SchedulerStore> {
        Arc::clone(&self.store)
    }

    /// Insert or update a job, computing partition hash and next cron fire time when needed.
    ///
    /// Appends a [`JobRevision`] when `current_revision` changes.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use chronon_backend_mem::InMemorySchedulerStore;
    /// use chronon_core::{Job, ScheduleKind};
    /// use chronon_runtime::CoordinatorService;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> chronon_core::Result<()> {
    /// let store = Arc::new(InMemorySchedulerStore::new());
    /// let coordinator = CoordinatorService::new(store);
    /// let mut job = Job::new("nightly", "noop");
    /// job.schedule_kind = ScheduleKind::Manual;
    /// coordinator.upsert_job(job).await?;
    /// assert_eq!(coordinator.list_jobs().await?.len(), 1);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// End-to-end with a real script: `cargo run -p uf-chronon --example script_macro --features mem`
    /// (stringly job) or `script_handle_job` (typed `ScriptHandle` defaults).
    pub async fn upsert_job(&self, mut job: Job) -> Result<()> {
        if job.partition_hash.is_none() {
            job.partition_hash = Some(partition_hash_i64_for_job_id(&job.job_id));
        }
        if job.schedule_kind == ScheduleKind::Cron {
            if let Some(ref cron_expr) = job.cron_expr {
                let cron = CronExpr::parse(cron_expr, job.timezone.as_deref())?;
                job.next_run_at = cron.next_from_now();
            }
        }
        job.updated_at = Utc::now();

        if let Some(existing) = self.store.get_job(&job.job_id).await? {
            if existing.script_name != job.script_name {
                return Err(ChrononError::ScriptMismatch {
                    expected: existing.script_name,
                    actual: job.script_name,
                    job_name: job.job_name,
                });
            }
            if existing.current_revision != job.current_revision {
                let revision = JobRevision::new(
                    &job.job_id,
                    job.current_revision,
                    job.actor_json.clone(),
                    serde_json::to_value(&job)?,
                );
                self.store.append_revision(&revision).await?;
            }
        } else {
            let revision = JobRevision::new(
                &job.job_id,
                1,
                job.actor_json.clone(),
                serde_json::to_value(&job)?,
            );
            self.store.append_revision(&revision).await?;
        }

        self.store.upsert_job(&job).await
    }

    /// Load a job by stable `job_id`.
    pub async fn get_job(&self, job_id: &str) -> Option<Job> {
        self.store.get_job(job_id).await.ok().flatten()
    }

    /// Load a job by human-readable `job_name`.
    pub async fn get_job_by_name(&self, job_name: &str) -> Option<Job> {
        self.store.get_job_by_name(job_name).await.ok().flatten()
    }

    /// All jobs in the store.
    ///
    /// # Errors
    ///
    /// Returns a storage error when the underlying store fails.
    pub async fn list_jobs(&self) -> Result<Vec<Job>> {
        self.store.list_jobs().await
    }

    /// Disable scheduling for `job_id` without deleting the row.
    pub async fn pause_job(&self, job_id: &str) -> Result<()> {
        self.store.pause_job(job_id).await
    }

    /// Re-enable scheduling for `job_id`.
    pub async fn resume_job(&self, job_id: &str) -> Result<()> {
        self.store.resume_job(job_id).await
    }

    /// Paginated run listing with optional `job_id` and status string filters.
    ///
    /// Unrecognized status strings are ignored (no filter).
    pub async fn list_runs(
        &self,
        job_id: Option<&str>,
        status: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Run>> {
        let status_filter = status.and_then(parse_run_status);
        self.store
            .list_runs_filtered(job_id, status_filter, offset, limit)
            .await
    }

    /// Load a single run by `run_id`.
    pub async fn get_run(&self, run_id: &str) -> Result<Option<Run>> {
        self.store.get_run(run_id).await
    }

    /// Revision history for `job_id`, oldest first per store ordering.
    pub async fn list_revisions(&self, job_id: &str) -> Result<Vec<JobRevision>> {
        self.store.list_revisions(job_id).await
    }

    /// Enqueue an immediate run using the job's stored `params_json`.
    ///
    /// Returns the new `run_id`. Errors with [`ChrononError::JobNotFound`] when missing.
    ///
    /// Manual jobs ([`ScheduleKind::Manual`]) are never due for the tick loop; use this
    /// (or HTTP `POST /jobs/run_now`) to trigger them. Works for any schedule kind.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use chronon_backend_mem::InMemorySchedulerStore;
    /// use chronon_core::{Job, ScheduleKind};
    /// use chronon_runtime::CoordinatorService;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> chronon_core::Result<()> {
    /// let coordinator = CoordinatorService::new(Arc::new(InMemorySchedulerStore::new()));
    /// let mut job = Job::new("probe", "noop");
    /// job.schedule_kind = ScheduleKind::Manual;
    /// coordinator.upsert_job(job.clone()).await?;
    /// let run_id = coordinator.run_now(&job.job_id).await?;
    /// assert!(!run_id.is_empty());
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Runnable sample: `cargo run -p uf-chronon --example run_now --features mem`.
    pub async fn run_now(&self, job_id: &str) -> Result<String> {
        self.run_now_with_params(job_id, None).await
    }

    /// Enqueue an immediate run, optionally overriding params JSON.
    pub async fn run_now_with_params(
        &self,
        job_id: &str,
        params_override: Option<Value>,
    ) -> Result<String> {
        self.run_now_with_params_and_actor(job_id, params_override, None)
            .await
    }

    /// Enqueue an immediate run with optional params and actor snapshot overrides.
    ///
    /// When `actor_override` is `Some`, it is written onto the run (user-triggered
    /// enqueue). When `None`, the job's stored `actor_json` is used (autonomous /
    /// default jobs typically System).
    pub async fn run_now_with_params_and_actor(
        &self,
        job_id: &str,
        params_override: Option<Value>,
        actor_override: Option<Value>,
    ) -> Result<String> {
        let Some(job) = self.store.get_job(job_id).await? else {
            return Err(ChrononError::JobNotFound(job_id.to_string()));
        };
        let now = Utc::now();
        let mut run = Run::for_job(&job.job_id, &job.script_name, now);
        run.actor_json = actor_override.unwrap_or_else(|| job.actor_json.clone());
        run.params_json = params_override.unwrap_or_else(|| job.params_json.clone());
        run.pool_id = Some(job_execution_pool_id(&job));
        let run_id = run.run_id.clone();
        self.store.create_run(&run).await?;
        Ok(run_id)
    }
}

fn parse_run_status(s: &str) -> Option<RunStatus> {
    match s.to_ascii_lowercase().as_str() {
        "queued" => Some(RunStatus::Queued),
        "claimed" => Some(RunStatus::Claimed),
        "running" => Some(RunStatus::Running),
        "success" => Some(RunStatus::Success),
        "failed" => Some(RunStatus::Failed),
        "canceled" => Some(RunStatus::Canceled),
        "timeout" => Some(RunStatus::Timeout),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chronon_backend_mem::InMemorySchedulerStore;

    #[tokio::test]
    async fn upsert_and_run_now() {
        let store = Arc::new(InMemorySchedulerStore::new());
        let svc = CoordinatorService::new(store);
        let job = Job::new("j1", "script_a");
        svc.upsert_job(job.clone()).await.unwrap();
        let run_id = svc.run_now(&job.job_id).await.unwrap();
        assert_ne!(run_id, "");
    }
}
