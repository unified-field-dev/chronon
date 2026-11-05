//! In-memory [`SchedulerStore`] implementation split by concern.

mod claims;
mod coordinator;
mod jobs;
mod runs;
mod trait_impl;

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use chronon_core::error::{ChrononError, Result};
use chronon_core::models::{
    Job, JobRevision, PartitionAssignment, Run, SchedulerLeader, Script, Worker,
};
use parking_lot::RwLock;

/// Fixed primary key for the singleton leader election row.
pub const LEADER_ROW_ID: &str = "singleton";

/// Thread-safe in-memory persistence for jobs, runs, and coordinator metadata.
///
/// Process-local and **non-durable** — suitable for embedded experiments, unit tests,
/// examples, and benchmarks. Do **not** share across processes; use SQLite or Postgres for
/// durable / coordinator–worker topologies.
///
/// Enable the public crate `mem` feature to re-export this type from `chronon`. Wire with
/// `ChrononBuilder::scheduler_store(Arc::new(InMemorySchedulerStore::new()))` or
/// [`crate::install_default_mem_store`].
///
/// # Locking
///
/// Maps are guarded by [`parking_lot::RwLock`] for short, synchronous critical sections.
/// **Never hold a lock across `.await`** — take the lock, clone or mutate, drop the guard,
/// then await. This keeps Tokio worker threads unblocked under contention.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use chronon_backend_mem::InMemorySchedulerStore;
/// use chronon_core::{Job, SchedulerStore};
///
/// # #[tokio::main]
/// # async fn main() -> chronon_core::Result<()> {
/// let store = Arc::new(InMemorySchedulerStore::new());
/// store.upsert_job(&Job::new("demo", "noop")).await?;
/// assert_eq!(store.list_jobs().await?.len(), 1);
/// # Ok(())
/// # }
/// ```
#[derive(Default)]
pub struct InMemorySchedulerStore {
    /// Jobs keyed by [`Job::job_id`].
    pub(super) jobs: RwLock<HashMap<String, Job>>,
    /// Secondary index from [`Job::job_name`] to job id.
    pub(super) jobs_by_name: RwLock<HashMap<String, String>>,
    /// Runs keyed by [`Run::run_id`].
    pub(super) runs: RwLock<HashMap<String, Run>>,
    /// Job revision history keyed by job id.
    pub(super) revisions: RwLock<HashMap<String, Vec<JobRevision>>>,
    /// Script metadata keyed by script name.
    pub(super) scripts: RwLock<HashMap<String, Script>>,
    /// Singleton scheduler leader row.
    pub(super) leader: RwLock<Option<SchedulerLeader>>,
    /// Partition assignments keyed by partition id.
    pub(super) partitions: RwLock<HashMap<String, PartitionAssignment>>,
    /// Registered workers keyed by worker id.
    pub(super) workers: RwLock<HashMap<String, Worker>>,
}

impl InMemorySchedulerStore {
    /// Creates an empty store.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() {
    /// use chronon_backend_mem::InMemorySchedulerStore;
    /// use chronon_core::SchedulerStore;
    ///
    /// let store = InMemorySchedulerStore::new();
    /// assert!(store.list_jobs().await.unwrap().is_empty());
    /// # }
    /// ```
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or replace a job and update the name index.
    ///
    /// Rejects a name already held by another `job_id` with [`ChrononError::JobNameTaken`],
    /// matching the SQL `UNIQUE (job_name)` constraint, so the old row can't stay due unseen.
    pub(super) fn write_job(&self, job: Job) -> Result<()> {
        let mut jobs = self.jobs.write();
        let mut by_name = self.jobs_by_name.write();
        if let Some(existing_job_id) = by_name.get(&job.job_name) {
            if *existing_job_id != job.job_id {
                return Err(ChrononError::JobNameTaken {
                    job_name: job.job_name,
                    existing_job_id: existing_job_id.clone(),
                });
            }
        }
        if let Some(previous) = jobs.get(&job.job_id) {
            if previous.job_name != job.job_name {
                by_name.remove(&previous.job_name);
            }
        }
        by_name.insert(job.job_name.clone(), job.job_id.clone());
        jobs.insert(job.job_id.clone(), job);
        Ok(())
    }

    /// Remove a job, its name index entry, and its revisions. Runs stay as history.
    pub(super) fn remove_job(&self, job_id: &str) -> bool {
        let mut jobs = self.jobs.write();
        let mut by_name = self.jobs_by_name.write();
        let Some(job) = jobs.remove(job_id) else {
            return false;
        };
        by_name.remove(&job.job_name);
        drop(by_name);
        drop(jobs);
        self.revisions.write().remove(job_id);
        true
    }

    /// Mutate an existing job and stamp `updated_at` to now.
    pub(super) fn mutate_job<F>(&self, job_id: &str, f: F) -> Result<()>
    where
        F: FnOnce(&mut Job),
    {
        let mut jobs = self.jobs.write();
        let job = jobs
            .get_mut(job_id)
            .ok_or_else(|| ChrononError::JobNotFound(job_id.to_string()))?;
        f(job);
        job.updated_at = Utc::now();
        Ok(())
    }

    /// Mutate an existing job and stamp `updated_at` to `now`.
    pub(super) fn mutate_job_at<F>(&self, job_id: &str, now: DateTime<Utc>, f: F) -> Result<()>
    where
        F: FnOnce(&mut Job),
    {
        let mut jobs = self.jobs.write();
        let job = jobs
            .get_mut(job_id)
            .ok_or_else(|| ChrononError::JobNotFound(job_id.to_string()))?;
        f(job);
        job.updated_at = now;
        Ok(())
    }
}
