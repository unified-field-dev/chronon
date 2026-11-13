//! Run model - represents an execution instance of a job.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Status of a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RunStatus {
    /// Run is waiting to be executed (distributed: worker has not claimed yet).
    #[default]
    Queued,
    /// Run is claimed by a worker (lease held); not yet marked as executing.
    Claimed,
    /// Run is currently executing.
    Running,
    /// Run completed successfully.
    Success,
    /// Run failed with an error.
    Failed,
    /// Run was manually canceled.
    Canceled,
    /// Run exceeded its timeout.
    Timeout,
}

impl RunStatus {
    /// Check if the run is in a terminal state.
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Success | Self::Failed | Self::Canceled | Self::Timeout
        )
    }

    /// Check if the run is currently active.
    pub const fn is_active(&self) -> bool {
        matches!(self, Self::Queued | Self::Claimed | Self::Running)
    }
}

impl std::fmt::Display for RunStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Queued => write!(f, "queued"),
            Self::Claimed => write!(f, "claimed"),
            Self::Running => write!(f, "running"),
            Self::Success => write!(f, "success"),
            Self::Failed => write!(f, "failed"),
            Self::Canceled => write!(f, "canceled"),
            Self::Timeout => write!(f, "timeout"),
        }
    }
}

/// An execution instance of a scheduled job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Run {
    /// Unique identifier (UUID).
    pub run_id: String,

    /// Job that spawned this run (None for ad-hoc child runs).
    pub job_id: Option<String>,

    /// Script being executed.
    pub script_name: String,

    // Parent/child lineage
    /// Parent run that spawned this run (for child runs).
    pub parent_run_id: Option<String>,

    /// Root run in the lineage tree.
    pub root_run_id: Option<String>,

    /// Index among siblings (for child runs).
    pub child_index: Option<i32>,

    // Timing
    /// When the run was scheduled to execute.
    pub scheduled_for: DateTime<Utc>,

    /// When execution actually started.
    pub started_at: Option<DateTime<Utc>>,

    /// When execution finished.
    pub finished_at: Option<DateTime<Utc>>,

    /// Execution duration in milliseconds.
    pub duration_ms: Option<i64>,

    // Status
    /// Current status of the run.
    pub status: RunStatus,

    /// Retry attempt number (1 for first attempt).
    pub attempt: i32,

    // Distributed-mode fields
    /// Instance ID that executed this run.
    pub instance_id: Option<String>,

    /// Placement information.
    pub placement_json: Option<Value>,

    /// Target execution pool (distributed mode). `None` in DB means the `"default"` pool.
    pub pool_id: Option<String>,

    // Identity for identity reconstruction
    /// Serialized Actor for identity reconstruction.
    pub actor_json: Value,

    /// Parameters passed to the script.
    pub params_json: Value,

    // Output
    /// Captured stdout from the script.
    pub stdout_text: Option<String>,

    /// Captured stderr from the script.
    pub stderr_text: Option<String>,

    /// Error details if the run failed.
    pub error_json: Option<Value>,

    /// Execution statistics.
    pub stats_json: Option<Value>,

    /// Worker instance id while `status` is `claimed` / renewed during execution.
    pub claimed_by: Option<String>,

    /// Worker lease expiry for reclaim / failover.
    pub claim_lease_until: Option<DateTime<Utc>>,
}

/// Position of a run in the newest-first run listing, used as a keyset cursor.
///
/// Runs are ordered by `scheduled_for` descending, then `run_id` descending. Pass the key of
/// the last run on one page as `after` to
/// [`SchedulerStore::list_runs_after`](crate::store::SchedulerStore::list_runs_after) to read
/// the next page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunKey {
    /// [`Run::scheduled_for`] of the run this key points at.
    pub scheduled_for: DateTime<Utc>,
    /// [`Run::run_id`] of the run this key points at. Breaks ties between equal timestamps.
    pub run_id: String,
}

impl From<&Run> for RunKey {
    fn from(run: &Run) -> Self {
        Self {
            scheduled_for: run.scheduled_for,
            run_id: run.run_id.clone(),
        }
    }
}

impl Run {
    /// Create a queued run with generated IDs and empty runtime fields.
    ///
    /// The run starts in `RunStatus::Queued`. Timestamps, duration, and error
    /// details are populated later by `start`, `complete`, or `fail`.
    pub fn new(script_name: impl Into<String>, scheduled_for: DateTime<Utc>) -> Self {
        Self {
            run_id: uuid::Uuid::new_v4().to_string(),
            job_id: None,
            script_name: script_name.into(),
            parent_run_id: None,
            root_run_id: None,
            child_index: None,
            scheduled_for,
            started_at: None,
            finished_at: None,
            duration_ms: None,
            status: RunStatus::Queued,
            attempt: 1,
            instance_id: None,
            placement_json: None,
            pool_id: None,
            actor_json: Value::Null,
            params_json: Value::Object(serde_json::Map::default()),
            stdout_text: None,
            stderr_text: None,
            error_json: None,
            stats_json: None,
            claimed_by: None,
            claim_lease_until: None,
        }
    }

    /// Create a run linked to a specific job.
    pub fn for_job(
        job_id: impl Into<String>,
        script_name: impl Into<String>,
        scheduled_for: DateTime<Utc>,
    ) -> Self {
        let mut run = Self::new(script_name, scheduled_for);
        run.job_id = Some(job_id.into());
        run
    }

    /// Mark the run as started.
    pub fn start(&mut self) {
        self.started_at = Some(Utc::now());
        self.status = RunStatus::Running;
    }

    /// Mark the run as successfully completed.
    pub fn complete(&mut self) {
        let finished = Utc::now();
        self.finished_at = Some(finished);
        self.status = RunStatus::Success;
        if let Some(started) = self.started_at {
            self.duration_ms = Some((finished - started).num_milliseconds());
        }
    }

    /// Mark the run as failed.
    pub fn fail(&mut self, error: impl Into<String>) {
        let finished = Utc::now();
        self.finished_at = Some(finished);
        self.status = RunStatus::Failed;
        self.error_json = Some(serde_json::json!({ "message": error.into() }));
        if let Some(started) = self.started_at {
            self.duration_ms = Some((finished - started).num_milliseconds());
        }
    }

    /// Mark the run as timed out.
    pub fn timeout(&mut self, error: impl Into<String>) {
        let finished = Utc::now();
        self.finished_at = Some(finished);
        self.status = RunStatus::Timeout;
        self.error_json = Some(serde_json::json!({ "message": error.into() }));
        if let Some(started) = self.started_at {
            self.duration_ms = Some((finished - started).num_milliseconds());
        }
    }
}
