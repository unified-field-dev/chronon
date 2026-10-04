//! JSON request and response types for the Chronon HTTP API.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use chronon_core::{Job, Run, ScheduleKind};

/// Body for `POST /jobs/upsert`.
///
/// Creates a new job or updates fields on an existing job matched by `job_name`.
#[derive(Debug, Deserialize, Serialize)]
pub struct UpsertJobRequest {
    /// Unique display name; used to locate an existing job on upsert.
    pub job_name: String,
    /// Registered script name; must exist in the host [`ScriptRegistry`](chronon_executor::ScriptRegistry).
    pub script_name: String,
    /// Cron expression when `schedule_kind` is [`ScheduleKindDto::Cron`].
    pub cron_expr: Option<String>,
    /// IANA timezone for cron evaluation; defaults to UTC when omitted.
    pub timezone: Option<String>,
    /// Scheduling mode; defaults to [`ScheduleKindDto::Cron`].
    #[serde(default)]
    pub schedule_kind: ScheduleKindDto,
    /// Script parameters JSON; defaults to `{}`.
    #[serde(default)]
    pub params: Value,
    /// Whether the scheduler should enqueue runs; defaults to `true`.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Max concurrent runs for this job; defaults to `1`.
    #[serde(default = "default_concurrency")]
    pub concurrency: i32,
    /// Per-run timeout in milliseconds; `None` uses executor defaults.
    pub timeout_ms: Option<i64>,
    /// Actor/session JSON passed to [`ScriptContext`](chronon_core::ScriptContext); omitted fields are ignored.
    #[serde(default)]
    pub actor_json: Option<Value>,
    /// Optional [`chronon_core::RetryPolicy`] JSON object.
    #[serde(default)]
    pub retry_policy: Option<Value>,
    /// Optional [`chronon_core::MisfirePolicy`] JSON object.
    #[serde(default)]
    pub misfire_policy: Option<Value>,
    /// Worker pool the job's runs are claimed from. Omitted keeps the stored pool;
    /// an empty string clears it back to the scheduler default (`general`).
    #[serde(default)]
    pub pool: Option<String>,
}

fn default_true() -> bool {
    true
}

fn default_concurrency() -> i32 {
    1
}

/// Wire format for [`ScheduleKind`](chronon_core::ScheduleKind) in JSON (`snake_case`).
#[derive(Debug, Default, Deserialize, Serialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleKindDto {
    /// Recurring cron schedule (default).
    #[default]
    Cron,
    /// Fire once on next tick after upsert.
    RunOnce,
    /// Only runs triggered via API (`run_now`).
    Manual,
}

impl From<ScheduleKindDto> for ScheduleKind {
    fn from(dto: ScheduleKindDto) -> Self {
        match dto {
            ScheduleKindDto::Cron => Self::Cron,
            ScheduleKindDto::RunOnce => Self::RunOnce,
            ScheduleKindDto::Manual => Self::Manual,
        }
    }
}

impl From<ScheduleKind> for ScheduleKindDto {
    fn from(kind: ScheduleKind) -> Self {
        match kind {
            ScheduleKind::Cron => Self::Cron,
            ScheduleKind::RunOnce => Self::RunOnce,
            ScheduleKind::Manual => Self::Manual,
        }
    }
}

/// Job summary returned by list/get/upsert endpoints.
#[derive(Debug, Serialize, Deserialize)]
pub struct JobResponse {
    /// Stable job identifier (UUID).
    pub job_id: String,
    /// Human-readable name from upsert.
    pub job_name: String,
    /// Bound script name.
    pub script_name: String,
    /// Whether scheduling is active.
    pub enabled: bool,
    /// Current schedule mode.
    pub schedule_kind: ScheduleKindDto,
    /// Cron string when applicable.
    pub cron_expr: Option<String>,
    /// Timezone used for cron.
    pub timezone: Option<String>,
    /// Next scheduled fire time (RFC3339) when known.
    pub next_run_at: Option<String>,
    /// Monotonic revision counter bumped on material changes.
    pub current_revision: i32,
    /// Creation timestamp (RFC3339).
    pub created_at: String,
    /// Last upsert timestamp (RFC3339).
    pub updated_at: String,
    /// Worker pool runs are claimed from; `None` means the scheduler default (`general`).
    #[serde(default)]
    pub pool: Option<String>,
}

impl From<Job> for JobResponse {
    fn from(job: Job) -> Self {
        Self {
            job_id: job.job_id,
            job_name: job.job_name,
            script_name: job.script_name,
            enabled: job.enabled,
            schedule_kind: job.schedule_kind.into(),
            cron_expr: job.cron_expr,
            timezone: job.timezone,
            next_run_at: job.next_run_at.map(|t| t.to_rfc3339()),
            current_revision: job.current_revision,
            created_at: job.created_at.to_rfc3339(),
            updated_at: job.updated_at.to_rfc3339(),
            pool: job.pool,
        }
    }
}

/// Body for job actions: pause, resume, and run-now.
#[derive(Debug, Deserialize)]
pub struct JobActionRequest {
    /// Target job id (not job name).
    pub job_id: String,
    /// Optional params override for `run_now`; omitted uses the job's stored params.
    #[serde(default)]
    pub params: Option<Value>,
}

/// Run summary for list/get run endpoints.
#[derive(Debug, Serialize, Deserialize)]
pub struct RunResponse {
    /// Unique run identifier.
    pub run_id: String,
    /// Parent job id when linked.
    pub job_id: Option<String>,
    /// Script executed for this run.
    pub script_name: String,
    /// Lowercase status string (`queued`, `running`, `success`, etc.).
    pub status: String,
    /// When the run was scheduled (RFC3339).
    pub scheduled_for: String,
    /// Execution start (RFC3339) when started.
    pub started_at: Option<String>,
    /// Completion time (RFC3339) when terminal.
    pub finished_at: Option<String>,
    /// Wall-clock duration in milliseconds when finished.
    pub duration_ms: Option<i64>,
    /// Attempt number for retries.
    pub attempt: i32,
    /// Pool the run was queued in.
    #[serde(default)]
    pub pool_id: Option<String>,
    /// Worker that claimed the run (`{instance}:{pool}:{slot}`).
    #[serde(default)]
    pub claimed_by: Option<String>,
}

impl From<Run> for RunResponse {
    fn from(run: Run) -> Self {
        Self {
            run_id: run.run_id,
            job_id: run.job_id,
            script_name: run.script_name,
            status: run.status.to_string(),
            scheduled_for: run.scheduled_for.to_rfc3339(),
            started_at: run.started_at.map(|t| t.to_rfc3339()),
            finished_at: run.finished_at.map(|t| t.to_rfc3339()),
            duration_ms: run.duration_ms,
            attempt: run.attempt,
            pool_id: run.pool_id,
            claimed_by: run.claimed_by,
        }
    }
}

/// Registered script metadata from `GET /scripts`.
#[derive(Debug, Serialize, Deserialize)]
pub struct ScriptResponse {
    /// Script name used in job definitions.
    pub name: String,
    /// JSON description of handler parameters.
    pub signature_json: String,
    /// Stable hash of `signature_json` for change detection.
    pub signature_hash: u64,
}

/// Query params for `GET /jobs`.
#[derive(Debug, Default, Deserialize)]
pub struct ListJobsQuery {
    /// Exact job name match.
    pub job_name: Option<String>,
    /// Exact script name match.
    pub script_name: Option<String>,
    /// Filter by enabled flag (`true` / `false`).
    pub enabled: Option<bool>,
    /// Filter by schedule kind (`cron`, `run_once`, `manual`).
    pub schedule_kind: Option<String>,
    /// Pagination offset; defaults to 0.
    pub offset: Option<usize>,
    /// Page size; defaults to 100, capped at 1000.
    pub limit: Option<usize>,
}

/// Query params for `GET /runs`.
#[derive(Debug, Default, Deserialize)]
pub struct ListRunsQuery {
    /// Filter by parent job id.
    pub job_id: Option<String>,
    /// Filter by status string (case-insensitive).
    pub status: Option<String>,
    /// Pagination offset; defaults to 0 in handlers.
    pub offset: Option<usize>,
    /// Page size; defaults to 100, capped at [`chronon_core::MAX_LIST_LIMIT`] (1000).
    pub limit: Option<usize>,
}
