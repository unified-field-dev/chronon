//! Generic HTTP client for remote coordinator access.

use std::time::Duration;

use chronon_core::models::{Job, ScheduleKind};
use chronon_core::{ChrononError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const DEFAULT_TIMEOUT_MS: u64 = 3000;

fn trim_slash(s: &str) -> String {
    s.trim_end_matches('/').to_string()
}

fn remote_timeout() -> Duration {
    let ms = std::env::var("CHRONON_REMOTE_HTTP_TIMEOUT_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_TIMEOUT_MS);
    Duration::from_millis(ms.max(100))
}

fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(remote_timeout())
        .build()
        .unwrap_or_else(|_| reqwest::Client::new())
}

#[derive(Debug, Serialize, Deserialize)]
struct ApiResponse<T> {
    success: bool,
    data: Option<T>,
    error: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct UpsertJobRequest {
    job_name: String,
    script_name: String,
    cron_expr: Option<String>,
    timezone: Option<String>,
    #[serde(default)]
    schedule_kind: ScheduleKindDto,
    #[serde(default)]
    params: Value,
    #[serde(default = "default_true")]
    enabled: bool,
    #[serde(default = "default_concurrency")]
    concurrency: i32,
    pub timeout_ms: Option<i64>,
    #[serde(default)]
    actor_json: Option<Value>,
    #[serde(default)]
    retry_policy: Option<Value>,
    #[serde(default)]
    misfire_policy: Option<Value>,
    #[serde(default)]
    pool: Option<String>,
}

fn default_true() -> bool {
    true
}

fn default_concurrency() -> i32 {
    1
}

#[derive(Debug, Default, Deserialize, Serialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
enum ScheduleKindDto {
    #[default]
    Cron,
    RunOnce,
    Manual,
}

impl From<ScheduleKind> for ScheduleKindDto {
    fn from(k: ScheduleKind) -> Self {
        match k {
            ScheduleKind::Cron => Self::Cron,
            ScheduleKind::RunOnce => Self::RunOnce,
            ScheduleKind::Manual => Self::Manual,
        }
    }
}

/// Non-sensitive job summary returned by remote `GET /jobs` (matches Axum `JobResponse`).
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct JobSummary {
    /// Stable job identifier (UUID).
    pub job_id: String,
    /// Human-readable name.
    pub job_name: String,
    /// Bound script name.
    pub script_name: String,
    /// Whether scheduling is active.
    pub enabled: bool,
    /// Schedule mode as snake_case string (`cron`, `run_once`, `manual`).
    pub schedule_kind: String,
    /// Cron string when applicable.
    pub cron_expr: Option<String>,
    /// Timezone used for cron.
    pub timezone: Option<String>,
    /// Next scheduled fire time (RFC3339) when known.
    pub next_run_at: Option<String>,
    /// Monotonic revision counter.
    pub current_revision: i32,
    /// Creation timestamp (RFC3339).
    pub created_at: String,
    /// Last upsert timestamp (RFC3339).
    pub updated_at: String,
    /// Worker pool runs are claimed from; `None` means the scheduler default (`general`).
    /// Older coordinators omit the field.
    #[serde(default)]
    pub pool: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct JobResponse {
    job_id: String,
    job_name: String,
    script_name: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct JobActionRequest {
    job_id: String,
    #[serde(default)]
    params: Option<Value>,
}

/// HTTP client for the remote HTTP client topology — schedule/trigger jobs without local
/// Chronon loops.
///
/// Targets the `chronon-axum` API at `{base_url}/api/chronon/*` (see `API_PREFIX`).
/// Pair with a host that mounts `chronon_router` on an embedded or coordinator–worker process.
///
/// | Method | HTTP |
/// |--------|------|
/// | [`Self::upsert_job`] | `POST /jobs/upsert` |
/// | [`Self::list_jobs`] | `GET /jobs` |
/// | [`Self::run_now`] | `POST /jobs/run_now` |
///
/// Timeout: `CHRONON_REMOTE_HTTP_TIMEOUT_MS` (default 3000, minimum 100). Base URL helper:
/// [`resolve_remote_base_url`]. Optional builder mark: [`crate::ChrononBuilder::remote_coordinator`]
/// ([`DeploymentShape::RemoteClient`](crate::DeploymentShape::RemoteClient)) so you do not call
/// [`crate::Chronon::run`] by mistake.
///
/// # Examples
///
/// ```no_run
/// use chronon_core::{Job, ScheduleKind};
/// use chronon_runtime::{resolve_remote_base_url, RemoteCoordinatorClient};
///
/// # async fn demo() -> chronon_core::Result<()> {
/// let base = resolve_remote_base_url()
///     .unwrap_or_else(|| "http://127.0.0.1:8080".into());
/// let client = RemoteCoordinatorClient::new(base);
///
/// let mut job = Job::new("remote-job", "nightly_cleanup");
/// job.schedule_kind = ScheduleKind::Manual;
/// client.upsert_job(job.clone()).await?;
/// let _run_id = client.run_now(&job.job_id).await?;
/// let _jobs = client.list_jobs().await?;
/// # Ok(())
/// # }
/// ```
///
/// Runnable: `cargo run -p uf-chronon --example remote_http_client --features mem,axum`
/// (API mount sketches: `axum_host`, `axum_auth_wrap`).
pub struct RemoteCoordinatorClient {
    base_url: String,
    client: reqwest::Client,
}

impl RemoteCoordinatorClient {
    /// Create a client for `base_url` (trailing slashes stripped).
    ///
    /// Timeout from `CHRONON_REMOTE_HTTP_TIMEOUT_MS` (default 3s).
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: trim_slash(&base_url.into()),
            client: http_client(),
        }
    }

    fn api_url(&self, path: &str) -> String {
        format!("{}/api/chronon{}", self.base_url, path)
    }

    async fn parse_response<T: for<'de> Deserialize<'de>>(resp: reqwest::Response) -> Result<T> {
        let status = resp.status();
        let body: ApiResponse<T> = resp
            .json()
            .await
            .map_err(|e| ChrononError::internal_source("remote decode", e))?;
        if !status.is_success() || !body.success {
            return Err(ChrononError::Internal(
                body.error.unwrap_or_else(|| format!("HTTP {status}")),
            ));
        }
        body.data
            .ok_or_else(|| ChrononError::Internal("remote empty data".into()))
    }

    /// POST `/jobs/upsert` with fields from `job`.
    ///
    /// Always sends `pool`; `None` goes out as an empty string so the coordinator
    /// resets the job to the default pool instead of keeping a stale one.
    pub async fn upsert_job(&self, job: Job) -> Result<()> {
        let req = UpsertJobRequest {
            job_name: job.job_name,
            script_name: job.script_name,
            cron_expr: job.cron_expr,
            timezone: job.timezone,
            schedule_kind: job.schedule_kind.into(),
            params: job.params_json,
            enabled: job.enabled,
            concurrency: job.concurrency,
            timeout_ms: job.timeout_ms,
            actor_json: Some(job.actor_json),
            retry_policy: Some(job.retry_policy_json),
            misfire_policy: Some(job.misfire_policy_json),
            pool: Some(job.pool.unwrap_or_default()),
        };
        let _: JobResponse = Self::parse_response(
            self.client
                .post(self.api_url("/jobs/upsert"))
                .json(&req)
                .send()
                .await
                .map_err(|e| ChrononError::internal_source("remote upsert_job", e))?,
        )
        .await?;
        Ok(())
    }

    /// GET `/jobs` and return non-sensitive job summaries.
    pub async fn list_jobs(&self) -> Result<Vec<JobSummary>> {
        Self::parse_response(
            self.client
                .get(self.api_url("/jobs"))
                .send()
                .await
                .map_err(|e| ChrononError::internal_source("remote list_jobs", e))?,
        )
        .await
    }

    /// POST `/jobs/run_now` and return the enqueued `run_id`.
    pub async fn run_now(&self, job_id: &str) -> Result<String> {
        let req = JobActionRequest {
            job_id: job_id.to_string(),
            params: None,
        };
        let resp: String = Self::parse_response(
            self.client
                .post(self.api_url("/jobs/run_now"))
                .json(&req)
                .send()
                .await
                .map_err(|e| ChrononError::internal_source("remote run_now", e))?,
        )
        .await?;
        Ok(resp)
    }
}

/// Resolve remote API base URL from `CHRONON_REMOTE_BASE_URL`.
///
/// Returns [`None`] when unset or whitespace-only. Non-empty values are trimmed and
/// trailing `/` is stripped — pass the result to [`RemoteCoordinatorClient::new`] or
/// [`crate::ChrononBuilder::remote_coordinator`].
///
/// # Examples
///
/// ```
/// use chronon_runtime::{resolve_remote_base_url, RemoteCoordinatorClient};
///
/// let base = resolve_remote_base_url()
///     .unwrap_or_else(|| "http://127.0.0.1:8080".into());
/// let client = RemoteCoordinatorClient::new(base);
/// let _ = client; // call upsert_job / run_now / list_jobs against a live API host
/// ```
pub fn resolve_remote_base_url() -> Option<String> {
    if let Ok(u) = std::env::var("CHRONON_REMOTE_BASE_URL") {
        let t = u.trim();
        if !t.is_empty() {
            return Some(trim_slash(t));
        }
    }
    None
}
