//! Axum route handlers for `/api/chronon/*`.
//!
//! All routes extract [`RequireAdmin`]. HTTP upsert rejects System-shaped `actor_json`
//! via [`RejectExternalSystemActor`] (`EnqueueTrust::External`). In-process
//! [`CoordinatorService`](chronon_runtime::CoordinatorService) upsert may still set System.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};

use chronon_core::{
    ActorJsonPolicy, ChrononError, EnqueueTrust, Job, RejectExternalSystemActor, ScheduleKind,
    MAX_LIST_LIMIT,
};

use crate::auth::RequireAdmin;
use crate::dto::{
    JobActionRequest, JobResponse, ListJobsQuery, ListRunsQuery, RunResponse, UpsertJobRequest,
};
use crate::handlers_common::{chronon_err, ApiResponse};
use crate::state::ChrononState;
use chronon_scheduler::CronExpr;

/// Clamp optional list `limit` query to [`MAX_LIST_LIMIT`] (default 100).
#[must_use]
pub(crate) fn clamp_list_limit(limit: Option<usize>) -> usize {
    limit.unwrap_or(100).min(MAX_LIST_LIMIT)
}

/// Longest pool id accepted on upsert.
const MAX_POOL_CHARS: usize = 128;

/// Trimmed pool for [`Job::pool`]; empty clears it to the scheduler default.
///
/// Pool ids become queue key segments, so only `[A-Za-z0-9._/-]` is accepted.
fn normalize_pool(raw: &str) -> Result<Option<String>, String> {
    let pool = raw.trim();
    if pool.is_empty() {
        return Ok(None);
    }
    let safe = pool.len() <= MAX_POOL_CHARS
        && pool
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '/' | '-'));
    if safe {
        Ok(Some(pool.to_string()))
    } else {
        Err(format!(
            "Invalid pool: use at most {MAX_POOL_CHARS} characters from [A-Za-z0-9._/-]"
        ))
    }
}

/// `POST /jobs/upsert` — create or update a job matched by `job_name`; 400 if script missing,
/// cron invalid, or `pool` malformed.
///
/// When a job with the same `job_name` already exists, its `job_id` and `created_at` are preserved and
/// `current_revision` is bumped. Concurrency, timeout, and retry policy values are clamped to
/// [`chronon_core::MAX_JOB_CONCURRENCY`] / [`chronon_core::MAX_TIMEOUT_MS`] / retry ceilings.
#[tracing::instrument(skip(_admin, state, req), fields(job_name = %req.job_name, script_name = %req.script_name))]
pub async fn upsert_job(
    _admin: RequireAdmin,
    State(state): State<ChrononState>,
    Json(req): Json<UpsertJobRequest>,
) -> (StatusCode, Json<ApiResponse<JobResponse>>) {
    if !state.registry.contains(&req.script_name) {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::err(format!(
                "Script '{}' not found",
                req.script_name
            ))),
        );
    }

    let mut job = match state.coordinator.get_job_by_name(&req.job_name).await {
        Some(existing) => {
            let mut job = existing;
            job.script_name.clone_from(&req.script_name);
            job.current_revision = job.current_revision.saturating_add(1);
            job
        }
        None => Job::new(&req.job_name, &req.script_name),
    };
    job.enabled = req.enabled;
    job.schedule_kind = req.schedule_kind.into();
    job.cron_expr = req.cron_expr.clone();
    job.timezone = req.timezone.clone();
    job.params_json = req.params.clone();
    job.concurrency = req.concurrency;
    job.timeout_ms = req.timeout_ms;
    if let Some(ref actor) = req.actor_json {
        if !actor.is_null() {
            if let Err(e) = RejectExternalSystemActor.validate(EnqueueTrust::External, actor) {
                return chronon_err(&e);
            }
            job.actor_json = actor.clone();
        }
    }
    if let Some(ref policy) = req.retry_policy {
        if !policy.is_null() {
            job.retry_policy_json = policy.clone();
        }
    }
    if let Some(ref policy) = req.misfire_policy {
        if !policy.is_null() {
            job.misfire_policy_json = policy.clone();
        }
    }
    if let Some(ref pool) = req.pool {
        match normalize_pool(pool) {
            Ok(pool) => job.pool = pool,
            Err(msg) => {
                return (StatusCode::BAD_REQUEST, Json(ApiResponse::err(msg)));
            }
        }
    }
    job.clamp_security_bounds();

    if job.schedule_kind == ScheduleKind::Cron {
        if let Some(ref cron_expr) = job.cron_expr {
            match CronExpr::parse(cron_expr, job.timezone.as_deref()) {
                Ok(cron) => job.next_run_at = cron.next_from_now(),
                Err(e) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(ApiResponse::err(format!("Invalid cron expression: {e}"))),
                    );
                }
            }
        }
    }

    match state.coordinator.upsert_job(job.clone()).await {
        Ok(()) => (StatusCode::OK, Json(ApiResponse::ok(job.into()))),
        Err(e) => chronon_err(&e),
    }
}

/// `GET /jobs` — list jobs with optional filters and pagination.
pub async fn list_jobs(
    _admin: RequireAdmin,
    State(state): State<ChrononState>,
    Query(query): Query<ListJobsQuery>,
) -> (StatusCode, Json<ApiResponse<Vec<JobResponse>>>) {
    let jobs = match state.coordinator.list_jobs().await {
        Ok(jobs) => jobs,
        Err(e) => return chronon_err(&e),
    };

    if let Some(ref kind) = query.schedule_kind {
        let ok = matches!(kind.as_str(), "cron" | "run_once" | "manual");
        if !ok {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::err(format!(
                    "invalid schedule_kind '{kind}' (expected cron, run_once, or manual)"
                ))),
            );
        }
    }

    let mut filtered: Vec<Job> = jobs
        .into_iter()
        .filter(|j| {
            query.job_name.as_ref().is_none_or(|n| j.job_name == *n)
                && query
                    .script_name
                    .as_ref()
                    .is_none_or(|n| j.script_name == *n)
                && query.enabled.is_none_or(|e| j.enabled == e)
                && query
                    .schedule_kind
                    .as_ref()
                    .is_none_or(|k| match j.schedule_kind {
                        ScheduleKind::Cron => k == "cron",
                        ScheduleKind::RunOnce => k == "run_once",
                        ScheduleKind::Manual => k == "manual",
                    })
        })
        .collect();

    let offset = query.offset.unwrap_or(0);
    let limit = clamp_list_limit(query.limit);
    if offset >= filtered.len() {
        filtered.clear();
    } else {
        let end = (offset + limit).min(filtered.len());
        filtered = filtered[offset..end].to_vec();
    }

    let responses: Vec<JobResponse> = filtered.into_iter().map(Into::into).collect();
    (StatusCode::OK, Json(ApiResponse::ok(responses)))
}

/// `GET /jobs/{id}` — fetch one job by id; 404 when missing.
pub async fn get_job(
    _admin: RequireAdmin,
    State(state): State<ChrononState>,
    Path(job_id): Path<String>,
) -> (StatusCode, Json<ApiResponse<JobResponse>>) {
    match state.coordinator.get_job(&job_id).await {
        Some(job) => (StatusCode::OK, Json(ApiResponse::ok(job.into()))),
        None => chronon_err(&ChrononError::JobNotFound(job_id)),
    }
}

/// `POST /jobs/pause` — disable scheduling for a job.
pub async fn pause_job(
    _admin: RequireAdmin,
    State(state): State<ChrononState>,
    Json(req): Json<JobActionRequest>,
) -> (StatusCode, Json<ApiResponse<()>>) {
    match state.coordinator.pause_job(&req.job_id).await {
        Ok(()) => (StatusCode::OK, Json(ApiResponse::ok(()))),
        Err(e) => chronon_err(&e),
    }
}

/// `POST /jobs/resume` — re-enable scheduling for a job.
pub async fn resume_job(
    _admin: RequireAdmin,
    State(state): State<ChrononState>,
    Json(req): Json<JobActionRequest>,
) -> (StatusCode, Json<ApiResponse<()>>) {
    match state.coordinator.resume_job(&req.job_id).await {
        Ok(()) => (StatusCode::OK, Json(ApiResponse::ok(()))),
        Err(e) => chronon_err(&e),
    }
}

/// `POST /jobs/run_now` — enqueue an immediate run; returns new `run_id` in `data`.
///
/// Actor identity comes from the job row (snapshotted onto the run). Clients cannot supply
/// `actor_json` on this route; set identity via upsert (external rejects System) or in-process.
#[tracing::instrument(skip(_admin, state, req), fields(job_id = %req.job_id))]
pub async fn run_now(
    _admin: RequireAdmin,
    State(state): State<ChrononState>,
    Json(req): Json<JobActionRequest>,
) -> (StatusCode, Json<ApiResponse<String>>) {
    match state
        .coordinator
        .run_now_with_params(&req.job_id, req.params)
        .await
    {
        Ok(run_id) => (StatusCode::OK, Json(ApiResponse::ok(run_id))),
        Err(e) => chronon_err(&e),
    }
}

/// `GET /jobs/{id}/revisions` — revision history as JSON objects.
///
/// Sensitive fields (`changed_by_actor_json`, and `actor_json` / `params_json` inside
/// `snapshot_json`) are redacted in the HTTP response. Full snapshots remain in the store for
/// host admin tooling.
pub async fn get_job_revisions(
    _admin: RequireAdmin,
    State(state): State<ChrononState>,
    Path(job_id): Path<String>,
) -> (StatusCode, Json<ApiResponse<Vec<serde_json::Value>>>) {
    if state.coordinator.get_job(&job_id).await.is_none() {
        return chronon_err(&ChrononError::JobNotFound(job_id));
    }
    match state.coordinator.list_revisions(&job_id).await {
        Ok(revisions) => {
            let json_revisions: Vec<serde_json::Value> = revisions
                .into_iter()
                .map(|r| {
                    serde_json::json!({
                        "revision_id": r.revision_id,
                        "revision_number": r.revision_number,
                        "changed_at": r.changed_at.to_rfc3339(),
                        "changed_by_actor_json": serde_json::Value::Null,
                        "snapshot_json": redact_revision_snapshot(r.snapshot_json),
                    })
                })
                .collect();
            (StatusCode::OK, Json(ApiResponse::ok(json_revisions)))
        }
        Err(e) => chronon_err(&e),
    }
}

fn redact_revision_snapshot(mut snapshot: serde_json::Value) -> serde_json::Value {
    if let Some(obj) = snapshot.as_object_mut() {
        obj.insert("actor_json".into(), serde_json::Value::Null);
        obj.insert("params_json".into(), serde_json::Value::Null);
    }
    snapshot
}

/// `GET /runs` — paginated run list with optional filters.
pub async fn list_runs(
    _admin: RequireAdmin,
    State(state): State<ChrononState>,
    Query(query): Query<ListRunsQuery>,
) -> (StatusCode, Json<ApiResponse<Vec<RunResponse>>>) {
    let offset = query.offset.unwrap_or(0);
    let limit = clamp_list_limit(query.limit);
    match state
        .coordinator
        .list_runs(
            query.job_id.as_deref(),
            query.status.as_deref(),
            offset,
            limit,
        )
        .await
    {
        Ok(runs) => {
            let responses: Vec<RunResponse> = runs.into_iter().map(Into::into).collect();
            (StatusCode::OK, Json(ApiResponse::ok(responses)))
        }
        Err(e) => chronon_err(&e),
    }
}

/// `GET /runs/{id}` — fetch one run; 404 when missing.
pub async fn get_run(
    _admin: RequireAdmin,
    State(state): State<ChrononState>,
    Path(run_id): Path<String>,
) -> (StatusCode, Json<ApiResponse<RunResponse>>) {
    match state.coordinator.get_run(&run_id).await {
        Ok(Some(run)) => (StatusCode::OK, Json(ApiResponse::ok(run.into()))),
        Ok(None) => chronon_err(&ChrononError::RunNotFound(run_id)),
        Err(e) => chronon_err(&e),
    }
}

/// `GET /scripts` — list registered scripts from the host registry.
pub async fn list_scripts(
    _admin: RequireAdmin,
    State(state): State<ChrononState>,
) -> Json<ApiResponse<Vec<crate::dto::ScriptResponse>>> {
    let scripts: Vec<crate::dto::ScriptResponse> = state
        .registry
        .list()
        .into_iter()
        .map(|d| crate::dto::ScriptResponse {
            name: d.name.to_string(),
            signature_json: d.signature_json.to_string(),
            signature_hash: d.signature_hash,
        })
        .collect();
    Json(ApiResponse::ok(scripts))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use chronon_core::MAX_LIST_LIMIT;

    use super::clamp_list_limit;

    #[test]
    fn clamp_list_limit_defaults_and_caps() {
        assert_eq!(clamp_list_limit(None), 100);
        assert_eq!(clamp_list_limit(Some(50)), 50);
        assert_eq!(clamp_list_limit(Some(MAX_LIST_LIMIT)), MAX_LIST_LIMIT);
        assert_eq!(clamp_list_limit(Some(MAX_LIST_LIMIT + 1)), MAX_LIST_LIMIT);
        assert_eq!(clamp_list_limit(Some(1_000_000)), MAX_LIST_LIMIT);
    }
}
