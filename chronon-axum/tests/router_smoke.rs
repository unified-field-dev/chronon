//! Integration smoke tests for the Chronon Axum router.

#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use chronon_axum::{
    chronon_router, ApiResponse, ChrononState, JobResponse, RunResponse, ScriptResponse,
    StaticTokenAdminAuth,
};
use chronon_backend_mem::InMemorySchedulerStore;
use chronon_core::models::{Job, Run, RunStatus, ScheduleKind};
use chronon_core::{Result, ScriptContext};
use chronon_executor::{ScriptDescriptor, ScriptRegistry};
use chronon_runtime::CoordinatorService;
use http_body_util::BodyExt;
use tower::ServiceExt;

fn noop_invoke(
    _ctx: Box<dyn ScriptContext>,
    _params: serde_json::Value,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>> {
    Box::pin(async { Ok(()) })
}

#[derive(Clone)]
struct AppState {
    chronon: ChrononState,
}

impl axum::extract::FromRef<AppState> for ChrononState {
    fn from_ref(state: &AppState) -> Self {
        state.chronon.clone()
    }
}

fn test_state() -> AppState {
    let store = Arc::new(InMemorySchedulerStore::new());
    let coordinator = Arc::new(CoordinatorService::new(store));
    let registry = Arc::new({
        let mut r = ScriptRegistry::new();
        r.register(&ScriptDescriptor::new("test_script", noop_invoke));
        r
    });
    AppState {
        chronon: ChrononState::new(coordinator, registry),
    }
}

fn test_app(state: AppState) -> Router {
    chronon_router::<AppState>().with_state(state)
}

async fn json_body<T: serde::de::DeserializeOwned>(resp: axum::response::Response) -> T {
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&body).unwrap()
}

async fn seed_job(state: &AppState, job_name: &str) -> Job {
    let mut job = Job::new(job_name, "test_script");
    job.schedule_kind = ScheduleKind::Manual;
    state
        .chronon
        .coordinator
        .upsert_job(job.clone())
        .await
        .unwrap();
    state
        .chronon
        .coordinator
        .get_job_by_name(job_name)
        .await
        .expect("job stored")
}

async fn seed_run(state: &AppState, job: &Job) -> Run {
    let run_id = state
        .chronon
        .coordinator
        .run_now(&job.job_id)
        .await
        .unwrap();
    state
        .chronon
        .coordinator
        .get_run(&run_id)
        .await
        .unwrap()
        .expect("run stored")
}

#[tokio::test]
async fn upsert_job_ok() {
    let state = test_state();
    let app = test_app(state);
    let body = serde_json::json!({
        "job_name": "j1",
        "script_name": "test_script",
        "schedule_kind": "manual",
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/jobs/upsert")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let parsed: ApiResponse<JobResponse> = json_body(resp).await;
    assert!(parsed.success);
    assert_eq!(parsed.data.unwrap().job_name, "j1");
}

#[tokio::test]
async fn upsert_job_preserves_job_id_by_name() {
    let state = test_state();
    let app = test_app(state.clone());
    let body1 = serde_json::json!({
        "job_name": "stable-name",
        "script_name": "test_script",
        "schedule_kind": "manual",
        "enabled": true,
        "concurrency": 1,
    });
    let resp1 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/jobs/upsert")
                .header("content-type", "application/json")
                .body(Body::from(body1.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);
    let first: ApiResponse<JobResponse> = json_body(resp1).await;
    let job_id = first.data.as_ref().unwrap().job_id.clone();

    let body2 = serde_json::json!({
        "job_name": "stable-name",
        "script_name": "test_script",
        "schedule_kind": "manual",
        "enabled": false,
        "concurrency": 2,
    });
    let resp2 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/jobs/upsert")
                .header("content-type", "application/json")
                .body(Body::from(body2.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);
    let second: ApiResponse<JobResponse> = json_body(resp2).await;
    let updated = second.data.unwrap();
    assert_eq!(updated.job_id, job_id);
    assert!(!updated.enabled);
    assert_eq!(updated.current_revision, 2);

    let list = test_app(state)
        .oneshot(Request::builder().uri("/jobs").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let listed: ApiResponse<Vec<JobResponse>> = json_body(list).await;
    assert_eq!(listed.data.unwrap().len(), 1);
}

#[tokio::test]
async fn upsert_job_clamps_extreme_policy_knobs() {
    let state = test_state();
    let app = test_app(state.clone());
    let body = serde_json::json!({
        "job_name": "bounded",
        "script_name": "test_script",
        "schedule_kind": "manual",
        "concurrency": 9_999_999,
        "timeout_ms": 9_999_999_999_i64,
        "retry_policy": {
            "max_attempts": 9_999_999,
            "base_delay_ms": 9_999_999_999_u64,
            "backoff_multiplier": 2.0,
            "max_delay_ms": 9_999_999_999_u64
        }
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/jobs/upsert")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let parsed: ApiResponse<JobResponse> = json_body(resp).await;
    let job_id = parsed.data.unwrap().job_id;
    let job = state
        .chronon
        .coordinator
        .get_job(&job_id)
        .await
        .expect("job stored");
    assert_eq!(job.concurrency, chronon_core::MAX_JOB_CONCURRENCY);
    assert_eq!(job.timeout_ms, Some(chronon_core::MAX_TIMEOUT_MS));
    assert_eq!(
        job.retry_policy().max_attempts,
        chronon_core::MAX_RETRY_ATTEMPTS
    );
}

#[tokio::test]
async fn upsert_job_script_not_found() {
    let app = test_app(test_state());
    let body = serde_json::json!({
        "job_name": "j1",
        "script_name": "missing",
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/jobs/upsert")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let parsed: ApiResponse<JobResponse> = json_body(resp).await;
    assert!(!parsed.success);
    assert!(parsed.error.is_some());
}

#[tokio::test]
async fn upsert_job_invalid_cron() {
    let app = test_app(test_state());
    let body = serde_json::json!({
        "job_name": "j1",
        "script_name": "test_script",
        "cron_expr": "not-a-cron",
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/jobs/upsert")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn list_jobs_ok() {
    let state = test_state();
    seed_job(&state, "listed").await;
    let app = test_app(state);
    let resp = app
        .oneshot(Request::builder().uri("/jobs").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let parsed: ApiResponse<Vec<JobResponse>> = json_body(resp).await;
    assert!(parsed.success);
    assert_eq!(parsed.data.unwrap().len(), 1);
}

#[tokio::test]
async fn list_jobs_filters_and_rejects_bad_schedule_kind() {
    let state = test_state();
    seed_job(&state, "filter-a").await;
    let app = test_app(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/jobs?job_name=filter-a&enabled=true&limit=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let parsed: ApiResponse<Vec<JobResponse>> = json_body(resp).await;
    assert_eq!(parsed.data.unwrap().len(), 1);

    let app = test_app(state);
    let bad = app
        .oneshot(
            Request::builder()
                .uri("/jobs?schedule_kind=nope")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(bad.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn get_job_ok() {
    let state = test_state();
    let job = seed_job(&state, "fetch-me").await;
    let app = test_app(state);
    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/jobs/{}", job.job_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let parsed: ApiResponse<JobResponse> = json_body(resp).await;
    assert_eq!(parsed.data.unwrap().job_name, "fetch-me");
}

#[tokio::test]
async fn get_job_not_found() {
    let app = test_app(test_state());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/jobs/missing-id")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn pause_and_resume_job_ok() {
    let state = test_state();
    let job = seed_job(&state, "pausable").await;
    let app = test_app(state);
    let pause_body = serde_json::json!({ "job_id": job.job_id });
    let pause_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/jobs/pause")
                .header("content-type", "application/json")
                .body(Body::from(pause_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(pause_resp.status(), StatusCode::OK);

    let resume_body = serde_json::json!({ "job_id": job.job_id });
    let resume_resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/jobs/resume")
                .header("content-type", "application/json")
                .body(Body::from(resume_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resume_resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn run_now_ok() {
    let state = test_state();
    let job = seed_job(&state, "trigger").await;
    let app = test_app(state);
    let body = serde_json::json!({ "job_id": job.job_id });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/jobs/run_now")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let parsed: ApiResponse<String> = json_body(resp).await;
    assert_ne!(parsed.data.unwrap(), "");
}

#[tokio::test]
async fn run_now_job_not_found() {
    let app = test_app(test_state());
    let body = serde_json::json!({ "job_id": "missing-job" });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/jobs/run_now")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn get_job_revisions_ok() {
    let state = test_state();
    let job = seed_job(&state, "revisions").await;
    let app = test_app(state);
    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/jobs/{}/revisions", job.job_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let parsed: ApiResponse<Vec<serde_json::Value>> = json_body(resp).await;
    assert_ne!(parsed.data.unwrap(), [] as [serde_json::Value; 0]);
}

#[tokio::test]
async fn get_job_revisions_redacts_sensitive_fields() {
    let state = test_state();
    let app = test_app(state.clone());
    let body = serde_json::json!({
        "job_name": "secret-job",
        "script_name": "test_script",
        "schedule_kind": "manual",
        "params": { "token": "super-secret" },
        "actor_json": { "role": "admin", "session": "sess-1" },
    });
    let upsert = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/jobs/upsert")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let parsed: ApiResponse<JobResponse> = json_body(upsert).await;
    let job_id = parsed.data.unwrap().job_id;

    let resp = test_app(state.clone())
        .oneshot(
            Request::builder()
                .uri(format!("/jobs/{job_id}/revisions"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let revisions: ApiResponse<Vec<serde_json::Value>> = json_body(resp).await;
    let rev = &revisions.data.unwrap()[0];
    assert!(rev.get("changed_by_actor_json").unwrap().is_null());
    let snapshot = rev.get("snapshot_json").unwrap();
    assert!(snapshot.get("actor_json").unwrap().is_null());
    assert!(snapshot.get("params_json").unwrap().is_null());
    assert_eq!(
        snapshot.get("job_name").and_then(|v| v.as_str()),
        Some("secret-job")
    );

    let store_revs = state
        .chronon
        .coordinator
        .list_revisions(&job_id)
        .await
        .expect("store revisions");
    let store_snap = &store_revs[0].snapshot_json;
    assert_eq!(
        store_snap.get("actor_json"),
        Some(&serde_json::json!({ "role": "admin", "session": "sess-1" }))
    );
    assert_eq!(
        store_snap.get("params_json"),
        Some(&serde_json::json!({ "token": "super-secret" }))
    );
}

#[tokio::test]
async fn get_job_revisions_not_found() {
    let app = test_app(test_state());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/jobs/missing-job/revisions")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn list_runs_ok() {
    let state = test_state();
    let job = seed_job(&state, "runs-job").await;
    seed_run(&state, &job).await;
    let app = test_app(state);
    let resp = app
        .oneshot(Request::builder().uri("/runs").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let parsed: ApiResponse<Vec<RunResponse>> = json_body(resp).await;
    assert_eq!(parsed.data.unwrap().len(), 1);
}

#[tokio::test]
async fn list_runs_clamps_limit() {
    let state = test_state();
    let job = seed_job(&state, "runs-limit").await;
    for _ in 0..3 {
        seed_run(&state, &job).await;
    }
    let app = test_app(state);
    let over = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/runs?limit=1000000")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(over.status(), StatusCode::OK);
    let parsed: ApiResponse<Vec<RunResponse>> = json_body(over).await;
    // Cap is applied before the store query (`clamp_list_limit`); with fewer rows than the
    // cap we still return all three. Unit coverage of the exact cap lives on `clamp_list_limit`.
    assert_eq!(parsed.data.unwrap().len(), 3);

    let in_range = app
        .oneshot(
            Request::builder()
                .uri("/runs?limit=2")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(in_range.status(), StatusCode::OK);
    let parsed: ApiResponse<Vec<RunResponse>> = json_body(in_range).await;
    assert_eq!(parsed.data.unwrap().len(), 2);
}

#[tokio::test]
async fn get_run_ok() {
    let state = test_state();
    let job = seed_job(&state, "run-fetch").await;
    let run = seed_run(&state, &job).await;
    let app = test_app(state);
    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/runs/{}", run.run_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let parsed: ApiResponse<RunResponse> = json_body(resp).await;
    assert_eq!(parsed.data.unwrap().run_id, run.run_id);
}

#[tokio::test]
async fn get_run_not_found() {
    let app = test_app(test_state());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/runs/missing-run")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn list_scripts_ok() {
    let app = test_app(test_state());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/scripts")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let parsed: ApiResponse<Vec<ScriptResponse>> = json_body(resp).await;
    assert!(parsed.success);
    assert_eq!(parsed.data.unwrap().len(), 1);
}

#[tokio::test]
async fn list_runs_filtered_by_status() {
    let state = test_state();
    let job = seed_job(&state, "filter-job").await;
    let mut run = seed_run(&state, &job).await;
    run.status = RunStatus::Success;
    state
        .chronon
        .coordinator
        .store()
        .update_run(&run)
        .await
        .unwrap();
    let app = test_app(state);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/runs?status=success")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let parsed: ApiResponse<Vec<RunResponse>> = json_body(resp).await;
    assert_eq!(parsed.data.unwrap().len(), 1);
}

#[tokio::test]
async fn builder_requires_admin_auth_when_flagged() {
    let store = Arc::new(InMemorySchedulerStore::new());
    let coordinator = Arc::new(CoordinatorService::new(store));
    let registry = Arc::new({
        let mut r = ScriptRegistry::new();
        r.register(&ScriptDescriptor::new("test_script", noop_invoke));
        r
    });
    let result = ChrononState::builder(coordinator, registry)
        .require_admin_auth(true)
        .build();
    let Err(err) = result else {
        panic!("must refuse without verifier");
    };
    assert!(err.to_string().contains("AdminAuth"));
}

#[tokio::test]
async fn admin_auth_missing_token_401() {
    let store = Arc::new(InMemorySchedulerStore::new());
    let coordinator = Arc::new(CoordinatorService::new(store));
    let registry = Arc::new({
        let mut r = ScriptRegistry::new();
        r.register(&ScriptDescriptor::new("test_script", noop_invoke));
        r
    });
    let chronon = ChrononState::builder(coordinator, registry)
        .admin_auth(Arc::new(StaticTokenAdminAuth::new("secret")))
        .require_admin_auth(true)
        .build()
        .unwrap();
    let app = test_app(AppState { chronon });
    let denied = app
        .oneshot(
            Request::builder()
                .uri("/scripts")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn admin_auth_valid_token_200() {
    let store = Arc::new(InMemorySchedulerStore::new());
    let coordinator = Arc::new(CoordinatorService::new(store));
    let registry = Arc::new({
        let mut r = ScriptRegistry::new();
        r.register(&ScriptDescriptor::new("test_script", noop_invoke));
        r
    });
    let chronon = ChrononState::builder(coordinator, registry)
        .admin_auth(Arc::new(StaticTokenAdminAuth::new("secret")))
        .require_admin_auth(true)
        .build()
        .unwrap();
    let app = test_app(AppState { chronon });
    let allowed = app
        .oneshot(
            Request::builder()
                .uri("/scripts")
                .header("x-chronon-admin-token", "secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(allowed.status(), StatusCode::OK);
    let parsed: ApiResponse<Vec<ScriptResponse>> = json_body(allowed).await;
    assert!(parsed.success);
    assert_eq!(parsed.data.unwrap().len(), 1);
}

#[tokio::test]
async fn upsert_rejects_system_shaped_actor_json() {
    let app = test_app(test_state());
    let body = serde_json::json!({
        "job_name": "elevated",
        "script_name": "test_script",
        "schedule_kind": "manual",
        "actor_json": { "System": { "operation": "mint" } },
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/jobs/upsert")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let parsed: ApiResponse<JobResponse> = json_body(resp).await;
    assert!(!parsed.success);
    assert!(parsed.error.as_deref().unwrap_or("").contains("System"));
}

#[tokio::test]
async fn auth_middleware_rejects_without_bearer() {
    use axum::http::header;
    use axum::middleware::{self, Next};
    use axum::response::{IntoResponse, Response};

    const DEMO_TOKEN: &str = "demo-chronon-admin-token";

    async fn require_bearer(req: Request<Body>, next: Next) -> Response {
        let authorized = req
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v == format!("Bearer {DEMO_TOKEN}"));
        if authorized {
            next.run(req).await
        } else {
            StatusCode::UNAUTHORIZED.into_response()
        }
    }

    let state = test_state();
    let app = Router::new()
        .merge(chronon_router::<AppState>())
        .layer(middleware::from_fn(require_bearer))
        .with_state(state);

    let denied = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/scripts")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);

    let allowed = app
        .oneshot(
            Request::builder()
                .uri("/scripts")
                .header(header::AUTHORIZATION, format!("Bearer {DEMO_TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(allowed.status(), StatusCode::OK);
    let parsed: ApiResponse<Vec<ScriptResponse>> = json_body(allowed).await;
    assert!(parsed.success);
    assert_eq!(parsed.data.unwrap().len(), 1);
}

async fn upsert(app: &Router, body: serde_json::Value) -> axum::response::Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/jobs/upsert")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn upsert_job_pool_round_trips_happy_path() {
    let state = test_state();
    let app = test_app(state.clone());
    let base = serde_json::json!({
        "job_name": "pooled",
        "script_name": "test_script",
        "schedule_kind": "manual",
    });

    let mut with_pool = base.clone();
    with_pool["pool"] = serde_json::json!(" chronon-mve-a ");
    let resp = upsert(&app, with_pool).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let parsed: ApiResponse<JobResponse> = json_body(resp).await;
    let created = parsed.data.unwrap();
    assert_eq!(created.pool.as_deref(), Some("chronon-mve-a"));

    let resp = upsert(&app, base.clone()).await;
    let kept: ApiResponse<JobResponse> = json_body(resp).await;
    assert_eq!(
        kept.data.unwrap().pool.as_deref(),
        Some("chronon-mve-a"),
        "omitted pool keeps the stored one"
    );

    let job = state
        .chronon
        .coordinator
        .get_job(&created.job_id)
        .await
        .unwrap();
    let run = seed_run(&state, &job).await;
    let resp = test_app(state.clone())
        .oneshot(
            Request::builder()
                .uri(format!("/runs/{}", run.run_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let run: ApiResponse<RunResponse> = json_body(resp).await;
    assert_eq!(run.data.unwrap().pool_id.as_deref(), Some("chronon-mve-a"));

    let mut cleared = base;
    cleared["pool"] = serde_json::json!("");
    let resp = upsert(&app, cleared).await;
    let cleared: ApiResponse<JobResponse> = json_body(resp).await;
    assert_eq!(
        cleared.data.unwrap().pool,
        None,
        "empty pool resets to default"
    );
}

#[tokio::test]
async fn upsert_job_rejects_malformed_pool_sad() {
    let state = test_state();
    let app = test_app(state.clone());
    let long = "p".repeat(129);
    for bad in ["general:x", "{general}", "pool a", long.as_str()] {
        let resp = upsert(
            &app,
            serde_json::json!({
                "job_name": "bad-pool",
                "script_name": "test_script",
                "schedule_kind": "manual",
                "pool": bad,
            }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "{bad}");
        let parsed: ApiResponse<JobResponse> = json_body(resp).await;
        assert!(parsed.error.unwrap().starts_with("Invalid pool"));
    }
    assert!(
        state
            .chronon
            .coordinator
            .get_job_by_name("bad-pool")
            .await
            .is_none(),
        "rejected upsert stores nothing"
    );
}
