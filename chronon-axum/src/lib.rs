//! Axum HTTP API for Chronon (`/api/chronon/*`).
//!
//! Mounts job, run, and script routes on a host Axum server. Handlers delegate to
//! [`chronon_runtime::CoordinatorService`] and read script metadata from
//! [`chronon_executor::ScriptRegistry`].
//!
//! # Features
//!
//! - **AdminAuth** — host-supplied [`AdminAuth`] via [`ChrononStateBuilder`]; lab helpers
//!   [`StaticTokenAdminAuth`] / [`AllowAllAdminAuth`]. Production identity belongs to Higgs.
//! - **`CHRONON_REQUIRE_ADMIN_AUTH`** — when set, [`ChrononStateBuilder::build`] and
//!   [`RequireAdmin`] fail closed without a verifier.
//! - **External actor policy** — HTTP upsert rejects System- and ServiceUser-shaped
//!   `actor_json` ([`RejectExternalPrivilegedActor`](chronon_core::RejectExternalPrivilegedActor)).
//! - **Error hygiene** — HTTP envelopes sanitize/redact credentials in error strings.
//!
//! # Security
//!
//! Wrap-before-public-bind: nest under [`API_PREFIX`], install [`AdminAuth`] (or host middleware),
//! and set `CHRONON_REQUIRE_ADMIN_AUTH=1` before exposing the API. See repository `SECURITY.md`
//! and the `axum_auth_wrap` example.
//!
//! # Routes
//!
//! - `GET/POST /jobs/*` — list, upsert (by `job_name`), pause, resume, run now
//! - `GET /runs/*` — list (limit capped at 1000) and fetch runs
//! - `GET /scripts` — list registered scripts
//! - `GET /jobs/{id}/revisions` — revision metadata with actor/params redacted
//!
//! All responses use the [`ApiResponse`] envelope (`success`, `data`, `error`).
//! [`UpsertJobRequest::script_name`] must exist in the registry or upsert returns 400.
//! Concurrency, timeout, and retry knobs are clamped to production ceilings.
//!
//! # Remote HTTP clients
//!
//! Mount this router on an embedded or coordinator–worker host **behind host auth**, then point
//! [`chronon_runtime::RemoteCoordinatorClient`] at `{base_url}` (paths under
//! [`API_PREFIX`]). See the `chronon` crate [Remote HTTP client](https://docs.rs/uf-chronon/latest/chronon/index.html#remote-http-client) section.
//!
//! # Examples
//!
//! Completed setup with [`RequireAdmin`] / [`StaticTokenAdminAuth`]:
//!
//! ```
//! use std::sync::Arc;
//! use axum::extract::FromRef;
//! use axum::Router;
//! use chronon_axum::{
//!     chronon_router, ChrononState, StaticTokenAdminAuth, API_PREFIX,
//! };
//! use chronon_backend_mem::InMemorySchedulerStore;
//! use chronon_core::{Result as ChrononResult, ScriptContext};
//! use chronon_executor::{ScriptDescriptor, ScriptRegistry};
//! use chronon_runtime::CoordinatorService;
//!
//! fn noop(
//!     _ctx: Box<dyn ScriptContext>,
//!     _params: serde_json::Value,
//! ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ChrononResult<()>> + Send>> {
//!     Box::pin(async { Ok(()) })
//! }
//!
//! #[derive(Clone)]
//! struct AppState {
//!     chronon: ChrononState,
//! }
//!
//! impl FromRef<AppState> for ChrononState {
//!     fn from_ref(state: &AppState) -> Self {
//!         state.chronon.clone()
//!     }
//! }
//!
//! # fn mount() -> chronon_core::Result<Router<AppState>> {
//! let store = Arc::new(InMemorySchedulerStore::new());
//! let coordinator = Arc::new(CoordinatorService::new(store));
//! let registry = Arc::new({
//!     let mut r = ScriptRegistry::new();
//!     r.register(&ScriptDescriptor::new("demo", noop));
//!     r
//! });
//! let chronon = ChrononState::builder(coordinator, registry)
//!     .admin_auth(Arc::new(StaticTokenAdminAuth::new("lab-token")))
//!     .require_admin_auth(true)
//!     .build()?;
//! Ok(Router::new()
//!     .nest(API_PREFIX, chronon_router::<AppState>())
//!     .with_state(AppState { chronon }))
//! # }
//! ```
//!
//! Runnable:
//! `cargo run -p uf-chronon --example axum_host --features mem,axum`,
//! `axum_auth_wrap`, and
//! `remote_http_client` (client against a nested router).

mod auth;
mod dto;
mod handlers;
mod handlers_common;
mod state;

use axum::{
    extract::FromRef,
    routing::{get, post},
    Router,
};

pub use auth::{
    require_admin_auth_from_env, AdminAuth, AdminAuthError, AllowAllAdminAuth, RequireAdmin,
    StaticTokenAdminAuth, REQUIRE_ADMIN_AUTH_ENV,
};
pub use dto::{
    JobActionRequest, JobResponse, ListJobsQuery, ListRunsQuery, RunResponse, ScheduleKindDto,
    ScriptResponse, UpsertJobRequest,
};
pub use handlers_common::ApiResponse;
pub use state::{ChrononState, ChrononStateBuilder, HttpUpsertActorProvider};

/// API mount prefix for host routers (e.g. `nest(API_PREFIX, chronon_router())`).
pub const API_PREFIX: &str = "/api/chronon";

/// Create the Chronon API router with job, run, and script routes.
///
/// Host state `S` must implement [`FromRef<S>`] for [`ChrononState`]. Nest under
/// [`API_PREFIX`] (`/api/chronon`) so [`chronon_runtime::RemoteCoordinatorClient`] paths match.
///
/// Handlers extract [`RequireAdmin`]. Install [`AdminAuth`] on [`ChrononState`] (or set
/// `CHRONON_REQUIRE_ADMIN_AUTH=1` and fail closed) before public bind. See `SECURITY.md`.
///
/// See the crate-level example (RequireAdmin + StaticTokenAdminAuth).
pub fn chronon_router<S>() -> Router<S>
where
    S: Clone + Send + Sync + 'static,
    ChrononState: FromRef<S>,
{
    Router::new()
        .route("/jobs", get(handlers::list_jobs))
        .route("/jobs/upsert", post(handlers::upsert_job))
        .route("/jobs/pause", post(handlers::pause_job))
        .route("/jobs/resume", post(handlers::resume_job))
        .route("/jobs/run_now", post(handlers::run_now))
        .route("/jobs/{id}", get(handlers::get_job))
        .route("/jobs/{id}/revisions", get(handlers::get_job_revisions))
        .route("/runs", get(handlers::list_runs))
        .route("/runs/{id}", get(handlers::get_run))
        .route("/scripts", get(handlers::list_scripts))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::sync::Arc;

    use chronon_backend_mem::InMemorySchedulerStore;
    use chronon_core::{Result, ScriptContext};
    use chronon_executor::{ScriptDescriptor, ScriptRegistry};
    use chronon_runtime::CoordinatorService;

    use crate::ChrononState;

    fn noop(
        _ctx: Box<dyn ScriptContext>,
        _params: serde_json::Value,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>> {
        Box::pin(async { Ok(()) })
    }

    fn parts() -> (Arc<CoordinatorService>, Arc<ScriptRegistry>) {
        let store = Arc::new(InMemorySchedulerStore::new());
        let coordinator = Arc::new(CoordinatorService::new(store));
        let registry = Arc::new({
            let mut r = ScriptRegistry::new();
            r.register(&ScriptDescriptor::new("demo", noop));
            r
        });
        (coordinator, registry)
    }

    #[test]
    fn builder_requires_auth_when_flagged() {
        let (coordinator, registry) = parts();
        let result = ChrononState::builder(coordinator, registry)
            .require_admin_auth(true)
            .build();
        let Err(err) = result else {
            panic!("must fail without AdminAuth");
        };
        assert!(err.to_string().contains("AdminAuth"));
    }
}
