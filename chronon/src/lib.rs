//! Chronon is a Rust cron and run-once scheduler for services: typed script handlers,
//! durable job/run history, and an optional coordinator–worker split behind a thin
//! [`SchedulerStore`](chronon_core::SchedulerStore) port.
//!
//! Wire storage once with [`ChrononBuilder`], register scripts with [`script`], schedule
//! [`Job`](chronon_core::Job)s, then call [`Chronon::run`]. Swap `mem`, `sqlite`, Postgres,
//! or Postgres+Redis without changing script code.
//!
//! ## Features
//!
//! - **Typed scripts** — `#[chronon::script]` registers handlers with inventory; params stay typed.
//! - **Fluent job construction** — preferred [`JobBuilder`] for cron / run-once / manual schedules
//!   (seed helpers on [`ScriptHandle`] remain as a low-level alternate).
//! - **Durable jobs and runs** — schedule config, revisions, and execution history on
//!   [`SchedulerStore`](chronon_core::SchedulerStore).
//! - **Upsert-by-name** — HTTP/job upsert preserves `job_id` when `job_name` already exists and
//!   bumps revision (see [Remote HTTP client](#remote-http-client) / axum handlers).
//! - **Security bounds** — list pagination and policy knobs clamp to
//!   [`MAX_LIST_LIMIT`](chronon_core::MAX_LIST_LIMIT) / related ceilings.
//! - **Revision redaction** — HTTP revision responses omit actor/params; store keeps full snapshots.
//! - **Schema allowlist** — isolated Postgres schema names must match
//!   `validate_postgres_schema_name` in `chronon-backend-sql-common`.
//! - **Composable storage** — in-memory, SQLite, PostgreSQL, or Postgres + Redis claim overlay.
//! - **Embedded or split topology** — one process, or coordinator / worker / remote HTTP client
//!   (see [Choose a topology](#choose-a-topology)).
//! - **Host identity** — [`ContextFactory`](chronon_core::ContextFactory) rebuilds run-time
//!   context from the **run** `actor_json` snapshot.
//! - **Optional HTTP API** — mount [`chronon_router`] (`axum` feature) with [`AdminAuth`] /
//!   [`RequireAdmin`] and `CHRONON_REQUIRE_ADMIN_AUTH`; external upsert rejects System-shaped
//!   `actor_json`.
//!
//! *Cron and run-once scheduling without locking you into one database or a full workflow engine.*
//!
//! This crate ships with **no default features** (`default = []`). Enable explicitly:
//! `mem`, `sqlite`, `postgres`, `redis` (requires `postgres`), `axum`, `telemetry-console`.
//!
//! # Getting started
//!
//! You always define scripts with `#[chronon::script]` and schedule via the generated
//! [`ScriptHandle`] with [`JobBuilder`] (preferred), then
//! [`CoordinatorService::upsert_job`] / [`CoordinatorService::run_now`]. What changes is
//! **which process ticks the schedule and which process executes scripts**.
//!
//! ## Choose a topology
//!
//! - **[Embedded (one process)](#embedded-one-process)** — one binary schedules **and**
//!   executes. Start here.
//! - **[Coordinator–worker (split processes)](#coordinator-worker-split)** — one process ticks /
//!   enqueues; one or more **worker** binaries claim and run scripts.
//! - **[Remote HTTP client](#remote-http-client)** — your app has **no** local Chronon loops; it
//!   talks to a coordinator HTTP API via [`RemoteCoordinatorClient`]. Optional.
//!
//! | Topology | Builder | Store fit | When to use |
//! |----------|---------|-----------|-------------|
//! | Embedded | [`.embedded()`](ChrononBuilder::embedded) | mem / sqlite / postgres / postgres+redis | Local, single host, or simple production |
//! | Coordinator | [`.coordinator_only()`](ChrononBuilder::coordinator_only) | Shared durable (postgres ± redis) | Scale-out: tick only |
//! | Worker | [`.worker(pool)`](ChrononBuilder::worker) | Same shared store | Scale-out: claim + execute |
//! | Remote client | [`.remote_coordinator(url)`](ChrononBuilder::remote_coordinator) | None locally | Schedule via HTTP |
//!
//! Topology is [`DeploymentShape`] on [`ChrononBuilder`]. After you pick a topology, continue with
//! [define a script](#4-define-a-script) (shared by every topology).
//!
//! ## Embedded (one process)
//!
//! This process runs the scheduler tick **and** the worker. There is no second binary.
//!
//! ```text
//! Your app ──ScriptHandle / upsert_job──► Chronon ──tick + claim──► script handlers
//!                                            │
//!                                            └──► mem / SQLite / Postgres / Postgres+Redis
//! ```
//!
//! | Backend | Type | Feature | Topology | Embedded boot |
//! |---------|------|---------|----------|---------------|
//! | In-memory | [`InMemorySchedulerStore`] | `mem` | embedded only | Below |
//! | SQLite | [`SqliteSchedulerStore`] | `sqlite` | embedded | [sqlite crate](../chronon_backend_sqlite/index.html#embedded) |
//! | PostgreSQL | [`PostgresSchedulerStore`] | `postgres` | embedded or coordinator–worker | [postgres crate](../chronon_backend_postgres/index.html#embedded) |
//! | Postgres + Redis | [`PostgresRedisSchedulerStore`] | `postgres,redis` | embedded or coordinator–worker | [redis crate](../chronon_backend_redis/index.html#embedded) |
//!
//! **In-memory first run** — `#[chronon::script]` generates a handle factory and
//! `NightlyCleanupParams`; prefer that over stringly `Job::new`:
//!
//! ```ignore
//! use std::sync::Arc;
//! use chronon::prelude::*;
//! use chronon::InMemorySchedulerStore;
//!
//! #[chronon::script(name = "nightly_cleanup")]
//! async fn nightly_cleanup(
//!     ctx: Box<dyn ScriptContext>,
//!     retention_days: u32,
//! ) -> chronon::Result<()> {
//!     let _ = (ctx.label(), retention_days);
//!     Ok(())
//! }
//!
//! # async fn main() -> chronon::Result<()> {
//! let chronon = ChrononBuilder::new()
//!     .scheduler_store(Arc::new(InMemorySchedulerStore::new()))
//!     .context_factory(Arc::new(JsonScriptContextFactory))
//!     .embedded()
//!     .auto_registry()
//!     .build()?;
//!
//! let job = JobBuilder::new(&nightly_cleanup())
//!     .name("nightly-schedule")
//!     .cron("0 2 * * *")?
//!     .timezone("UTC")
//!     .params(NightlyCleanupParams { retention_days: 7 })
//!     .build()?;
//! chronon.coordinator_service().upsert_job(job).await?;
//! // chronon.scheduler.init_partitions().await;
//! // chronon.run().await?;
//! # Ok(())
//! # }
//! ```
//!
//! Runnable: `script_handle_job`, `script_macro`, `embedded_tick`, `run_now` (`--features mem`).
//! Other stores: follow the Embedded links in the table above. Then continue with
//! [define a script](#4-define-a-script).
//!
//! ## Coordinator–worker (split processes)
//!
//! Use this when you want **scale-out execution** or to keep scheduling separate from script
//! work. Both processes share the same durable store; they do **not** share memory.
//! [`InMemorySchedulerStore`] cannot cross process boundaries — coordinator–worker needs SQLite
//! (same-host file), Postgres, or Postgres+Redis.
//!
//! ```text
//! Coordinator binary ──tick──► shared store ──claim──► Worker binary(ies)
//!        │                                              │
//!        └── ScriptHandle / upsert_job           script handlers
//! ```
//!
//! ### What you create
//!
//! | Piece | Purpose |
//! |-------|---------|
//! | Shared scripts | Same `#[chronon::script]` names linked into **workers** |
//! | Coordinator binary | [`.coordinator_only()`](ChrononBuilder::coordinator_only) — tick + partitions; **no** worker slots |
//! | Worker binary(ies) | [`.worker(pool)`](ChrononBuilder::worker) — claim + execute; unique [`.instance_id()`](ChrononBuilder::instance_id) |
//! | Shared store | Postgres (add Redis for production claim throughput) |
//!
//! ### Pick a shared store
//!
//! Wire coordinator and worker from the adapter pages (production default: Postgres + Redis):
//!
//! | Backend | Feature | Coordinator | Worker |
//! |---------|---------|-------------|--------|
//! | Postgres + Redis | `postgres,redis` | [Coordinator](../chronon_backend_redis/index.html#coordinator-binary) | [Worker](../chronon_backend_redis/index.html#worker-binary) |
//! | PostgreSQL | `postgres` | [Coordinator](../chronon_backend_postgres/index.html#coordinator-binary) | [Worker](../chronon_backend_postgres/index.html#worker-binary) |
//! | SQLite (same host) | `sqlite` | [Coordinator](../chronon_backend_sqlite/index.html#coordinator-binary) | [Worker](../chronon_backend_sqlite/index.html#worker-binary) |
//!
//! ### Run both
//!
//! 1. Start Postgres (and Redis). Set `CHRONON_POSTGRES_URL` / `CHRONON_REDIS_URL`.
//! 2. Start the **coordinator** ([`Chronon::run`] — leader election assigns partitions).
//! 3. Start one or more **workers** with unique `CHRONON_INSTANCE_ID` values.
//! 4. Upsert jobs (via [`ScriptHandle`]) from the coordinator, an Axum host, or a
//!    [remote HTTP client](#remote-http-client).
//!
//! ```bash
//! export CHRONON_POSTGRES_URL=postgres://user:pass@localhost/chronon
//! export CHRONON_REDIS_URL=redis://127.0.0.1:6379
//! cargo run -p uf-chronon --example coordinator_daemon --features postgres,redis &
//! CHRONON_INSTANCE_ID=worker-a cargo run -p uf-chronon --example worker_daemon --features postgres,redis
//! ```
//!
//! Same-host SQLite split (shared file path):
//!
//! ```bash
//! export CHRONON_SQLITE_PATH=/tmp/chronon-split.db
//! cargo run -p uf-chronon --example sqlite_coordinator_daemon --features sqlite &
//! CHRONON_INSTANCE_ID=worker-a cargo run -p uf-chronon --example sqlite_worker_daemon --features sqlite
//! ```
//!
//! ## Remote HTTP client
//!
//! Use this when an application process should **schedule or trigger jobs** but must not run
//! Chronon loops locally. Pair it with a host that mounts [`chronon_router`] on an embedded or
//! coordinator–worker coordinator process.
//!
//! ```text
//! App binary ──RemoteCoordinatorClient──HTTP──► API host (chronon_router)
//!                                                    │
//!                                                    └── embedded or coordinator + store
//! ```
//!
//! **API host** — nest the router under [`API_PREFIX`] (`/api/chronon`) **behind host
//! authentication** (Chronon does not authenticate these routes). Sketches:
//! `axum_host` (`mem,axum`), `axum_auth_wrap` (Tower Bearer demo). See repository `SECURITY.md`.
//!
//! **App binary** — prefer [`JobBuilder`] from your [`ScriptHandle`], then call
//! [`RemoteCoordinatorClient`] (do not call [`Chronon::run`]):
//!
//! ```ignore
//! use chronon::prelude::*;
//!
//! let base = resolve_remote_base_url()
//!     .unwrap_or_else(|| "http://127.0.0.1:8080".into());
//! let client = RemoteCoordinatorClient::new(base);
//!
//! let job = JobBuilder::new(&nightly_cleanup())
//!     .name("nightly-schedule")
//!     .manual()
//!     .params(NightlyCleanupParams { retention_days: 7 })
//!     .build()?;
//! client.upsert_job(job.clone()).await?;
//! let _run_id = client.run_now(&job.job_id).await?;
//! ```
//!
//! Runnable end-to-end demo (short-lived mem host + client):
//! `cargo run -p uf-chronon --example remote_http_client --features mem,axum`.
//!
//! Set `CHRONON_REMOTE_BASE_URL` for [`resolve_remote_base_url`]. Timeout:
//! `CHRONON_REMOTE_HTTP_TIMEOUT_MS` (default 3000).
//!
//! ## 4. Define a script
//!
//! `#[chronon::script]` registers the handler **and** turns the function into a
//! [`ScriptHandle`] factory. Parameter types become a generated `*Params` struct
//! (for example `NightlyCleanupParams`).
//!
//! ```ignore
//! use chronon::prelude::*;
//!
//! #[chronon::script(name = "nightly_cleanup")]
//! async fn nightly_cleanup(
//!     ctx: Box<dyn ScriptContext>,
//!     retention_days: u32,
//! ) -> chronon::Result<()> {
//!     println!("{}: retaining {retention_days} days", ctx.label());
//!     Ok(())
//! }
//!
//! // nightly_cleanup() -> ScriptHandle<NightlyCleanupParams>
//! // NightlyCleanupParams { retention_days: u32 }
//! ```
//!
//! Use [`.auto_registry()`](ChrononBuilder::auto_registry) so inventory picks up every
//! `#[chronon::script]` linked into the binary. In a coordinator–worker split, scripts must be
//! linked into **worker** binaries (that is where they run).
//!
//! See [`script`], [`ScriptHandle`], and [`ScriptContext`](chronon_core::ScriptContext).
//! Runnable: `script_handle_job`, `script_macro`.
//!
//! ## 5. Schedule and trigger jobs
//!
//! **Preferred:** build a [`Job`](chronon_core::Job) with [`JobBuilder`] from the generated
//! [`ScriptHandle`], then upsert. This validates cron and sets `next_run_at` for you.
//!
//! | [`ScheduleKind`](chronon_core::ScheduleKind) | Builder method | Behavior |
//! |----------------------------------------------|----------------|----------|
//! | `Cron` | [`.cron`](JobBuilder::cron) (+ optional [`.timezone`](JobBuilder::timezone)) | Recurring |
//! | `RunOnce` | [`.run_once_at`](JobBuilder::run_once_at) | Fires when `next_run_at` is due |
//! | `Manual` | [`.manual`](JobBuilder::manual) | Never due for tick — only [`CoordinatorService::run_now`] |
//!
//! ```ignore
//! use chronon::prelude::*;
//!
//! let nightly = JobBuilder::new(&nightly_cleanup())
//!     .name("nightly-schedule")
//!     .cron("0 2 * * *")?
//!     .params(NightlyCleanupParams { retention_days: 7 })
//!     .build()?;
//! chronon.coordinator_service().upsert_job(nightly).await?;
//!
//! let manual = JobBuilder::new(&nightly_cleanup())
//!     .name("cleanup-now")
//!     .manual()
//!     .params(NightlyCleanupParams { retention_days: 30 })
//!     .build()?;
//! let id = manual.job_id.clone();
//! chronon.coordinator_service().upsert_job(manual).await?;
//! chronon.coordinator_service().run_now(&id).await?;
//! ```
//!
//! Low-level alternate: [`ScriptHandle::job_with_params`] then mutate schedule fields on the
//! [`Job`](chronon_core::Job) — prefer [`JobBuilder`] in new code.
//!
//! Cron uses standard five-field syntax (optional sixth field for seconds). Parse helpers:
//! [`CronExpr`]. Runnable: `script_handle_job`, `run_now`, `embedded_tick`.
//!
//! Storage wiring: [Embedded](#embedded-one-process) (mem below; other backends on adapter
//! crates) and [Coordinator–worker](#coordinator-worker-split) (link table).
//!
//! # Notes
//!
//! - **No default Cargo features** — enable `mem`, `sqlite`, `postgres`, `redis`, and/or `axum`
//!   explicitly. Document the public crate with `--all-features` so rustdoc links resolve.
//! - **Coordinator–worker scripts live on workers** — inventory must be linked into the binary
//!   that calls `.worker(...)`; the coordinator ticks but does not execute handlers.
//! - **Call `scheduler.init_partitions().await` before [`Chronon::run`]** on the embedded
//!   shape. Coordinator-only needs no such call — leader election assigns partitions to
//!   whichever replica wins the lease.
//! - **RemoteClient must not call [`Chronon::run`]** — that shape returns an error; use
//!   [`RemoteCoordinatorClient`].
//! - **`mem` is embedded-only** — it does not cross process boundaries.
//!
//! # Architecture
//!
//! Your application owns identity policy and business logic. Chronon owns scheduling semantics:
//! due queries, claiming, cron evaluation, and script dispatch. Production trust boundaries
//! (HTTP auth, store credentials, fail-closed [`ContextFactory`](chronon_core::ContextFactory),
//! list/policy clamps, revision redaction, schema allowlisting) are documented in the repository
//! `SECURITY.md`.
//!
//! | Concern | Where |
//! |---------|--------|
//! | Upsert-by-name | Axum upsert + `get_job_by_name` |
//! | AdminAuth / require flag | `chronon-axum` `RequireAdmin` + `CHRONON_REQUIRE_ADMIN_AUTH` |
//! | External System actor | `RejectExternalSystemActor` on HTTP upsert |
//! | Actor snapshot at execute | Runtime worker / `Executor::spawn_run` use run `actor_json` |
//! | List / policy bounds | `MAX_*` + `Job::clamp_security_bounds` / handler `.min(MAX_LIST_LIMIT)` |
//! | Revision HTTP redaction | Axum revision handlers |
//! | Error sanitize / URL redact | `sanitize_error_message` / `redact_endpoint` |
//! | Postgres schema allowlist | `validate_postgres_schema_name` in sql-common |
//!
//! ```text
//! Your app / worker binary
//!         │
//!         ▼
//!  ChrononBuilder ──► SchedulerStore port ──► mem | sqlite | postgres | postgres+redis | custom
//!         │
//!         ├──► Scheduler (tick / partitions)
//!         └──► Executor + ScriptRegistry  ◄── ContextFactory / #[chronon::script]
//! ```
//!
//! Coordinator–worker splits the loops across processes that share the store:
//!
//! ```text
//! Coordinator ──.coordinator_only()──► tick + partitions ──► SchedulerStore
//! Worker(s)   ──.worker(pool)────────► claim + execute   ──► same SchedulerStore
//! ```
//!
//! # Configuration
//!
//! Settings merge in this order: explicit [`ChrononBuilder`] values override environment
//! defaults where both exist.
//!
//! | Setting | Builder API | Environment | Default |
//! |---------|-------------|-------------|---------|
//! | Store | `.scheduler_store()` / `.scheduler_store_from_global()` | — | required |
//! | Context factory | `.context_factory()` | — | `NoOpContextFactory` |
//! | Telemetry | `.telemetry_sink()` | — | `NoOpSink` |
//! | Script registry | `.script_registry()` / `.auto_registry()` | — | empty or inventory |
//! | Tick interval | `.tick_interval_ms()` | `CHRONON_TICK_INTERVAL_MS` | 250 ms |
//! | Instance id | `.instance_id()` | — | random UUID |
//! | Partition count | — (env only) | `CHRONON_NUM_PARTITIONS` | 64 |
//! | Worker pool | `.worker(pool)` / env | `CHRONON_WORKER_POOL` | `"general"` |
//! | Worker concurrency | — | `CHRONON_WORKER_CONCURRENCY` | 4 |
//! | Remote base URL | `.remote_coordinator(url)` | `CHRONON_REMOTE_BASE_URL` | — |
//!
//! Lease TTLs and tick batch limits are environment-only. See `chronon-scheduler` crate
//! documentation for the full table.
//!
//! # Cargo features
//!
//! | Feature | Type | Status |
//! |---------|------|--------|
//! | `mem` | [`InMemorySchedulerStore`] | Ready — tests and local embedded |
//! | `sqlite` | [`SqliteSchedulerStore`] | Ready — embedded file-backed |
//! | `postgres` | [`PostgresSchedulerStore`] | Ready — shared durable |
//! | `redis` | [`PostgresRedisSchedulerStore`] | Ready — Postgres + Redis claim overlay (**requires `postgres`**) |
//! | `axum` | [`chronon_router`], HTTP DTOs | Ready — mount on host Axum server (**host must authenticate**) |
//! | `telemetry-console` | Documents `ConsoleSink` usage | Optional marker (`ConsoleSink` always re-exported) |
//!
//! # Runnable examples
//!
//! Canonical path (see crate README **How to run examples** for multi-worker recipes):
//!
//! | Example | Topology | Features |
//! |---------|----------|----------|
//! | `sqlite_boot` | Embedded | `sqlite` |
//! | `sqlite_coordinator_daemon` / `sqlite_worker_daemon` | Coordinator–worker (local) | `sqlite` |
//! | `coordinator_daemon` / `worker_daemon` | Coordinator–worker (Postgres+Redis) | `postgres,redis` |
//! | `remote_http_client` | Remote HTTP client | `mem,axum` |
//!
//! Other examples: `script_macro`, `script_handle_job`, `run_now`, `embedded_tick`,
//! `store_router_boot`, `postgres_boot`, `postgres_redis_boot`, `axum_host`, `axum_auth_wrap`,
//! `postgres_coordinator_daemon`, `postgres_worker_daemon`.
//!
//! ```bash
//! cargo run -p uf-chronon --example sqlite_boot --features sqlite
//! cargo run -p uf-chronon --example remote_http_client --features mem,axum
//! ```

pub use chronon_macros::script;
pub use quark::inventory;

pub mod prelude {
    //! Curated re-exports for **application developers** building Chronon worker binaries.
    //!
    //! One-import surface for models, runtime boot, scheduler, executor, and the [`script`] macro.
    //! Prefer `use chronon::prelude::*;` in worker binaries and integration tests rather than
    //! importing internal crates directly. For durable storage wiring, also enable public crate features
    //! (`sqlite`, `postgres`, `redis`) and construct the matching [`SchedulerStore`] adapter.

    pub use crate::script;
    pub use chronon_core::{
        ChrononError, ContextFactory, Job, JobRevision, JsonScriptContextFactory,
        NoOpContextFactory, NoOpScriptContext, Result, Run, RunStatus, ScheduleKind,
        SchedulerStore, Script, ScriptContext, ScriptHandle, StoreRouter, DEFAULT_STORE_NAME,
    };
    pub use chronon_executor::{Executor, ExecutorEvent, ScriptDescriptor, ScriptRegistry};
    pub use chronon_runtime::{
        builder, resolve_remote_base_url, Chronon, ChrononBuilder, CoordinatorService,
        DeploymentShape, JobSummary, RemoteCoordinatorClient,
    };
    pub use chronon_scheduler::{CronExpr, JobBuilder, Scheduler, SchedulerConfig};
}

pub use chronon_core as core;
pub use chronon_core::{ChrononError, Result, ScriptHandle};
pub use chronon_executor::{ScriptDescriptor, ScriptRegistry};
pub use chronon_runtime::{
    builder, resolve_remote_base_url, Chronon, ChrononBuilder, CoordinatorService, DeploymentShape,
    RemoteCoordinatorClient,
};
pub use chronon_scheduler::{CronExpr, JobBuilder};

#[cfg(feature = "axum")]
pub use chronon_axum::{
    chronon_router, require_admin_auth_from_env, AdminAuth, AdminAuthError, AllowAllAdminAuth,
    ApiResponse, ChrononState, ChrononStateBuilder, RequireAdmin, StaticTokenAdminAuth, API_PREFIX,
    REQUIRE_ADMIN_AUTH_ENV,
};

#[cfg(feature = "mem")]
pub use chronon_backend_mem::{install_default_mem_store, InMemorySchedulerStore};

#[cfg(feature = "sqlite")]
pub use chronon_backend_sqlite::SqliteSchedulerStore;

#[cfg(feature = "postgres")]
pub use chronon_backend_postgres::{postgres_test_url, PostgresSchedulerStore};

#[cfg(feature = "redis")]
pub use chronon_backend_redis::{PostgresRedisSchedulerStore, RedisQueueLayer};

pub use chronon_telemetry::{ConsoleSink, NoOpSink, TelemetrySink};
