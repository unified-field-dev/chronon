# chronon-axum

HTTP API under `/api/chronon/*` — jobs, runs, scripts.

## Features

| Topic | Behavior |
|-------|----------|
| `AdminAuth` / `RequireAdmin` | Host verifier on `ChrononState`; lab: `StaticTokenAdminAuth`, `AllowAllAdminAuth` |
| `CHRONON_REQUIRE_ADMIN_AUTH` | Builder and extractor fail closed without a verifier |
| Upsert | Matched by `job_name`; updates preserve `job_id` and bump revision |
| External `actor_json` | Rejects System- and ServiceUser-shaped JSON; in-process may set either |
| List jobs/runs | Default `limit` 100, capped at 1000 |
| Revisions | HTTP response redacts actor/params; store keeps full snapshots |
| Errors | Sanitize + redact URL userinfo in envelopes |

## Production security

Install [`AdminAuth`](https://docs.rs/chronon-axum), set `CHRONON_REQUIRE_ADMIN_AUTH=1`, and wrap-before-public-bind. See [SECURITY.md](../SECURITY.md) and the `axum_auth_wrap` example.

## Mount

```rust
use std::sync::Arc;
use chronon_axum::{
    chronon_router, ChrononState, StaticTokenAdminAuth, API_PREFIX,
};

let chronon = ChrononState::builder(coordinator, registry)
    .admin_auth(Arc::new(StaticTokenAdminAuth::new("lab-token")))
    .require_admin_auth(true)
    .build()?;
Router::new()
    .nest(API_PREFIX, chronon_router::<AppState>())
    .with_state(AppState { chronon })
```
