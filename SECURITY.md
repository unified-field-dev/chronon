# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.1.x   | Yes       |

## Reporting a Vulnerability

Please report security issues privately to the repository maintainers via GitHub Security Advisories on [unified-field-dev/chronon](https://github.com/unified-field-dev/chronon/security/advisories/new).

Do not open public issues for undisclosed vulnerabilities.

## Operator hardening (L0)

Chronon is a **library scheduler**. Hosts (for example Higgs) own identity, authorization, and network exposure.

| Area | Guidance |
|------|----------|
| HTTP admin | Install a host [`AdminAuth`](https://docs.rs/chronon-axum) verifier on [`ChrononState`](https://docs.rs/chronon-axum). Set `CHRONON_REQUIRE_ADMIN_AUTH=1` so mounts fail closed without one. Lab helpers: `StaticTokenAdminAuth` / `AllowAllAdminAuth`. Chronon does not ship Soliton HMAC/mTLS. |
| Wrap before public bind | Nest `chronon_router` under `/api/chronon`, apply auth, then bind. Never expose an unauthenticated Chronon API on a public interface. See `axum_auth_wrap`. |
| HTTP `actor_json` | External upsert rejects System-shaped JSON (`RejectExternalSystemActor`). Default marker is `{"Service":{"name":"chronon_api"}}`. In-process upsert may set System. |
| Errors | Run/handler errors are sanitized and URL userinfo is redacted before persist and HTTP envelopes. |
| Persistence credentials | Treat Postgres / SQLite / Redis URLs as high-privilege secrets. Prefer TLS and authenticated Redis; use a unique Redis `key_prefix` per deployment. |
| Schema allowlist | Isolated Postgres schemas (`connect_postgres_isolated` / `CHRONON_POSTGRES_SCHEMA`) accept only `^[A-Za-z_][A-Za-z0-9_]*$`. |
| Script allowlist | Only scripts registered in the host `ScriptRegistry` can run. |
| HTTP revisions | `GET /jobs/{id}/revisions` nulls `changed_by_actor_json` and strips `actor_json` / `params_json` from `snapshot_json`. Full snapshots remain in the store. |
| Body size | Axum’s default body limit applies unless the host sets [`DefaultBodyLimit`](https://docs.rs/axum/latest/axum/extract/struct.DefaultBodyLimit.html). |
