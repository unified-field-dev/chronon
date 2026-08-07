# chronon-e2e

Matrix-driven **correctness** integration tests for Chronon scheduler runtime features.

## vs inline crate tests

| | Inline `#[cfg(test)]` | `chronon-e2e` |
|---|---|---|
| Scope | Fast, crate-scoped defaults | Cross-cutting matrix |
| Storage | In-process `mem` | `mem` + `sqlite` + postgres/redis on PR CI |
| Assertions | Unit/integration | Declarative scenario runner + catalog |

## CI strategy

| Trigger | Scope | Command |
|---------|-------|---------|
| Push / PR | **Core** — mem + sqlite × embedded + coordinator-worker | `cargo test -p chronon-e2e -p chronon-axum -- --test-threads=1` |
| Push / PR | **Durable** — postgres + postgres-redis scenario matrix (`--ignored`) | `cargo test -p chronon-e2e --test scenarios -- --ignored` (see `e2e-durable` in [`ci.yml`](../.github/workflows/ci.yml)) |
| AWS fleet | **Full gate** — durable + distributed smokes | `$UF_LAB_ROOT/chronon/infra/aws/chronon/run-e2e-aws.sh` |
| AWS preflight | Mirror full PR CI suite | `$UF_LAB_ROOT/chronon/infra/aws/chronon/run-remote-ci.sh` |

## Coverage matrix

**PR CI:** `mem` + `sqlite` (core job) and postgres / postgres-redis (durable job). Sad paths marked **(sad)**.

| Scenario | mem | sqlite | postgres | postgres-redis |
|----------|:---:|:---:|:---:|:---:|
| All catalog scenarios × embedded (14) | ✓ | ✓ | ✓ (ignored) | ✓ (ignored) |
| All catalog scenarios × coordinator-worker (14) | ✓ | ✓ | ✓ (ignored) | ✓ (ignored) |
| Distributed smokes (multi-worker) | — | — | — | ✓ (AWS only) |

**Axum HTTP hardening** ([`chronon-axum/tests/router_smoke.rs`](../chronon-axum/tests/router_smoke.rs)): AdminAuth require/token, System actor reject, upsert-by-name, policy clamps, list limit clamp, revision redaction (+ store retention), host auth middleware — run on every PR with `cargo test -p chronon-axum`. Catalog includes `actor_snapshot_toctou`. AWS e2e: no new scenarios (unit/integration cover C-1..C-4); existing scripts pick up the catalog via matrix macros.

**Store contract** ([`run_store_contract`](../chronon-testkit/src/store_contract.rs)): mem, sqlite, postgres, redis composite, concurrent claim exclusivity.

## Run

```bash
export CARGO_BUILD_JOBS=1

# PR CI core slice
cargo test -p chronon-e2e -p chronon-axum -- --test-threads=1

# Durable postgres + redis
export CHRONON_POSTGRES_URL=postgres://...
export CHRONON_REDIS_URL=redis://127.0.0.1:6379
cargo test -p chronon-e2e -- --ignored --test-threads=1

# Multi-process distributed smoke (local child daemons / AWS)
cargo test -p chronon-e2e --test distributed_smoke -- --ignored --test-threads=1
```

## AWS E2E

Fleet layout and commands: `$UF_LAB_ROOT/chronon/infra/aws/chronon/README.md`.

```bash
$UF_LAB_ROOT/chronon/infra/aws/chronon/run-remote-ci.sh   # full PR CI mirror + durable
$UF_LAB_ROOT/chronon/infra/aws/chronon/deploy-and-run-e2e.sh  # durable + distributed smokes
```

Requires `CHRONON_E2E_HOST`, `CHRONON_DATA_IP`, and `CHRONON_SSH_KEY` (path to the EC2 SSH private key).

## Related

- Harness + catalog: [`chronon-testkit`](../chronon-testkit/README.md)
- Benchmarks: [`chronon-bench`](../chronon-bench/README.md)
