//! Bootstrap SQL tables and indexes for the Chronon scheduler store.
//!
//! Internal — used by [`SqlSchedulerStore`](crate::SqlSchedulerStore) on connect.

use chronon_core::Result;

use crate::{SqlDialect, SqlPool, SqlSchedulerStore};

const JOB_TABLE: &str = r"
CREATE TABLE IF NOT EXISTS chronon_job (
    job_id TEXT PRIMARY KEY,
    job_name TEXT NOT NULL UNIQUE,
    script_name TEXT NOT NULL,
    script_sig_hash TEXT NOT NULL,
    enabled INTEGER NOT NULL,
    schedule_kind TEXT NOT NULL,
    cron_expr TEXT,
    timezone TEXT,
    run_once_at TIMESTAMPTZ,
    run_once_claimed_at TIMESTAMPTZ,
    run_once_claimed_by TEXT,
    run_once_completed_at TIMESTAMPTZ,
    run_once_claim_expires_at TIMESTAMPTZ,
    partition_hash BIGINT,
    claim_lease_id TEXT,
    claim_lease_until TIMESTAMPTZ,
    pool TEXT,
    region TEXT,
    placement_json TEXT,
    actor_json TEXT NOT NULL,
    params_json TEXT NOT NULL,
    concurrency INTEGER NOT NULL,
    timeout_ms BIGINT,
    retry_policy_json TEXT NOT NULL,
    misfire_policy_json TEXT NOT NULL,
    parent_limits_json TEXT,
    next_run_at TIMESTAMPTZ,
    current_revision INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
)";

const RUN_TABLE: &str = r"
CREATE TABLE IF NOT EXISTS chronon_run (
    run_id TEXT PRIMARY KEY,
    job_id TEXT,
    script_name TEXT NOT NULL,
    parent_run_id TEXT,
    root_run_id TEXT,
    child_index INTEGER,
    scheduled_for TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    duration_ms BIGINT,
    status TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    instance_id TEXT,
    placement_json TEXT,
    pool_id TEXT,
    actor_json TEXT NOT NULL,
    params_json TEXT NOT NULL,
    stdout_text TEXT,
    stderr_text TEXT,
    error_json TEXT,
    stats_json TEXT,
    claimed_by TEXT,
    claim_lease_until TIMESTAMPTZ
)";

const REVISION_TABLE: &str = r"
CREATE TABLE IF NOT EXISTS chronon_job_revision (
    revision_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    revision_number INTEGER NOT NULL,
    changed_at TIMESTAMPTZ NOT NULL,
    changed_by_actor_json TEXT NOT NULL,
    snapshot_json TEXT NOT NULL
)";

const SCRIPT_TABLE: &str = r"
CREATE TABLE IF NOT EXISTS chronon_script (
    script_id TEXT PRIMARY KEY,
    script_name TEXT NOT NULL UNIQUE,
    signature_json TEXT NOT NULL,
    signature_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
)";

const LEADER_TABLE: &str = r"
CREATE TABLE IF NOT EXISTS chronon_scheduler_leader (
    leader_id TEXT PRIMARY KEY,
    leader_instance_id TEXT NOT NULL,
    leader_lease_until TIMESTAMPTZ NOT NULL,
    last_heartbeat_at TIMESTAMPTZ NOT NULL
)";

const PARTITION_TABLE: &str = r"
CREATE TABLE IF NOT EXISTS chronon_partition_assignment (
    partition_id TEXT PRIMARY KEY,
    owner_instance_id TEXT NOT NULL,
    lease_until TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
)";

const WORKER_TABLE: &str = r"
CREATE TABLE IF NOT EXISTS chronon_worker (
    worker_id TEXT PRIMARY KEY,
    pool_id TEXT NOT NULL,
    cell_id TEXT,
    status TEXT NOT NULL,
    last_heartbeat_at TIMESTAMPTZ NOT NULL,
    capacity_json TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
)";

/// Create tables and indexes if missing.
pub async fn ensure_schema(store: &SqlSchedulerStore) -> Result<()> {
    if store.dialect() == SqlDialect::Postgres {
        let SqlPool::Postgres(pool) = store.pool() else {
            return Err(chronon_core::ChrononError::Internal(
                "postgres dialect without postgres pool".into(),
            ));
        };
        let mut conn = pool.acquire().await.map_err(crate::error_map::map_err)?;
        sqlx::query("SELECT pg_advisory_lock(872349013)")
            .execute(&mut *conn)
            .await
            .map_err(crate::error_map::map_err)?;
        let result = ensure_schema_tables_on_conn(&mut conn).await;
        let _ = sqlx::query("SELECT pg_advisory_unlock(872349013)")
            .execute(&mut *conn)
            .await;
        return result;
    }
    ensure_schema_tables(store).await
}

async fn ensure_schema_tables_on_conn(
    conn: &mut sqlx::pool::PoolConnection<sqlx::Postgres>,
) -> Result<()> {
    for ddl in schema_table_ddls() {
        sqlx::query(ddl)
            .execute(&mut **conn)
            .await
            .map_err(crate::error_map::map_err)?;
    }
    for ddl in schema_index_ddls() {
        sqlx::query(ddl)
            .execute(&mut **conn)
            .await
            .map_err(crate::error_map::map_err)?;
    }
    Ok(())
}

async fn ensure_schema_tables(store: &SqlSchedulerStore) -> Result<()> {
    for ddl in schema_table_ddls() {
        store.run_ddl(ddl).await?;
    }
    for ddl in schema_index_ddls() {
        store.run_ddl(ddl).await?;
    }
    Ok(())
}

fn schema_table_ddls() -> [&'static str; 7] {
    [
        JOB_TABLE,
        RUN_TABLE,
        REVISION_TABLE,
        SCRIPT_TABLE,
        LEADER_TABLE,
        PARTITION_TABLE,
        WORKER_TABLE,
    ]
}

fn schema_index_ddls() -> [&'static str; 6] {
    [
        "CREATE INDEX IF NOT EXISTS chronon_job_name ON chronon_job (job_name)",
        "CREATE INDEX IF NOT EXISTS chronon_job_due_partitions ON chronon_job (next_run_at, partition_hash)
             WHERE enabled = 1 AND schedule_kind != 'manual' AND next_run_at IS NOT NULL",
        "CREATE INDEX IF NOT EXISTS chronon_run_job_id ON chronon_run (job_id)",
        "CREATE INDEX IF NOT EXISTS chronon_run_queued_pool ON chronon_run (pool_id, scheduled_for)
             WHERE status = 'queued'",
        "CREATE INDEX IF NOT EXISTS chronon_run_scheduled_for_run_id ON chronon_run (scheduled_for, run_id)",
        "CREATE INDEX IF NOT EXISTS chronon_job_revision_job ON chronon_job_revision (job_id, revision_number)",
    ]
}

#[cfg(test)]
mod tests {
    use crate::SqlSchedulerStore;

    #[tokio::test]
    async fn schema_idempotent_sqlite() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        let b1 = SqlSchedulerStore::connect_sqlite(&url).await.unwrap();
        let b2 = SqlSchedulerStore::connect_sqlite(&url).await.unwrap();
        drop(b1);
        drop(b2);
    }

    #[tokio::test]
    async fn sqlite_wal_concurrent_upserts_do_not_busy() {
        use std::sync::Arc;

        use chronon_core::models::Job;
        use chronon_core::store::SchedulerStore;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        let store = Arc::new(SqlSchedulerStore::connect_sqlite(&url).await.unwrap());
        let mut joins = Vec::new();
        for task in 0..8 {
            let store = Arc::clone(&store);
            joins.push(tokio::spawn(async move {
                for i in 0..40 {
                    let job = Job::new(format!("w{task}-{i}"), "script_a");
                    store.upsert_job(&job).await.expect("upsert");
                }
            }));
        }
        for join in joins {
            join.await.expect("task");
        }
    }

    #[tokio::test]
    async fn sqlite_memory_url_still_connects() {
        SqlSchedulerStore::connect_sqlite("sqlite://:memory:")
            .await
            .expect("memory sqlite");
    }
}
