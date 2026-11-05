//! Job persistence for the SQL scheduler store.
//!
//! Internal — used by [`SqlSchedulerStore`](crate::SqlSchedulerStore); not a stable public API.

use chrono::{DateTime, Utc};

use chronon_core::error::{ChrononError, Result};
use chronon_core::models::Job;

use crate::row::{row_to_job, JobRow};
use crate::{bind_sql, sql_execute, sql_fetch_all_map, sql_fetch_optional_map, SqlSchedulerStore};

pub(crate) async fn upsert_job(store: &SqlSchedulerStore, job: &Job) -> Result<()> {
    let row = JobRow::from_model(job)?;
    let sql = bind_sql(
        store.dialect,
        "INSERT INTO chronon_job (
            job_id, job_name, script_name, script_sig_hash, enabled, schedule_kind,
            cron_expr, timezone, run_once_at, run_once_claimed_at, run_once_claimed_by,
            run_once_completed_at, run_once_claim_expires_at, partition_hash, claim_lease_id,
            claim_lease_until, pool, region, placement_json, actor_json, params_json,
            concurrency, timeout_ms, retry_policy_json, misfire_policy_json, parent_limits_json,
            next_run_at, current_revision, updated_at, created_at
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        ON CONFLICT (job_id) DO UPDATE SET
            job_name = excluded.job_name,
            script_name = excluded.script_name,
            script_sig_hash = excluded.script_sig_hash,
            enabled = excluded.enabled,
            schedule_kind = excluded.schedule_kind,
            cron_expr = excluded.cron_expr,
            timezone = excluded.timezone,
            run_once_at = excluded.run_once_at,
            run_once_claimed_at = excluded.run_once_claimed_at,
            run_once_claimed_by = excluded.run_once_claimed_by,
            run_once_completed_at = excluded.run_once_completed_at,
            run_once_claim_expires_at = excluded.run_once_claim_expires_at,
            partition_hash = excluded.partition_hash,
            claim_lease_id = excluded.claim_lease_id,
            claim_lease_until = excluded.claim_lease_until,
            pool = excluded.pool,
            region = excluded.region,
            placement_json = excluded.placement_json,
            actor_json = excluded.actor_json,
            params_json = excluded.params_json,
            concurrency = excluded.concurrency,
            timeout_ms = excluded.timeout_ms,
            retry_policy_json = excluded.retry_policy_json,
            misfire_policy_json = excluded.misfire_policy_json,
            parent_limits_json = excluded.parent_limits_json,
            next_run_at = excluded.next_run_at,
            current_revision = excluded.current_revision,
            updated_at = excluded.updated_at,
            created_at = excluded.created_at",
    );
    // `sql_execute!` returns early on error via `?`; the async block keeps that error local.
    let result = async {
        sql_execute!(store, &sql, |q| {
            let enabled_flag = i32::from(row.enabled);
            q.bind(&row.job_id)
                .bind(&row.job_name)
                .bind(&row.script_name)
                .bind(&row.script_sig_hash)
                .bind(enabled_flag)
                .bind(&row.schedule_kind)
                .bind(&row.cron_expr)
                .bind(&row.timezone)
                .bind(row.run_once_at)
                .bind(row.run_once_claimed_at)
                .bind(&row.run_once_claimed_by)
                .bind(row.run_once_completed_at)
                .bind(row.run_once_claim_expires_at)
                .bind(row.partition_hash)
                .bind(&row.claim_lease_id)
                .bind(row.claim_lease_until)
                .bind(&row.pool)
                .bind(&row.region)
                .bind(&row.placement_json)
                .bind(&row.actor_json)
                .bind(&row.params_json)
                .bind(row.concurrency)
                .bind(row.timeout_ms)
                .bind(&row.retry_policy_json)
                .bind(&row.misfire_policy_json)
                .bind(&row.parent_limits_json)
                .bind(row.next_run_at)
                .bind(row.current_revision)
                .bind(row.updated_at)
                .bind(row.created_at)
        })
    }
    .await;
    match result {
        Err(ChrononError::StorageError { message, source })
            if message.contains("UNIQUE")
                || message.contains("unique")
                || message.contains("duplicate") =>
        {
            // The upsert is keyed on job_id, so a unique violation means job_name collided.
            match get_job_by_name(store, &job.job_name).await? {
                Some(existing) if existing.job_id != job.job_id => {
                    Err(ChrononError::JobNameTaken {
                        job_name: job.job_name.clone(),
                        existing_job_id: existing.job_id,
                    })
                }
                _ => Err(ChrononError::StorageError { message, source }),
            }
        }
        other => other,
    }
}

pub(crate) async fn get_job(store: &SqlSchedulerStore, job_id: &str) -> Result<Option<Job>> {
    let sql = bind_sql(store.dialect, "SELECT * FROM chronon_job WHERE job_id = ?");
    sql_fetch_optional_map!(store, &sql, |q| q.bind(job_id), |r| row_to_job(&r))
}

pub(crate) async fn get_job_by_name(
    store: &SqlSchedulerStore,
    job_name: &str,
) -> Result<Option<Job>> {
    let sql = bind_sql(
        store.dialect,
        "SELECT * FROM chronon_job WHERE job_name = ?",
    );
    sql_fetch_optional_map!(store, &sql, |q| q.bind(job_name), |r| row_to_job(&r))
}

pub(crate) async fn list_jobs(store: &SqlSchedulerStore) -> Result<Vec<Job>> {
    let sql = bind_sql(
        store.dialect,
        "SELECT * FROM chronon_job ORDER BY created_at ASC",
    );
    sql_fetch_all_map!(store, &sql, |q| q, |r| row_to_job(r))
}

pub(crate) async fn list_due_jobs(
    store: &SqlSchedulerStore,
    before: DateTime<Utc>,
) -> Result<Vec<Job>> {
    let sql = bind_sql(
        store.dialect,
        "SELECT * FROM chronon_job
         WHERE enabled = 1 AND next_run_at IS NOT NULL AND next_run_at <= ?
         ORDER BY next_run_at ASC",
    );
    sql_fetch_all_map!(store, &sql, |q| q.bind(before), |r| row_to_job(r))
}

pub(crate) async fn pause_job(store: &SqlSchedulerStore, job_id: &str) -> Result<()> {
    let sql = bind_sql(
        store.dialect,
        "UPDATE chronon_job SET enabled = 0, updated_at = ? WHERE job_id = ?",
    );
    let now = Utc::now();
    let result = sql_execute!(store, &sql, |q| q.bind(now).bind(job_id));
    result?;
    if get_job(store, job_id).await?.is_none() {
        return Err(ChrononError::JobNotFound(job_id.to_string()));
    }
    Ok(())
}

pub(crate) async fn resume_job(store: &SqlSchedulerStore, job_id: &str) -> Result<()> {
    let sql = bind_sql(
        store.dialect,
        "UPDATE chronon_job SET enabled = 1, updated_at = ? WHERE job_id = ?",
    );
    let now = Utc::now();
    sql_execute!(store, &sql, |q| q.bind(now).bind(job_id))?;
    if get_job(store, job_id).await?.is_none() {
        return Err(ChrononError::JobNotFound(job_id.to_string()));
    }
    Ok(())
}

pub(crate) async fn delete_job(store: &SqlSchedulerStore, job_id: &str) -> Result<bool> {
    if get_job(store, job_id).await?.is_none() {
        return Ok(false);
    }
    let sql = bind_sql(
        store.dialect,
        "DELETE FROM chronon_job_revision WHERE job_id = ?",
    );
    sql_execute!(store, &sql, |q| q.bind(job_id))?;
    let sql = bind_sql(store.dialect, "DELETE FROM chronon_job WHERE job_id = ?");
    sql_execute!(store, &sql, |q| q.bind(job_id))?;
    Ok(true)
}
