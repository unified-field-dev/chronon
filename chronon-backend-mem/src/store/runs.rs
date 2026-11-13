//! Run persistence and worker claim helpers.

use chrono::{DateTime, Duration, Utc};

use chronon_core::error::ChrononError;
use chronon_core::models::{Run, RunKey, RunStatus};
use chronon_core::Result;

use super::InMemorySchedulerStore;

pub(super) fn create_run(store: &InMemorySchedulerStore, run: &Run) -> Result<()> {
    store.runs.write().insert(run.run_id.clone(), run.clone());
    Ok(())
}

pub(super) fn update_run(store: &InMemorySchedulerStore, run: &Run) -> Result<()> {
    let mut runs = store.runs.write();
    if !runs.contains_key(&run.run_id) {
        return Err(ChrononError::RunNotFound(run.run_id.clone()));
    }
    runs.insert(run.run_id.clone(), run.clone());
    Ok(())
}

pub(super) fn get_run(store: &InMemorySchedulerStore, run_id: &str) -> Result<Option<Run>> {
    Ok(store.runs.read().get(run_id).cloned())
}

pub(super) fn list_runs_for_job(
    store: &InMemorySchedulerStore,
    job_id: &str,
    limit: usize,
) -> Result<Vec<Run>> {
    let mut runs: Vec<_> = store
        .runs
        .read()
        .values()
        .filter(|r| r.job_id.as_deref() == Some(job_id))
        .cloned()
        .collect();
    runs.sort_by_key(|b| std::cmp::Reverse(b.scheduled_for));
    runs.truncate(limit);
    Ok(runs)
}

pub(super) fn list_runs_filtered(
    store: &InMemorySchedulerStore,
    job_id: Option<&str>,
    status: Option<RunStatus>,
    offset: usize,
    limit: usize,
) -> Result<Vec<Run>> {
    let mut runs: Vec<_> = store
        .runs
        .read()
        .values()
        .filter(|r| job_id.is_none_or(|id| r.job_id.as_deref() == Some(id)))
        .filter(|r| status.is_none_or(|s| r.status == s))
        .cloned()
        .collect();
    sort_newest_first(&mut runs);
    Ok(runs.into_iter().skip(offset).take(limit).collect())
}

pub(super) fn list_runs_after(
    store: &InMemorySchedulerStore,
    job_id: Option<&str>,
    status: Option<RunStatus>,
    after: Option<&RunKey>,
    limit: usize,
) -> Result<Vec<Run>> {
    let mut runs: Vec<_> = store
        .runs
        .read()
        .values()
        .filter(|r| job_id.is_none_or(|id| r.job_id.as_deref() == Some(id)))
        .filter(|r| status.is_none_or(|s| r.status == s))
        .filter(|r| {
            after.is_none_or(|k| {
                (r.scheduled_for, r.run_id.as_str()) < (k.scheduled_for, k.run_id.as_str())
            })
        })
        .cloned()
        .collect();
    sort_newest_first(&mut runs);
    runs.truncate(limit);
    Ok(runs)
}

/// `scheduled_for` descending, then `run_id` descending (matches the SQL `ORDER BY`).
fn sort_newest_first(runs: &mut [Run]) {
    runs.sort_by(|a, b| {
        b.scheduled_for
            .cmp(&a.scheduled_for)
            .then_with(|| b.run_id.cmp(&a.run_id))
    });
}

pub(super) fn claim_next_queued(
    store: &InMemorySchedulerStore,
    pool_id: &str,
    worker_id: &str,
    now: DateTime<Utc>,
    lease_ttl_secs: i64,
) -> Result<Option<Run>> {
    let mut runs = store.runs.write();
    let mut candidate: Option<(String, Run)> = None;
    for (id, run) in runs.iter() {
        if run.status != RunStatus::Queued {
            continue;
        }
        if run.scheduled_for > now {
            continue;
        }
        if run.claim_lease_until.is_some_and(|u| u > now) {
            continue;
        }
        let pool_ok = run.pool_id.as_deref() == Some(pool_id)
            || (pool_id == "general"
                && (run.pool_id.is_none() || run.pool_id.as_deref() == Some("general")));
        if !pool_ok {
            continue;
        }
        if candidate
            .as_ref()
            .is_none_or(|(_, c)| run.scheduled_for < c.scheduled_for)
        {
            candidate = Some((id.clone(), run.clone()));
        }
    }
    let Some((run_id, mut run)) = candidate else {
        return Ok(None);
    };
    run.status = RunStatus::Claimed;
    run.claimed_by = Some(worker_id.to_string());
    run.claim_lease_until = Some(now + Duration::seconds(lease_ttl_secs));
    runs.insert(run_id, run.clone());
    Ok(Some(run))
}

pub(super) fn claim_run_by_id(
    store: &InMemorySchedulerStore,
    run_id: &str,
    pool_id: &str,
    worker_id: &str,
    now: DateTime<Utc>,
    lease_ttl_secs: i64,
) -> Result<Option<Run>> {
    let mut runs = store.runs.write();
    let Some(run) = runs.get(run_id) else {
        return Ok(None);
    };
    if run.status != RunStatus::Queued {
        return Ok(None);
    }
    if run.scheduled_for > now {
        return Ok(None);
    }
    if run.claim_lease_until.is_some_and(|u| u > now) {
        return Ok(None);
    }
    let pool_ok = run.pool_id.as_deref() == Some(pool_id)
        || (pool_id == "general"
            && (run.pool_id.is_none() || run.pool_id.as_deref() == Some("general")));
    if !pool_ok {
        return Ok(None);
    }
    let mut run = run.clone();
    run.status = RunStatus::Claimed;
    run.claimed_by = Some(worker_id.to_string());
    run.claim_lease_until = Some(now + Duration::seconds(lease_ttl_secs));
    runs.insert(run_id.to_string(), run.clone());
    Ok(Some(run))
}

pub(super) fn renew_run_lease(
    store: &InMemorySchedulerStore,
    run_id: &str,
    worker_id: &str,
    now: DateTime<Utc>,
    lease_ttl_secs: i64,
) -> Result<bool> {
    let mut runs = store.runs.write();
    let Some(run) = runs.get_mut(run_id) else {
        return Ok(false);
    };
    if run.claimed_by.as_deref() != Some(worker_id) {
        return Ok(false);
    }
    if !matches!(run.status, RunStatus::Claimed | RunStatus::Running) {
        return Ok(false);
    }
    run.claim_lease_until = Some(now + Duration::seconds(lease_ttl_secs));
    Ok(true)
}

pub(super) fn reclaim_expired_run_leases(
    store: &InMemorySchedulerStore,
    now: DateTime<Utc>,
) -> Result<Vec<String>> {
    let mut runs = store.runs.write();
    let mut reclaimed = Vec::new();
    for (id, run) in runs.iter_mut() {
        if !matches!(run.status, RunStatus::Claimed | RunStatus::Running) {
            continue;
        }
        if run.claim_lease_until.is_none_or(|u| u > now) {
            continue;
        }
        run.status = RunStatus::Queued;
        run.claimed_by = None;
        run.claim_lease_until = None;
        run.started_at = None;
        reclaimed.push(id.clone());
    }
    Ok(reclaimed)
}
