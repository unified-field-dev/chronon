//! Shared [`SchedulerStore`](chronon_core::store::SchedulerStore) contract checks for backend adapters.

use std::sync::Arc;

use chrono::{Duration, Utc};
use chronon_core::models::{
    Job, JobRevision, Run, RunStatus, ScheduleKind, Script, Worker, WorkerStatus,
};
use chronon_core::store::SchedulerStore;
use chronon_core::{ChrononError, Result};
use serde_json::json;

fn cron_job_due(name: &str, partition_hash: i64) -> Job {
    let mut job = Job::new(name, "script-a");
    job.schedule_kind = ScheduleKind::Cron;
    job.cron_expr = Some("0 * * * * *".into());
    job.next_run_at = Some(Utc::now() - Duration::seconds(1));
    job.partition_hash = Some(partition_hash);
    job
}

fn queued_run(job_id: &str, pool_id: Option<&str>, scheduled_for: chrono::DateTime<Utc>) -> Run {
    let mut run = Run::for_job(job_id, "script-a", scheduled_for);
    run.pool_id = pool_id.map(str::to_string);
    run
}

/// Minimal port contract every storage adapter must satisfy before matrix E2E.
pub async fn run_store_contract(store: Arc<dyn SchedulerStore>) -> Result<()> {
    let store_ref = store.as_ref();
    upsert_job_roundtrip_by_name(store_ref).await?;
    upsert_job_rejects_taken_name(store_ref).await?;
    delete_job_removes_row_and_frees_name(store_ref).await?;
    list_due_jobs_respects_enabled_and_next_run_at(store_ref).await?;
    claim_job_for_tick_exclusive_then_reclaim_after_release(store_ref).await?;
    find_due_respects_partition_filter(store_ref).await?;
    find_due_skips_completed_run_once(store_ref).await?;
    try_claim_run_once_lease_contention(store_ref).await?;
    claim_next_queued_orders_by_scheduled_for_and_filters_pool(store_ref).await?;
    renew_run_lease_requires_matching_worker(store_ref).await?;
    reclaim_expired_running_run_allows_reclaim(store_ref).await?;
    reclaim_skips_active_non_expired_lease(store_ref).await?;
    leader_election_blocks_second_instance(store_ref).await?;
    pause_and_resume_job(store_ref).await?;
    script_roundtrip(store_ref).await?;
    revision_append_and_list(store_ref).await?;
    worker_register_and_heartbeat(store_ref).await?;
    run_once_double_claim_rejected(store_ref).await?;
    concurrent_claim_next_queued_exclusive(store).await?;
    Ok(())
}

async fn upsert_job_roundtrip_by_name(store: &dyn SchedulerStore) -> Result<()> {
    let job = Job::new("nightly", "cleanup");
    let job_id = job.job_id.clone();

    store.upsert_job(&job).await?;
    let by_id = store.get_job(&job_id).await?.expect("found");
    assert_eq!(by_id.job_name, "nightly");

    let by_name = store.get_job_by_name("nightly").await?.expect("found");
    assert_eq!(by_name.job_id, job_id);
    Ok(())
}

async fn upsert_job_rejects_taken_name(store: &dyn SchedulerStore) -> Result<()> {
    let first = cron_job_due("name-taken", 0);
    store.upsert_job(&first).await?;

    let mut updated = first.clone();
    updated.current_revision += 1;
    store.upsert_job(&updated).await?;
    let stored = store.get_job(&first.job_id).await?.expect("found");
    assert_eq!(stored.current_revision, updated.current_revision);

    let second = cron_job_due("name-taken", 0);
    let err = store
        .upsert_job(&second)
        .await
        .expect_err("second job under a taken name");
    assert!(
        matches!(
            err,
            ChrononError::JobNameTaken { ref job_name, ref existing_job_id }
                if job_name == "name-taken" && *existing_job_id == first.job_id
        ),
        "unexpected error: {err}"
    );
    assert!(store.get_job(&second.job_id).await?.is_none());
    let by_name = store.get_job_by_name("name-taken").await?.expect("found");
    assert_eq!(by_name.job_id, first.job_id);
    let named = store
        .list_jobs()
        .await?
        .iter()
        .filter(|j| j.job_name == "name-taken")
        .count();
    assert_eq!(named, 1);

    assert!(store.delete_job(&first.job_id).await?);
    Ok(())
}

async fn delete_job_removes_row_and_frees_name(store: &dyn SchedulerStore) -> Result<()> {
    let job = cron_job_due("delete-me", 0);
    let job_id = job.job_id.clone();
    store.upsert_job(&job).await?;
    store
        .append_revision(&JobRevision::new(&job_id, 1, json!(null), json!({})))
        .await?;

    assert!(store.delete_job(&job_id).await?);
    assert!(store.get_job(&job_id).await?.is_none());
    assert!(store.get_job_by_name("delete-me").await?.is_none());
    assert!(store
        .list_due_jobs(Utc::now())
        .await?
        .iter()
        .all(|j| j.job_id != job_id));
    assert!(store.list_revisions(&job_id).await?.is_empty());
    assert!(!store.delete_job(&job_id).await?);

    let reused = cron_job_due("delete-me", 0);
    store.upsert_job(&reused).await?;
    let by_name = store.get_job_by_name("delete-me").await?.expect("found");
    assert_eq!(by_name.job_id, reused.job_id);
    assert!(store.delete_job(&reused.job_id).await?);
    Ok(())
}

async fn list_due_jobs_respects_enabled_and_next_run_at(store: &dyn SchedulerStore) -> Result<()> {
    let now = Utc::now();

    let mut due = Job::new("due", "s1");
    due.next_run_at = Some(now - Duration::seconds(5));
    store.upsert_job(&due).await?;

    let mut future = Job::new("future", "s1");
    future.next_run_at = Some(now + Duration::hours(1));
    store.upsert_job(&future).await?;

    let mut paused = Job::new("paused", "s1");
    paused.next_run_at = Some(now - Duration::seconds(5));
    paused.enabled = false;
    store.upsert_job(&paused).await?;

    let due_jobs = store.list_due_jobs(now).await?;
    let names: Vec<_> = due_jobs.iter().map(|j| j.job_name.as_str()).collect();
    assert!(names.contains(&"due"));
    assert!(!names.contains(&"future"));
    assert!(!names.contains(&"paused"));
    Ok(())
}

async fn claim_job_for_tick_exclusive_then_reclaim_after_release(
    store: &dyn SchedulerStore,
) -> Result<()> {
    let job = cron_job_due("tick-job", 0);
    let job_id = job.job_id.clone();
    store.upsert_job(&job).await?;

    let now = Utc::now();
    let ok_a = store
        .claim_job_for_tick(&job_id, "claim-a", now, 60)
        .await?;
    let ok_b = store
        .claim_job_for_tick(&job_id, "claim-b", now, 60)
        .await?;
    assert!(ok_a);
    assert!(!ok_b);

    store.release_job_tick_claim(&job_id).await?;
    let ok_c = store
        .claim_job_for_tick(&job_id, "claim-c", now, 60)
        .await?;
    assert!(ok_c);
    Ok(())
}

async fn find_due_respects_partition_filter(store: &dyn SchedulerStore) -> Result<()> {
    let job = cron_job_due("partitioned", 2);
    let job_id = job.job_id.clone();
    store.upsert_job(&job).await?;

    let until = Utc::now() + Duration::milliseconds(50);
    let due = store
        .find_due_job_ids_in_partitions(&[2], until, 50)
        .await?;
    assert!(due.contains(&job_id));

    let other = store
        .find_due_job_ids_in_partitions(&[0, 1, 3], until, 50)
        .await?;
    assert!(!other.contains(&job_id));
    Ok(())
}

async fn find_due_skips_completed_run_once(store: &dyn SchedulerStore) -> Result<()> {
    let mut job = cron_job_due("run-once", 1);
    job.schedule_kind = ScheduleKind::RunOnce;
    let job_id = job.job_id.clone();
    store.upsert_job(&job).await?;

    store.mark_run_once_completed(&job_id, Utc::now()).await?;

    let until = Utc::now() + Duration::milliseconds(50);
    let due = store
        .find_due_job_ids_in_partitions(&[1], until, 50)
        .await?;
    assert!(!due.contains(&job_id));
    Ok(())
}

async fn try_claim_run_once_lease_contention(store: &dyn SchedulerStore) -> Result<()> {
    let mut job = Job::new("once", "s1");
    job.schedule_kind = ScheduleKind::RunOnce;
    let job_id = job.job_id.clone();
    store.upsert_job(&job).await?;

    let now = Utc::now();
    assert!(
        store
            .try_claim_run_once(&job_id, "coord-a", now, 60)
            .await?
    );
    assert!(
        !store
            .try_claim_run_once(&job_id, "coord-b", now, 60)
            .await?
    );

    store.mark_run_once_completed(&job_id, Utc::now()).await?;
    assert!(
        !store
            .try_claim_run_once(&job_id, "coord-c", now, 60)
            .await?
    );
    Ok(())
}

async fn claim_next_queued_orders_by_scheduled_for_and_filters_pool(
    store: &dyn SchedulerStore,
) -> Result<()> {
    let now = Utc::now();

    let early = queued_run("job-1", Some("workers"), now - Duration::minutes(2));
    let late = queued_run("job-1", Some("workers"), now - Duration::minutes(1));
    let other_pool = queued_run("job-1", Some("other"), now - Duration::hours(1));

    store.create_run(&late).await?;
    store.create_run(&early).await?;
    store.create_run(&other_pool).await?;

    let claimed = store
        .claim_next_queued("workers", "worker-1", now, 30)
        .await?
        .expect("claimed");
    assert_eq!(claimed.run_id, early.run_id);
    assert_eq!(claimed.status, RunStatus::Claimed);
    assert_eq!(claimed.claimed_by.as_deref(), Some("worker-1"));

    let general_run = queued_run("job-2", None, now);
    store.create_run(&general_run).await?;
    let from_general = store
        .claim_next_queued("general", "worker-2", now, 30)
        .await?
        .expect("general pool");
    assert_eq!(from_general.run_id, general_run.run_id);
    Ok(())
}

async fn renew_run_lease_requires_matching_worker(store: &dyn SchedulerStore) -> Result<()> {
    let now = Utc::now();
    let run = queued_run("job-1", Some("pool"), now);
    let run_id = run.run_id.clone();
    store.create_run(&run).await?;

    store.claim_next_queued("pool", "worker-a", now, 30).await?;

    assert!(store.renew_run_lease(&run_id, "worker-a", now, 60).await?);
    assert!(!store.renew_run_lease(&run_id, "worker-b", now, 60).await?);
    Ok(())
}

async fn reclaim_expired_running_run_allows_reclaim(store: &dyn SchedulerStore) -> Result<()> {
    let now = Utc::now();
    let mut run = queued_run("job-reclaim", Some("pool"), now);
    let run_id = run.run_id.clone();
    store.create_run(&run).await?;

    let claimed = store
        .claim_next_queued("pool", "worker-a", now, 30)
        .await?
        .expect("claimed");
    assert_eq!(claimed.run_id, run_id);

    run = store.get_run(&run_id).await?.expect("run");
    run.status = RunStatus::Running;
    run.claim_lease_until = Some(now - Duration::seconds(1));
    store.update_run(&run).await?;

    let reclaimed = store.reclaim_expired_run_leases(now).await?;
    assert!(
        reclaimed.iter().any(|id| id == &run_id),
        "expected {run_id} in {reclaimed:?}"
    );

    let again = store
        .claim_next_queued("pool", "worker-b", now, 30)
        .await?
        .expect("reclaimed run claimable");
    assert_eq!(again.run_id, run_id);
    assert_eq!(again.status, RunStatus::Claimed);
    assert_eq!(again.claimed_by.as_deref(), Some("worker-b"));
    Ok(())
}

async fn reclaim_skips_active_non_expired_lease(store: &dyn SchedulerStore) -> Result<()> {
    let now = Utc::now();
    let run = queued_run("job-active-lease", Some("pool"), now);
    let run_id = run.run_id.clone();
    store.create_run(&run).await?;

    store
        .claim_next_queued("pool", "worker-a", now, 300)
        .await?
        .expect("claimed");

    let reclaimed = store.reclaim_expired_run_leases(now).await?;
    assert!(
        !reclaimed.iter().any(|id| id == &run_id),
        "must not steal active lease"
    );

    assert!(store.renew_run_lease(&run_id, "worker-a", now, 300).await?);
    Ok(())
}

async fn leader_election_blocks_second_instance(store: &dyn SchedulerStore) -> Result<()> {
    assert!(store.try_acquire_leader("inst-a", 30).await?);
    assert!(!store.try_acquire_leader("inst-b", 30).await?);

    store.renew_leader_lease("inst-a", 30).await?;
    let leader = store.get_leader().await?.expect("leader");
    assert_eq!(leader.leader_instance_id, "inst-a");
    Ok(())
}

async fn pause_and_resume_job(store: &dyn SchedulerStore) -> Result<()> {
    let job = Job::new("pausable", "s1");
    let job_id = job.job_id.clone();
    store.upsert_job(&job).await?;

    store.pause_job(&job_id).await?;
    let paused = store.get_job(&job_id).await?.expect("job");
    assert!(!paused.enabled);

    store.resume_job(&job_id).await?;
    let resumed = store.get_job(&job_id).await?.expect("job");
    assert!(resumed.enabled);
    Ok(())
}

async fn script_roundtrip(store: &dyn SchedulerStore) -> Result<()> {
    let script = Script::new("cleanup", json!({"type": "object"}), "hash-v1".into());
    let script_name = script.script_name.clone();
    store.upsert_script(&script).await?;

    let fetched = store.get_script(&script_name).await?.expect("script");
    assert_eq!(fetched.signature_hash, "hash-v1");
    Ok(())
}

async fn revision_append_and_list(store: &dyn SchedulerStore) -> Result<()> {
    let job = Job::new("rev-job", "s1");
    let job_id = job.job_id.clone();
    store.upsert_job(&job).await?;

    let revision = JobRevision::new(
        &job_id,
        1,
        json!({"actor": "test"}),
        json!({"job_name": "rev-job"}),
    );
    store.append_revision(&revision).await?;

    let revisions = store.list_revisions(&job_id).await?;
    assert_eq!(revisions.len(), 1);
    assert_eq!(revisions[0].revision_number, 1);
    Ok(())
}

async fn worker_register_and_heartbeat(store: &dyn SchedulerStore) -> Result<()> {
    let now = Utc::now();
    let worker = Worker {
        worker_id: "worker-1".into(),
        pool_id: "general".into(),
        cell_id: None,
        status: WorkerStatus::Online,
        last_heartbeat_at: now,
        capacity_json: None,
        created_at: now,
        updated_at: now,
    };
    store.register_worker(&worker).await?;

    let later = now + Duration::seconds(5);
    store.heartbeat_worker("worker-1", later).await?;
    Ok(())
}

async fn run_once_double_claim_rejected(store: &dyn SchedulerStore) -> Result<()> {
    let mut job = Job::new("once-claim", "s1");
    job.schedule_kind = ScheduleKind::RunOnce;
    let job_id = job.job_id.clone();
    store.upsert_job(&job).await?;

    let now = Utc::now();
    assert!(
        store
            .try_claim_run_once(&job_id, "coord-a", now, 60)
            .await?
    );
    assert!(
        !store
            .try_claim_run_once(&job_id, "coord-b", now, 60)
            .await?
    );
    Ok(())
}

async fn concurrent_claim_next_queued_exclusive(store: Arc<dyn SchedulerStore>) -> Result<()> {
    use std::collections::HashSet;

    let now = Utc::now();
    let prefill = 16usize;
    for i in 0..prefill {
        let run = queued_run(
            "concurrent-job",
            Some("general"),
            now - Duration::seconds(i as i64),
        );
        store.create_run(&run).await?;
    }

    let mut handles = Vec::new();
    for worker_idx in 0..4u32 {
        let store = Arc::clone(&store);
        let worker_id = format!("concurrent-worker-{worker_idx}");
        handles.push(tokio::spawn(async move {
            let mut claimed = Vec::new();
            loop {
                match store
                    .claim_next_queued("general", &worker_id, Utc::now(), 30)
                    .await
                {
                    Ok(Some(run)) => claimed.push(run.run_id),
                    Ok(None) => break,
                    Err(e) => return Err(e),
                }
            }
            Ok::<_, chronon_core::ChrononError>(claimed)
        }));
    }

    let mut all_run_ids = Vec::new();
    for handle in handles {
        let ids = handle
            .await
            .map_err(|e| chronon_core::ChrononError::Internal(format!("join: {e}")))??;
        all_run_ids.extend(ids);
    }

    assert_eq!(
        all_run_ids.len(),
        prefill,
        "expected {prefill} claims, got {}",
        all_run_ids.len()
    );
    let unique: HashSet<_> = all_run_ids.iter().collect();
    assert_eq!(unique.len(), prefill, "duplicate run_id claims detected");
    Ok(())
}
