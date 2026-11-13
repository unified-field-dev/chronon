//! Keyset paging contract for [`SchedulerStore::list_runs_after`].
//!
//! Run it against a fresh store: the assertions count every run in the store.

use std::collections::HashSet;

use chrono::{DateTime, Duration, TimeZone, Utc};
use chronon_core::models::{Run, RunKey, RunStatus};
use chronon_core::store::SchedulerStore;
use chronon_core::{ChrononError, Result};
use uuid::Uuid;

const RUN_COUNT: usize = 2_500;
const PAGE: usize = 100;
/// Runs that share one `scheduled_for`, so the `run_id` tie-break is exercised on every page.
const RUNS_PER_TIMESTAMP: usize = 5;
const STATUSES: [RunStatus; 5] = [
    RunStatus::Queued,
    RunStatus::Running,
    RunStatus::Success,
    RunStatus::Failed,
    RunStatus::Canceled,
];

/// Seed 2,500 runs across two jobs and mixed statuses, then check that paging with
/// `after = last row's key` returns every matching run once, in order, and that runs
/// inserted mid-scan are returned only when they sort after the cursor.
///
/// Expects an empty store.
pub async fn run_list_runs_after_contract(store: &dyn SchedulerStore) -> Result<()> {
    let job_a = format!("keyset-a-{}", Uuid::new_v4().simple());
    let job_b = format!("keyset-b-{}", Uuid::new_v4().simple());
    // Whole seconds keep timestamps exact on every backend (Postgres stores microseconds).
    let base = Utc
        .timestamp_opt(1_900_000_000, 0)
        .single()
        .ok_or_else(|| ChrononError::Internal("base timestamp".into()))?;

    let mut seeded = Vec::with_capacity(RUN_COUNT);
    for i in 0..RUN_COUNT {
        let job = if i % 2 == 0 { &job_a } else { &job_b };
        let offset = i64::try_from(i / RUNS_PER_TIMESTAMP).unwrap_or(i64::MAX);
        let mut run = Run::for_job(job, "script-a", base - Duration::seconds(offset));
        run.status = STATUSES[(i / 3) % STATUSES.len()];
        seeded.push(run);
    }
    // Insert out of order (7919 is coprime with 2,500) so storage order differs from sort order.
    for i in 0..RUN_COUNT {
        store.create_run(&seeded[(i * 7919) % RUN_COUNT]).await?;
    }

    let filters: [(Option<&str>, Option<RunStatus>); 4] = [
        (None, None),
        (Some(&job_a), None),
        (None, Some(RunStatus::Success)),
        (Some(&job_b), Some(RunStatus::Failed)),
    ];
    for (job_id, status) in filters {
        let expected = expected_ids(&seeded, job_id, status);
        let got = scan(store, job_id, status).await?;
        for run in &got {
            assert!(
                job_id.is_none_or(|id| run.job_id.as_deref() == Some(id)),
                "job filter {job_id:?} returned run of job {:?}",
                run.job_id
            );
            assert!(
                status.is_none_or(|s| run.status == s),
                "status filter {status:?} returned {:?}",
                run.status
            );
        }
        let got_ids: Vec<&str> = got.iter().map(|r| r.run_id.as_str()).collect();
        assert_eq!(
            got_ids.len(),
            expected.len(),
            "row count for job {job_id:?} status {status:?}"
        );
        assert_eq!(
            got_ids, expected,
            "order for job {job_id:?} status {status:?}"
        );
    }

    mid_scan_inserts(store, &seeded, base).await
}

/// Ids of `runs` matching the filters, sorted newest first with the `run_id` tie-break.
fn expected_ids<'a>(
    runs: &'a [Run],
    job_id: Option<&str>,
    status: Option<RunStatus>,
) -> Vec<&'a str> {
    let mut matching: Vec<&Run> = runs
        .iter()
        .filter(|r| job_id.is_none_or(|id| r.job_id.as_deref() == Some(id)))
        .filter(|r| status.is_none_or(|s| r.status == s))
        .collect();
    matching.sort_by(|a, b| {
        b.scheduled_for
            .cmp(&a.scheduled_for)
            .then_with(|| b.run_id.cmp(&a.run_id))
    });
    matching.iter().map(|r| r.run_id.as_str()).collect()
}

/// Read pages of [`PAGE`] until a short page, using the last row's key as the next cursor.
async fn scan(
    store: &dyn SchedulerStore,
    job_id: Option<&str>,
    status: Option<RunStatus>,
) -> Result<Vec<Run>> {
    let mut all = Vec::new();
    let mut after: Option<RunKey> = None;
    loop {
        let page = store
            .list_runs_after(job_id, status, after.as_ref(), PAGE)
            .await?;
        assert!(page.len() <= PAGE, "page larger than limit");
        let done = page.len() < PAGE;
        after = page.last().map(RunKey::from).or(after);
        all.extend(page);
        if done {
            return Ok(all);
        }
    }
}

async fn mid_scan_inserts(
    store: &dyn SchedulerStore,
    seeded: &[Run],
    base: DateTime<Utc>,
) -> Result<()> {
    let mut seen: Vec<Run> = Vec::new();
    let mut after: Option<RunKey> = None;
    for _ in 0..3 {
        let page = store
            .list_runs_after(None, None, after.as_ref(), PAGE)
            .await?;
        assert_eq!(page.len(), PAGE);
        after = page.last().map(RunKey::from);
        seen.extend(page);
    }
    let key = after
        .clone()
        .ok_or_else(|| ChrononError::Internal("no cursor after three pages".into()))?;
    assert!(
        key.scheduled_for < base,
        "cursor should be past the newest run"
    );

    let newer = Run::for_job(
        "keyset-mid",
        "script-a",
        key.scheduled_for + Duration::seconds(1),
    );
    let older = Run::for_job(
        "keyset-mid",
        "script-a",
        key.scheduled_for - Duration::milliseconds(500),
    );
    store.create_run(&newer).await?;
    store.create_run(&older).await?;

    loop {
        let page = store
            .list_runs_after(None, None, after.as_ref(), PAGE)
            .await?;
        let done = page.len() < PAGE;
        after = page.last().map(RunKey::from).or(after);
        seen.extend(page);
        if done {
            break;
        }
    }

    let ids: Vec<&str> = seen.iter().map(|r| r.run_id.as_str()).collect();
    assert!(
        !ids.contains(&newer.run_id.as_str()),
        "run newer than the cursor must not be returned by later pages"
    );
    assert!(
        ids.contains(&older.run_id.as_str()),
        "run older than the cursor must be returned by later pages"
    );
    let unique: HashSet<&str> = ids.iter().copied().collect();
    assert_eq!(unique.len(), ids.len(), "a run was returned twice");
    assert_eq!(
        ids.len(),
        seeded.len() + 1,
        "every seeded run plus the older insert"
    );
    for pair in seen.windows(2) {
        let (a, b) = (RunKey::from(&pair[0]), RunKey::from(&pair[1]));
        assert!(
            (b.scheduled_for, b.run_id.as_str()) < (a.scheduled_for, a.run_id.as_str()),
            "rows out of order: {a:?} then {b:?}"
        );
    }
    Ok(())
}
