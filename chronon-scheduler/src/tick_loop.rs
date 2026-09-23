//! Coordinator tick: claim due jobs and enqueue runs.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use chronon_core::models::{Job, MisfirePolicy, Run, ScheduleKind};
use chronon_core::store::SchedulerStore;
use chronon_core::Result;
use chronon_telemetry::TelemetrySink;
use tokio::sync::Notify;
use tokio::time::sleep;
use tracing::warn;

use crate::cron::CronExpr;
use crate::env::env_flag;
use crate::partition_assigner::PartitionAssigner;
use crate::partitioning::{self, job_execution_pool_id};

async fn count_active_runs(store: &Arc<dyn SchedulerStore>, job_id: &str) -> Result<i32> {
    let runs = store.list_runs_for_job(job_id, 10_000).await?;
    Ok(runs.iter().filter(|r| r.status.is_active()).count() as i32)
}

fn cron_next_run_at(job: &Job, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
    job.cron_expr
        .as_ref()
        .and_then(|expr| CronExpr::parse(expr, job.timezone.as_deref()).ok())
        .and_then(|c| c.next_after(after))
}

/// `max_misfire_window_secs == 0` disables gating (always enqueue). Otherwise enqueue only when
/// lateness is within the window and `run_immediately` is true.
fn should_enqueue_for_misfire(
    scheduled_for: DateTime<Utc>,
    now: DateTime<Utc>,
    policy: &MisfirePolicy,
) -> bool {
    if policy.max_misfire_window_secs == 0 {
        return true;
    }
    let lateness_secs = now
        .signed_duration_since(scheduled_for)
        .num_seconds()
        .max(0) as u64;
    if lateness_secs == 0 {
        return true;
    }
    lateness_secs <= policy.max_misfire_window_secs && policy.run_immediately
}

async fn skip_due_job(
    store: &Arc<dyn SchedulerStore>,
    telemetry: &Arc<dyn TelemetrySink>,
    job: &Job,
    job_id: &str,
    now: DateTime<Utc>,
) -> Result<bool> {
    if job.schedule_kind == ScheduleKind::RunOnce {
        if let Err(e) = store.mark_run_once_completed(job_id, now).await {
            warn!(
                job_id = %job_id,
                error = %e,
                "failed to mark run-once completed during skip"
            );
        }
    }
    let next_run_at = if job.schedule_kind == ScheduleKind::Cron {
        cron_next_run_at(job, now)
    } else {
        None
    };
    if let Err(e) = store.persist_post_tick_job_state(job_id, next_run_at).await {
        telemetry.log_event(
            "chronon_scheduler_error",
            &[("component", "tick_loop"), ("message", &e.to_string())],
        );
    }
    if let Err(e) = store.release_job_tick_claim(job_id).await {
        warn!(
            job_id = %job_id,
            error = %e,
            "failed to release job tick claim during skip"
        );
    }
    Ok(false)
}

async fn enqueue_due_job(
    store: &Arc<dyn SchedulerStore>,
    telemetry: &Arc<dyn TelemetrySink>,
    instance_id: &str,
    job_id: &str,
) -> Result<bool> {
    let now = Utc::now();
    let claim_id = format!("{instance_id}:{}", uuid::Uuid::new_v4());
    let lease_ttl = partitioning::job_claim_lease_ttl_secs();

    let Ok(true) = store
        .claim_job_for_tick(job_id, &claim_id, now, lease_ttl)
        .await
    else {
        return Ok(false);
    };

    let Some(job) = store.get_job(job_id).await? else {
        if let Err(e) = store.release_job_tick_claim(job_id).await {
            warn!(
                job_id = %job_id,
                error = %e,
                "failed to release job tick claim after missing job"
            );
        }
        return Ok(false);
    };

    let active_count = count_active_runs(store, job_id).await.unwrap_or(0);
    if active_count >= job.concurrency {
        if let Err(e) = store.release_job_tick_claim(job_id).await {
            warn!(
                job_id = %job_id,
                error = %e,
                "failed to release job tick claim at concurrency limit"
            );
        }
        return Ok(false);
    }

    let scheduled_for = job.next_run_at.unwrap_or(now);
    if !should_enqueue_for_misfire(scheduled_for, now, &job.misfire_policy()) {
        return skip_due_job(store, telemetry, &job, job_id, now).await;
    }

    if job.schedule_kind == ScheduleKind::RunOnce {
        match store
            .try_claim_run_once(job_id, instance_id, now, lease_ttl)
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                if let Err(e) = store.release_job_tick_claim(job_id).await {
                    warn!(
                        job_id = %job_id,
                        error = %e,
                        "failed to release job tick claim after run-once contention"
                    );
                }
                return Ok(false);
            }
            Err(e) => {
                telemetry.log_event(
                    "chronon_scheduler_error",
                    &[("component", "tick_loop"), ("message", &e.to_string())],
                );
                if let Err(release_err) = store.release_job_tick_claim(job_id).await {
                    warn!(
                        job_id = %job_id,
                        error = %release_err,
                        "failed to release job tick claim after run-once claim error"
                    );
                }
                return Ok(false);
            }
        }
    }

    let mut run = Run::for_job(&job.job_id, &job.script_name, scheduled_for);
    run.actor_json = job.actor_json.clone();
    run.params_json = job.params_json.clone();
    run.pool_id = Some(job_execution_pool_id(&job));

    let run_id = run.run_id.clone();
    if let Err(e) = store.create_run(&run).await {
        warn!(
            job_id = %job_id,
            run_id = %run_id,
            error = %e,
            "failed to create run during tick enqueue"
        );
        if job.schedule_kind == ScheduleKind::RunOnce {
            if let Err(release_err) = store
                .release_run_once_claim(job_id, instance_id, Utc::now())
                .await
            {
                warn!(
                    job_id = %job_id,
                    error = %release_err,
                    "failed to release run-once claim after create_run failure"
                );
            }
        }
        if let Err(release_err) = store.release_job_tick_claim(job_id).await {
            warn!(
                job_id = %job_id,
                error = %release_err,
                "failed to release job tick claim after create_run failure"
            );
        }
        return Ok(false);
    }

    if job.schedule_kind == ScheduleKind::RunOnce {
        if let Err(e) = store.mark_run_once_completed(job_id, Utc::now()).await {
            warn!(
                job_id = %job_id,
                error = %e,
                "failed to mark run-once completed after enqueue"
            );
        }
    }

    telemetry.log_event(
        "chronon_scheduler_info",
        &[
            ("component", "tick_loop"),
            (
                "message",
                &format!(
                    "enqueued run run_id={run_id} job_id={} job_name={} script={}",
                    job.job_id, job.job_name, job.script_name
                ),
            ),
        ],
    );

    let next_run_at = if job.schedule_kind == ScheduleKind::Cron {
        cron_next_run_at(&job, now)
    } else {
        None
    };

    if let Err(e) = store.persist_post_tick_job_state(job_id, next_run_at).await {
        telemetry.log_event(
            "chronon_scheduler_error",
            &[("component", "tick_loop"), ("message", &e.to_string())],
        );
    }

    Ok(true)
}

#[cfg(test)]
mod misfire_tests {
    use super::*;
    use chronon_core::models::MisfirePolicy;

    #[test]
    fn window_zero_always_enqueues() {
        let now = Utc::now();
        let past = now - chrono::Duration::hours(2);
        let policy = MisfirePolicy::default();
        assert!(should_enqueue_for_misfire(past, now, &policy));
    }

    #[test]
    fn within_window_requires_run_immediately() {
        let now = Utc::now();
        let past = now - chrono::Duration::seconds(30);
        let skip = MisfirePolicy {
            run_immediately: false,
            max_misfire_window_secs: 60,
        };
        let fire = MisfirePolicy {
            run_immediately: true,
            max_misfire_window_secs: 60,
        };
        assert!(!should_enqueue_for_misfire(past, now, &skip));
        assert!(should_enqueue_for_misfire(past, now, &fire));
    }

    #[test]
    fn outside_window_skips() {
        let now = Utc::now();
        let past = now - chrono::Duration::seconds(120);
        let policy = MisfirePolicy {
            run_immediately: true,
            max_misfire_window_secs: 60,
        };
        assert!(!should_enqueue_for_misfire(past, now, &policy));
    }
}

/// Result of one coordinator tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TickResult {
    /// Runs successfully enqueued during this tick.
    pub enqueued: usize,
    /// Due jobs discovered before claim filtering.
    pub due_count: usize,
}

/// Executes one coordinator tick: query due jobs, claim, enqueue runs, sleep.
///
/// Honors `CHRONON_DISABLE_COORDINATOR` and drains quickly when the due batch is large.
/// Updates `draining` to request shorter sleeps on subsequent ticks when backlogged.
#[tracing::instrument(
    skip(store, telemetry, assigner, draining),
    fields(instance_id = %instance_id)
)]
pub async fn run_one_tick(
    store: &Arc<dyn SchedulerStore>,
    telemetry: &Arc<dyn TelemetrySink>,
    instance_id: &str,
    assigner: &PartitionAssigner,
    draining: &mut bool,
) -> TickResult {
    if env_flag("CHRONON_DISABLE_COORDINATOR") || env_flag("CHRONON_INTERACTIVE") {
        sleep(Duration::from_secs(30)).await;
        return TickResult {
            enqueued: 0,
            due_count: 0,
        };
    }

    let tick_ms = partitioning::tick_interval_ms_from_env();
    let batch_limit = partitioning::tick_batch_limit_from_env();

    telemetry.record_counter("chronon_scheduler_ticks", &[("component", "scheduler")], 1);
    tracing::debug!(tick_ms, batch_limit, "scheduler tick started");

    let owned = assigner.owned_partitions().await;
    if owned.is_empty() {
        sleep(Duration::from_millis(500)).await;
        return TickResult {
            enqueued: 0,
            due_count: 0,
        };
    }

    let now = Utc::now();
    let lookahead_ms: i64 = if *draining { 50 } else { 0 };
    let due_until = now + chrono::Duration::milliseconds(lookahead_ms);

    let ids = match store
        .find_due_job_ids_in_partitions(&owned, due_until, batch_limit)
        .await
    {
        Ok(ids) => ids,
        Err(e) => {
            telemetry.log_event(
                "chronon_scheduler_warn",
                &[("component", "tick_loop"), ("message", &e.to_string())],
            );
            sleep(Duration::from_millis(tick_ms)).await;
            return TickResult {
                enqueued: 0,
                due_count: 0,
            };
        }
    };

    if ids.is_empty() {
        *draining = false;
        let wait = match store.min_next_run_at_in_partitions(&owned).await {
            Ok(Some(t)) => {
                let buf_ms = 20i64;
                let ms = t
                    .signed_duration_since(now)
                    .num_milliseconds()
                    .saturating_sub(buf_ms)
                    .max(0);
                Duration::from_millis(ms as u64).min(Duration::from_secs(24 * 3600))
            }
            _ => Duration::from_millis(tick_ms),
        };
        sleep(wait).await;
        return TickResult {
            enqueued: 0,
            due_count: 0,
        };
    }

    let min_next = store
        .min_next_run_at_in_partitions(&owned)
        .await
        .ok()
        .flatten();
    let soon = min_next.is_some_and(|t| t <= now + chrono::Duration::milliseconds(250));
    *draining = u32::try_from(ids.len()).unwrap_or(u32::MAX) >= batch_limit / 2 || soon;

    let due_count = ids.len();
    let mut enqueued = 0usize;
    for job_id in ids {
        if enqueue_due_job(store, telemetry, instance_id, &job_id)
            .await
            .is_ok_and(|v| v)
        {
            enqueued += 1;
        }
    }

    if *draining {
        tokio::task::yield_now().await;
    } else {
        sleep(Duration::from_millis(tick_ms)).await;
    }

    tracing::debug!(
        enqueued,
        due_count,
        draining = *draining,
        "scheduler tick finished"
    );
    TickResult {
        enqueued,
        due_count,
    }
}

/// Coordinator-only loop (never executes scripts).
///
/// Runs [`run_one_tick`] until `shutdown` is notified; used by distributed coordinator
/// processes separate from worker executors. While `is_leader` is false, sleeps and
/// rechecks instead of ticking — only the elected leader should scan for due jobs in
/// split deployments (see [`crate::leader::LeaderElector`]). Embedded hosts pass a
/// permanently-`true` flag, so their loop body is unaffected.
pub async fn run_coordinator_tick_loop(
    store: Arc<dyn SchedulerStore>,
    telemetry: Arc<dyn TelemetrySink>,
    instance_id: String,
    assigner: Arc<PartitionAssigner>,
    is_leader: Arc<AtomicBool>,
    shutdown: Arc<Notify>,
) {
    let mut draining = false;
    loop {
        if !is_leader.load(Ordering::SeqCst) {
            let standby_wait = Duration::from_millis(partitioning::tick_interval_ms_from_env());
            tokio::select! {
                () = shutdown.notified() => break,
                () = sleep(standby_wait) => {}
            }
            continue;
        }
        tokio::select! {
            () = shutdown.notified() => break,
            _ = run_one_tick(&store, &telemetry, &instance_id, &assigner, &mut draining) => {}
        }
    }
}
