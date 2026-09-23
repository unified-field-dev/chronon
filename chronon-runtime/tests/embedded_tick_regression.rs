//! Regression test: `Embedded` deployment still ticks and enqueues due jobs after
//! `run_coordinator_tick_loop` grew a leader-gating parameter for `CoordinatorOnly`.
//!
//! Embedded passes a permanently-true `is_leader` flag, so this proves that threading
//! doesn't silently stall the embedded tick loop.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use chronon_backend_mem::InMemorySchedulerStore;
use chronon_core::store::SchedulerStore;
use chronon_core::{Job, ScheduleKind};
use chronon_runtime::ChrononBuilder;

/// Restores a prior env value (or removes the key) on drop.
struct EnvGuard {
    key: &'static str,
    previous: Option<String>,
}

impl EnvGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let previous = std::env::var(key).ok();
        std::env::set_var(key, value);
        Self { key, previous }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.previous {
            Some(value) => std::env::set_var(self.key, value),
            None => std::env::remove_var(self.key),
        }
    }
}

#[tokio::test]
async fn embedded_deployment_still_enqueues_due_jobs() {
    let _tick_guard = EnvGuard::set("CHRONON_TICK_INTERVAL_MS", "50");

    let store: Arc<dyn SchedulerStore> = Arc::new(InMemorySchedulerStore::new());
    let mut job = Job::new("embedded-regression-job", "test_script");
    job.schedule_kind = ScheduleKind::Cron;
    job.cron_expr = Some("* * * * *".to_string());
    job.next_run_at = Some(chrono::Utc::now() - chrono::Duration::seconds(1));
    job.partition_hash = Some(0);
    job.enabled = true;
    let job_id = job.job_id.clone();
    store.upsert_job(&job).await.unwrap();

    let mut chronon = ChrononBuilder::new()
        .scheduler_store(Arc::clone(&store))
        .embedded()
        .build()
        .expect("build embedded chronon");

    let shutdown = chronon.shutdown_handle();
    tokio::spawn(async move {
        let _ = chronon.run().await;
    });

    let budget = Duration::from_secs(2);
    let started = tokio::time::Instant::now();
    loop {
        let runs = store.list_runs_for_job(&job_id, 100).await.unwrap();
        if !runs.is_empty() {
            break;
        }
        assert!(
            started.elapsed() < budget,
            "embedded coordinator tick loop did not enqueue the due job within budget"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    shutdown.notify_waiters();
}
