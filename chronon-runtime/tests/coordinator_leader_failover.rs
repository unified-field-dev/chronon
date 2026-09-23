//! Two `CoordinatorOnly` replicas sharing one store: only the leader ticks, and a
//! standby resumes ticking after the leader is shut down.

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

fn due_job(job_name: &str) -> Job {
    let mut job = Job::new(job_name, "test_script");
    job.schedule_kind = ScheduleKind::Cron;
    job.cron_expr = Some("* * * * *".to_string());
    job.next_run_at = Some(chrono::Utc::now() - chrono::Duration::seconds(1));
    job.partition_hash = Some(0);
    job.enabled = true;
    job
}

#[tokio::test]
async fn standby_coordinator_does_not_enqueue_due_jobs() {
    let _ttl_guard = EnvGuard::set("CHRONON_LEADER_TTL_S", "2");
    let _renew_guard = EnvGuard::set("CHRONON_LEADER_RENEW_S", "1");
    let _tick_guard = EnvGuard::set("CHRONON_TICK_INTERVAL_MS", "50");

    let store: Arc<dyn SchedulerStore> = Arc::new(InMemorySchedulerStore::new());
    let job = due_job("job-standby-test");
    let job_id = job.job_id.clone();
    store.upsert_job(&job).await.unwrap();

    let mut chronon_a = ChrononBuilder::new()
        .scheduler_store(Arc::clone(&store))
        .instance_id("coord-a")
        .coordinator_only()
        .build()
        .expect("build coord-a");
    let mut chronon_b = ChrononBuilder::new()
        .scheduler_store(Arc::clone(&store))
        .instance_id("coord-b")
        .coordinator_only()
        .build()
        .expect("build coord-b");

    let shutdown_a = chronon_a.shutdown_handle();
    let shutdown_b = chronon_b.shutdown_handle();
    tokio::spawn(async move {
        let _ = chronon_a.run().await;
    });
    tokio::spawn(async move {
        let _ = chronon_b.run().await;
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    let runs = store.list_runs_for_job(&job_id, 100).await.unwrap();
    assert_eq!(
        runs.len(),
        1,
        "expected exactly one run enqueued once, by whichever replica won leadership"
    );

    shutdown_a.notify_waiters();
    shutdown_b.notify_waiters();
}

#[tokio::test]
async fn leader_failover_resumes_ticking_within_budget() {
    let ttl_secs = 2u64;
    let _ttl_guard = EnvGuard::set("CHRONON_LEADER_TTL_S", &ttl_secs.to_string());
    let _renew_guard = EnvGuard::set("CHRONON_LEADER_RENEW_S", "1");
    let _tick_guard = EnvGuard::set("CHRONON_TICK_INTERVAL_MS", "50");

    let store: Arc<dyn SchedulerStore> = Arc::new(InMemorySchedulerStore::new());

    let mut chronon_a = ChrononBuilder::new()
        .scheduler_store(Arc::clone(&store))
        .instance_id("coord-a")
        .coordinator_only()
        .build()
        .expect("build coord-a");
    let mut chronon_b = ChrononBuilder::new()
        .scheduler_store(Arc::clone(&store))
        .instance_id("coord-b")
        .coordinator_only()
        .build()
        .expect("build coord-b");

    let shutdown_a = chronon_a.shutdown_handle();
    let shutdown_b = chronon_b.shutdown_handle();
    tokio::spawn(async move {
        let _ = chronon_a.run().await;
    });
    tokio::spawn(async move {
        let _ = chronon_b.run().await;
    });

    // Let leadership settle, then find out who won.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let leader = store
        .get_leader()
        .await
        .unwrap()
        .expect("a leader should have been elected");
    let leader_shutdown = if leader.leader_instance_id == "coord-a" {
        &shutdown_a
    } else {
        &shutdown_b
    };

    // Kill the leader (simulates a crash) and seed a job only after that, so any run for
    // it is unambiguously attributable to the surviving replica taking over.
    leader_shutdown.notify_waiters();
    let failover_job = due_job("job-failover-test");
    let failover_job_id = failover_job.job_id.clone();
    store.upsert_job(&failover_job).await.unwrap();

    let budget = Duration::from_secs(ttl_secs * 2);
    let started = tokio::time::Instant::now();
    loop {
        let runs = store
            .list_runs_for_job(&failover_job_id, 100)
            .await
            .unwrap();
        if !runs.is_empty() {
            break;
        }
        assert!(
            started.elapsed() < budget,
            "surviving replica did not resume ticking within budget"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    shutdown_a.notify_waiters();
    shutdown_b.notify_waiters();
}
