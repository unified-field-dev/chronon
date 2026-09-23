//! Distributed smoke helpers for multi-worker and postgres-redis E2E.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Result};
use chrono::Utc;
use chronon_core::models::{Job, Run, RunStatus, ScheduleKind};
use chronon_core::store::SchedulerStore;
use chronon_scheduler::partition_hash_i64_for_job_id;

use crate::bootstrap::BootstrapSession;
use crate::fixtures::{register_builtin_probes, smoke_actor_json, NOOP_SCRIPT};
use crate::matrix::{DeploymentKind, MatrixSpec, StorageAdapter};

/// Whether postgres + redis env vars are set for distributed smokes.
pub fn distributed_store_available() -> bool {
    std::env::var("CHRONON_POSTGRES_URL").is_ok()
        && (std::env::var("CHRONON_REDIS_URL").is_ok()
            || std::env::var("CHRONON_TEST_REDIS_URL").is_ok())
}

fn postgres_redis_matrix() -> MatrixSpec {
    MatrixSpec {
        storage: StorageAdapter::PostgresRedis,
        deployment: DeploymentKind::CoordinatorWorker,
        ..MatrixSpec::default()
    }
}

/// Install a postgres-redis coordinator-worker session.
pub async fn install_postgres_redis_split() -> Result<BootstrapSession> {
    if !distributed_store_available() {
        bail!("CHRONON_POSTGRES_URL and CHRONON_REDIS_URL required");
    }
    let mut session = BootstrapSession::new(postgres_redis_matrix());
    session.install().await?;
    session.build_chronon()?;
    Ok(session)
}

fn queued_run(job_id: &str, pool_id: Option<&str>) -> Run {
    let now = Utc::now();
    let mut run = Run::for_job(job_id, NOOP_SCRIPT, now);
    run.pool_id = pool_id.map(str::to_string);
    run
}

/// Prefill `count` queued runs on `pool_id` for `job_id`.
pub async fn prefill_queued_runs(
    store: &dyn SchedulerStore,
    job_id: &str,
    pool_id: &str,
    count: usize,
) -> Result<()> {
    for _ in 0..count {
        let mut run = queued_run(job_id, Some(pool_id));
        run.job_id = Some(job_id.to_string());
        store.create_run(&run).await?;
    }
    Ok(())
}

/// Register noop script job used by distributed smokes.
pub async fn upsert_noop_job(store: &dyn SchedulerStore, job_name: &str) -> Result<String> {
    let mut job = Job::new(job_name, NOOP_SCRIPT);
    job.schedule_kind = ScheduleKind::Manual;
    job.actor_json = smoke_actor_json();
    job.partition_hash = Some(partition_hash_i64_for_job_id(&job.job_id));
    let job_id = job.job_id.clone();
    store.upsert_job(&job).await?;
    Ok(job_id)
}

/// Two workers drain a pre-filled queue; every run is claimed exactly once.
pub async fn dual_worker_claim_exclusive_smoke() -> Result<()> {
    let mut session = install_postgres_redis_split().await?;
    let store = session.store_dyn()?;
    let job_id = upsert_noop_job(store.as_ref(), "dual-claim-job").await?;
    let prefill = 8usize;
    prefill_queued_runs(store.as_ref(), &job_id, "general", prefill).await?;

    session.spawn_coordinator_worker_n(2).await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let runs = store.list_runs_for_job(&job_id, 100).await?;
        let claimed: Vec<_> = runs
            .iter()
            .filter(|r| r.status == RunStatus::Claimed || r.status == RunStatus::Success)
            .collect();
        if claimed.len() == prefill {
            let ids: HashSet<_> = claimed.iter().map(|r| &r.run_id).collect();
            if ids.len() != prefill {
                bail!("duplicate run claims detected");
            }
            session.shutdown_embedded().await?;
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            bail!(
                "expected {prefill} claimed/success runs, got {} (total runs {})",
                claimed.len(),
                runs.len()
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Worker on wrong pool claims nothing while correct pool drains.
pub async fn dual_worker_wrong_pool_idle_smoke() -> Result<()> {
    let mut session = install_postgres_redis_split().await?;
    let store = session.store_dyn()?;
    let job_id = upsert_noop_job(store.as_ref(), "wrong-pool-job").await?;
    prefill_queued_runs(store.as_ref(), &job_id, "workers", 4).await?;

    let telemetry = session.telemetry();
    let registry = {
        let mut r = chronon_executor::ScriptRegistry::new();
        register_builtin_probes(&mut r);
        Arc::new(r)
    };
    let store_dyn = session.store_dyn()?;

    let mut wrong_worker = chronon_runtime::ChrononBuilder::new()
        .scheduler_store(Arc::clone(&store_dyn))
        .context_factory(Arc::new(chronon_core::JsonScriptContextFactory))
        .telemetry_sink(telemetry)
        .script_registry(registry)
        .instance_id("wrong-pool-worker")
        .worker("general")
        .build()
        .map_err(|e| anyhow::anyhow!("build worker: {e}"))?;
    let stop = wrong_worker.shutdown_handle();
    let task = tokio::spawn(async move { wrong_worker.run().await });

    tokio::time::sleep(Duration::from_secs(2)).await;

    let runs = store.list_runs_for_job(&job_id, 100).await?;
    let claimed = runs
        .iter()
        .filter(|r| r.status == RunStatus::Claimed || r.status == RunStatus::Success)
        .count();
    if claimed != 0 {
        bail!("wrong-pool worker claimed {claimed} runs, expected 0");
    }

    stop.notify_waiters();
    let _ = task.await;
    session.shutdown_embedded().await?;
    Ok(())
}

/// Second coordinator instance cannot acquire leader lease.
pub async fn coordinator_leader_exclusive_smoke() -> Result<()> {
    let session = install_postgres_redis_split().await?;
    let store = session.store_dyn()?;
    assert!(store.try_acquire_leader("coord-a", 30).await?);
    assert!(!store.try_acquire_leader("coord-b", 30).await?);
    Ok(())
}

/// Two real `CoordinatorOnly` replicas against postgres-redis: only the elected leader
/// ticks, and the survivor resumes ticking after the leader is killed.
///
/// Unlike [`coordinator_leader_exclusive_smoke`] (which only exercises the store-level
/// lease CAS), this drives the actual `ChrononBuilder` + `LeaderElector` +
/// `run_coordinator_tick_loop` production wiring against the shared postgres-redis store,
/// validating the same failover contract `BM-CH4` measures — but as a pass/fail assertion,
/// not a latency sample.
pub async fn coordinator_failover_postgres_redis_smoke() -> Result<()> {
    if !distributed_store_available() {
        bail!("CHRONON_POSTGRES_URL and CHRONON_REDIS_URL required");
    }
    let mut session = BootstrapSession::new(postgres_redis_matrix());
    session.install().await?;
    let store = session.store_dyn()?;
    let telemetry = session.telemetry();

    let iso = uuid::Uuid::new_v4().simple().to_string();
    let mut coord_a = chronon_runtime::ChrononBuilder::new()
        .scheduler_store(Arc::clone(&store))
        .telemetry_sink(telemetry.clone())
        .instance_id(format!("failover-a-{iso}"))
        .coordinator_only()
        .build()
        .map_err(|e| anyhow::anyhow!("build coord-a: {e}"))?;
    let mut coord_b = chronon_runtime::ChrononBuilder::new()
        .scheduler_store(Arc::clone(&store))
        .telemetry_sink(telemetry)
        .instance_id(format!("failover-b-{iso}"))
        .coordinator_only()
        .build()
        .map_err(|e| anyhow::anyhow!("build coord-b: {e}"))?;

    let shutdown_a = coord_a.shutdown_handle();
    let shutdown_b = coord_b.shutdown_handle();
    tokio::spawn(async move {
        let _ = coord_a.run().await;
    });
    tokio::spawn(async move {
        let _ = coord_b.run().await;
    });

    // Let leadership settle (postgres round trips are slower than the in-memory backend).
    tokio::time::sleep(Duration::from_secs(2)).await;
    let leader = store
        .get_leader()
        .await?
        .ok_or_else(|| anyhow::anyhow!("no leader elected"))?;
    let (leader_shutdown, survivor_shutdown) =
        if leader.leader_instance_id.starts_with("failover-a") {
            (&shutdown_a, &shutdown_b)
        } else {
            (&shutdown_b, &shutdown_a)
        };

    // Kill the leader, then seed a due job only after that so any run for it is
    // unambiguously attributable to the survivor taking over.
    leader_shutdown.notify_waiters();
    let job = crate::fixtures::upsert_immediate_cron_job(
        store.as_ref(),
        &format!("failover-job-{iso}"),
        NOOP_SCRIPT,
        "* * * * *",
    )
    .await?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if !store.list_runs_for_job(&job.job_id, 10).await?.is_empty() {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            survivor_shutdown.notify_waiters();
            bail!("surviving replica did not resume ticking within budget");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    survivor_shutdown.notify_waiters();
    Ok(())
}

/// Coordinator tick enqueues runs; two workers claim via hybrid store.
pub async fn postgres_redis_hybrid_claim_roundtrip_smoke() -> Result<()> {
    use crate::fixtures::upsert_immediate_cron_job;

    let mut session = install_postgres_redis_split().await?;
    let store = session.store_dyn()?;
    let mut job = upsert_immediate_cron_job(
        store.as_ref(),
        "hybrid-roundtrip",
        NOOP_SCRIPT,
        "0 * * * * *",
    )
    .await?;
    job.actor_json = smoke_actor_json();
    store.upsert_job(&job).await?;

    session.init_partitions().await?;
    session.tick_once().await?;
    session.spawn_workers_n(2).await?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let runs = store.list_runs_for_job(&job.job_id, 100).await?;
        if runs.is_empty() {
            if tokio::time::Instant::now() >= deadline {
                bail!("expected at least one run after tick");
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        }
        let terminal = runs
            .iter()
            .filter(|r| r.status == RunStatus::Success || r.status == RunStatus::Claimed)
            .count();
        if terminal > 0 {
            session.shutdown_embedded().await?;
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("expected claimed or success run after hybrid roundtrip");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
