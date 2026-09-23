//! Embedded deployment: coordinator tick + worker in one process.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use chronon_executor::Executor;
use chronon_scheduler::{run_coordinator_tick_loop, Scheduler};
use chronon_telemetry::TelemetrySink;
use tokio::sync::Notify;

use crate::worker::run_worker_loop;

/// Run coordinator tick and worker loops in one process until `shutdown` is notified.
///
/// Called from [`Chronon::run`](crate::Chronon::run) for [`DeploymentShape::Embedded`](crate::DeploymentShape::Embedded).
pub async fn run_embedded_loops(
    scheduler: Arc<Scheduler>,
    executor: Arc<Executor>,
    telemetry: Arc<dyn TelemetrySink>,
    shutdown: Arc<Notify>,
) {
    scheduler.init_partitions().await;

    let store = scheduler.store();
    let instance_id = scheduler.instance_id().to_string();
    let assigner = scheduler.assigner();
    let pool = chronon_scheduler::worker_pool_from_env();

    let tick_shutdown = Arc::clone(&shutdown);
    let tick_store = Arc::clone(&store);
    let tick_telemetry = Arc::clone(&telemetry);
    let tick_instance = instance_id.clone();
    let tick_assigner = Arc::clone(&assigner);
    // Embedded hosts are always the sole ticker in their process; leadership doesn't apply.
    let tick_is_leader = Arc::new(AtomicBool::new(true));
    tokio::spawn(async move {
        run_coordinator_tick_loop(
            tick_store,
            tick_telemetry,
            tick_instance,
            tick_assigner,
            tick_is_leader,
            tick_shutdown,
        )
        .await;
    });

    run_worker_loop(store, executor, telemetry, pool, instance_id, shutdown).await;
}
