//! Coordinator-only deployment: leader election + tick loop without workers.

use std::sync::Arc;

use chronon_scheduler::{run_coordinator_tick_loop, LeaderElector, Scheduler};
use chronon_telemetry::TelemetrySink;
use tokio::sync::Notify;

/// Run leader election and coordinator tick loop until `shutdown` is notified.
///
/// Called from [`Chronon::run`](crate::Chronon::run) for [`DeploymentShape::CoordinatorOnly`](crate::DeploymentShape::CoordinatorOnly).
/// Only the elected leader ticks (owns every partition and scans for due jobs); other
/// `CoordinatorOnly` replicas stay on standby, campaigning for the lease, ready to take
/// over ticking automatically if the leader's lease lapses.
pub async fn run_coordinator_loops(
    scheduler: Arc<Scheduler>,
    telemetry: Arc<dyn TelemetrySink>,
    shutdown: Arc<Notify>,
) {
    let assigner = scheduler.assigner();

    let elector = Arc::new(LeaderElector::new(
        scheduler.store(),
        Arc::clone(&telemetry),
        scheduler.instance_id().to_string(),
    ));
    let is_leader = elector.is_leader_flag();

    let election_shutdown = Arc::clone(&shutdown);
    let election_assigner = Arc::clone(&assigner);
    tokio::spawn(async move {
        elector
            .run_election_loop(election_assigner, election_shutdown)
            .await;
    });

    run_coordinator_tick_loop(
        scheduler.store(),
        telemetry,
        scheduler.instance_id().to_string(),
        assigner,
        is_leader,
        shutdown,
    )
    .await;
}
