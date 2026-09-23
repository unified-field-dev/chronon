//! Assembled [`Chronon`] runtime and background loop dispatch.

use std::sync::Arc;

use chronon_core::store::SchedulerStore;
use chronon_executor::Executor;
use chronon_scheduler::Scheduler;
use tokio::sync::Notify;

use crate::builder::DeploymentShape;
use crate::coordinator::run_coordinator_loops;
use crate::coordinator_service::CoordinatorService;
use crate::embedded::run_embedded_loops;
use crate::events::spawn_event_handler;
use crate::worker::run_worker_loop;

/// Assembled Chronon runtime: store, scheduler, executor, and deployment loops.
///
/// Built via [`ChrononBuilder`](crate::ChrononBuilder). Hosts typically:
///
/// 1. Upsert jobs through [`Self::coordinator_service`] (or HTTP / remote client).
/// 2. Call [`Self::run`] to start shape-specific loops (or [`Self::tick_once`] in tests).
/// 3. Call [`Self::shutdown`] on exit.
///
/// | [`DeploymentShape`](crate::DeploymentShape) | [`Self::run`] behavior |
/// |---------------------------------------------|------------------------|
/// | `Embedded` | Tick + worker in this process |
/// | `CoordinatorOnly` | Tick + partitions only |
/// | `Worker(pool)` | Claim + execute for `pool` |
/// | `RemoteClient(_)` | **Error** — use [`crate::RemoteCoordinatorClient`] |
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use chronon_backend_mem::InMemorySchedulerStore;
/// use chronon_runtime::{ChrononBuilder, DeploymentShape};
///
/// let chronon = ChrononBuilder::new()
///     .scheduler_store(Arc::new(InMemorySchedulerStore::new()))
///     .embedded()
///     .build()
///     .unwrap();
/// assert_eq!(chronon.deployment, DeploymentShape::Embedded);
/// let _ = chronon.coordinator_service();
/// ```
pub struct Chronon {
    /// Shared persistence for jobs, runs, partitions, and workers.
    pub store: Arc<dyn SchedulerStore>,
    /// Tick engine and partition assigner handle.
    pub scheduler: Arc<Scheduler>,
    /// Script registry and async dispatch.
    pub executor: Arc<Executor>,
    /// Job/run CRUD API over [`Self::store`].
    pub coordinator: CoordinatorService,
    /// Deployment shape selected at build time.
    pub deployment: DeploymentShape,
    shutdown: Arc<Notify>,
    event_rx: Option<tokio::sync::mpsc::Receiver<chronon_executor::ExecutorEvent>>,
}

impl Chronon {
    pub(crate) fn new(
        store: Arc<dyn SchedulerStore>,
        scheduler: Arc<Scheduler>,
        executor: Arc<Executor>,
        deployment: DeploymentShape,
        shutdown: Arc<Notify>,
        event_rx: tokio::sync::mpsc::Receiver<chronon_executor::ExecutorEvent>,
    ) -> Self {
        let coordinator = CoordinatorService::new(store.clone());
        Self {
            store,
            scheduler,
            executor,
            coordinator,
            deployment,
            shutdown,
            event_rx: Some(event_rx),
        }
    }

    /// Signal all runtime loops to stop.
    pub fn shutdown(&self) {
        self.shutdown.notify_waiters();
    }

    /// Shared shutdown signal for background [`Self::run`] tasks (testkit / host wiring).
    pub fn shutdown_handle(&self) -> Arc<Notify> {
        Arc::clone(&self.shutdown)
    }

    /// Run deployment loops until [`Self::shutdown`] is called.
    ///
    /// Dispatches on [`Self::deployment`]:
    ///
    /// - [`DeploymentShape::Embedded`](crate::DeploymentShape::Embedded) — tick + worker
    /// - [`DeploymentShape::CoordinatorOnly`](crate::DeploymentShape::CoordinatorOnly) — tick only
    /// - [`DeploymentShape::Worker`](crate::DeploymentShape::Worker) — claim + execute
    /// - [`DeploymentShape::RemoteClient`](crate::DeploymentShape::RemoteClient) — returns
    ///   [`ChrononError::Internal`](chronon_core::ChrononError::Internal); use
    ///   [`crate::RemoteCoordinatorClient`] instead
    ///
    /// Call `scheduler.init_partitions().await` before [`Self::run`] on the `Embedded`
    /// shape so partition ownership is ready. `CoordinatorOnly` needs no such call —
    /// leader election assigns partitions to whichever replica wins the lease.
    ///
    /// # Examples
    ///
    /// Embedded: init partitions, then run until shutdown:
    ///
    /// ```no_run
    /// use std::sync::Arc;
    /// use chronon_backend_mem::InMemorySchedulerStore;
    /// use chronon_runtime::ChrononBuilder;
    ///
    /// # async fn demo() -> chronon_core::Result<()> {
    /// let mut chronon = ChrononBuilder::new()
    ///     .scheduler_store(Arc::new(InMemorySchedulerStore::new()))
    ///     .embedded()
    ///     .build()?;
    /// chronon.scheduler.init_partitions().await;
    /// // chronon.run().await?;  // blocks until chronon.shutdown()
    /// # let _ = chronon;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Runnable daemons: `cargo run -p uf-chronon --example coordinator_daemon --features postgres,redis`
    /// and `worker_daemon`.
    pub async fn run(&mut self) -> chronon_core::Result<()> {
        if let Some(rx) = self.event_rx.take() {
            spawn_event_handler(Arc::clone(&self.store), rx);
        }

        let shutdown = Arc::clone(&self.shutdown);
        let scheduler = Arc::clone(&self.scheduler);
        let executor = Arc::clone(&self.executor);
        let telemetry = scheduler.telemetry();

        match &self.deployment {
            DeploymentShape::RemoteClient(_) => {
                return Err(chronon_core::ChrononError::Internal(
                    "remote client does not run local scheduler loop".into(),
                ));
            }
            DeploymentShape::Embedded => {
                run_embedded_loops(scheduler, executor, telemetry, shutdown).await;
            }
            DeploymentShape::CoordinatorOnly => {
                run_coordinator_loops(scheduler, telemetry, shutdown).await;
            }
            DeploymentShape::Worker(pool) => {
                run_worker_loop(
                    self.store.clone(),
                    executor,
                    telemetry,
                    pool.clone(),
                    scheduler.instance_id().to_string(),
                    shutdown,
                )
                .await;
            }
        }
        Ok(())
    }

    /// Advance the scheduler by one tick (tests and manual stepping).
    pub async fn tick_once(&self) -> chronon_core::Result<chronon_scheduler::TickResult> {
        self.scheduler.tick_once().await
    }

    /// Job and run CRUD for HTTP handlers and host integration.
    pub fn coordinator_service(&self) -> &CoordinatorService {
        &self.coordinator
    }

    /// Script executor (registry + dispatch).
    pub fn executor(&self) -> &Executor {
        &self.executor
    }
}
