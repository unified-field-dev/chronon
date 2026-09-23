//! [`ChrononBuilder`] and [`DeploymentShape`] — runtime configuration and assembly.

use std::sync::Arc;

use chronon_core::context::{ContextFactory, NoOpContextFactory};
use chronon_core::error::{ChrononError, Result};
use chronon_core::store::SchedulerStore;
use chronon_executor::{
    event_channel_capacity_from_env, executor_concurrency_from_env, Executor, ScriptRegistry,
};
use chronon_scheduler::{Scheduler, SchedulerConfig};
use chronon_telemetry::{NoOpSink, TelemetrySink};
use tokio::sync::{mpsc, Notify};

/// Named deployment assembly — process topology shape.
///
/// Selected via [`ChrononBuilder`] fluent methods; drives which loops
/// [`crate::Chronon::run`] starts. Maps to the public crate getting-started topologies:
///
/// | Shape | Builder | Local loops | When to use |
/// |-------|---------|-------------|-------------|
/// | [`Self::Embedded`] | [`.embedded()`](ChrononBuilder::embedded) | Tick **and** worker | Embedded — one process |
/// | [`Self::CoordinatorOnly`] | [`.coordinator_only()`](ChrononBuilder::coordinator_only) | Tick only | Coordinator–worker coordinator binary |
/// | [`Self::Worker`] | [`.worker(pool)`](ChrononBuilder::worker) | Claim + execute | Coordinator–worker worker binary(ies) |
/// | [`Self::RemoteClient`] | [`.remote_coordinator(url)`](ChrononBuilder::remote_coordinator) | **None** | Remote HTTP client — schedule via HTTP |
///
/// Coordinator–worker and remote HTTP client need a shared durable store (Postgres, usually +
/// Redis) on the host that runs Chronon loops. See [`crate::RemoteCoordinatorClient`] for the
/// HTTP client path.
///
/// # Examples
///
/// ```
/// use chronon_runtime::DeploymentShape;
///
/// assert_eq!(DeploymentShape::default(), DeploymentShape::Embedded);
/// let worker = DeploymentShape::Worker("general".into());
/// assert!(matches!(worker, DeploymentShape::Worker(_)));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum DeploymentShape {
    /// Coordinator tick loop + worker in one process (embedded default).
    #[default]
    Embedded,
    /// Scheduler tick and leader election only; no script execution (coordinator binary).
    /// Only the elected leader among live `CoordinatorOnly` replicas ticks — others stand
    /// by and take over on failover. See [`chronon_scheduler::LeaderElector`].
    CoordinatorOnly,
    /// Worker loop for `pool_id`; claims runs from the shared store (worker binary).
    Worker(String),
    /// No local loops; host uses [`crate::RemoteCoordinatorClient`] against `base_url`.
    RemoteClient(String),
}

/// Builds a [`crate::Chronon`] runtime with explicit adapter injection.
///
/// Hosts call fluent setters then [`Self::build`]. Missing store is the only hard requirement;
/// context factory, telemetry, and registry fall back to no-op defaults.
///
/// Choose topology with [`.embedded()`](Self::embedded) / [`.coordinator_only()`](Self::coordinator_only) /
/// [`.worker()`](Self::worker) / [`.remote_coordinator()`](Self::remote_coordinator) — see
/// [`DeploymentShape`]. Use [`.auto_registry()`](Self::auto_registry) to pick up `#[chronon::script]`
/// handlers linked into this binary (required on coordinator–worker **workers**).
///
/// # Examples
///
/// Embedded with an empty registry:
///
/// ```
/// use std::sync::Arc;
/// use chronon_backend_mem::InMemorySchedulerStore;
/// use chronon_runtime::{ChrononBuilder, DeploymentShape};
///
/// let store = Arc::new(InMemorySchedulerStore::new());
/// let chronon = ChrononBuilder::new()
///     .scheduler_store(store)
///     .embedded()
///     .build()
///     .unwrap();
/// assert_eq!(chronon.deployment, DeploymentShape::Embedded);
/// assert_eq!(chronon.executor().script_count(), 0);
/// ```
///
/// Worker shape (scripts must be registered on this binary):
///
/// ```
/// use std::sync::Arc;
/// use chronon_backend_mem::InMemorySchedulerStore;
/// use chronon_runtime::{ChrononBuilder, DeploymentShape};
///
/// let chronon = ChrononBuilder::new()
///     .scheduler_store(Arc::new(InMemorySchedulerStore::new()))
///     .instance_id("worker-a")
///     .worker("general")
///     .build()
///     .unwrap();
/// assert!(matches!(chronon.deployment, DeploymentShape::Worker(_)));
/// ```
pub struct ChrononBuilder {
    store: Option<Arc<dyn SchedulerStore>>,
    context_factory: Option<Arc<dyn ContextFactory>>,
    telemetry: Option<Arc<dyn TelemetrySink>>,
    registry: Option<Arc<ScriptRegistry>>,
    deployment: DeploymentShape,
    auto_registry: bool,
    tick_interval_ms: u64,
    instance_id: Option<String>,
}

impl ChrononBuilder {
    /// Empty builder: embedded deployment, env-default tick interval, no store.
    pub fn new() -> Self {
        Self {
            store: None,
            context_factory: None,
            telemetry: None,
            registry: None,
            deployment: DeploymentShape::Embedded,
            auto_registry: false,
            tick_interval_ms: chronon_scheduler::tick_interval_ms_from_env(),
            instance_id: None,
        }
    }

    /// Required unless [`Self::scheduler_store_from_global`] is used.
    pub fn scheduler_store(mut self, store: Arc<dyn SchedulerStore>) -> Self {
        self.store = Some(store);
        self
    }

    /// Installs the process-global default store (e.g. mem backend); errors if unset.
    pub fn scheduler_store_from_global(mut self) -> Result<Self> {
        self.store = Some(chronon_core::default_store_from_global()?);
        Ok(self)
    }

    /// Factory used when executing scripts; defaults to [`NoOpContextFactory`].
    pub fn context_factory(mut self, factory: Arc<dyn ContextFactory>) -> Self {
        self.context_factory = Some(factory);
        self
    }

    /// Metrics sink shared by scheduler and executor; defaults to [`NoOpSink`].
    pub fn telemetry_sink(mut self, sink: Arc<dyn TelemetrySink>) -> Self {
        self.telemetry = Some(sink);
        self
    }

    /// Script registry for the executor; use [`Self::auto_registry`] to populate from inventory.
    pub fn script_registry(mut self, registry: Arc<ScriptRegistry>) -> Self {
        self.registry = Some(registry);
        self
    }

    /// Stable id for scheduler leader election and worker rows; random UUID if omitted.
    pub fn instance_id(mut self, id: impl Into<String>) -> Self {
        self.instance_id = Some(id.into());
        self
    }

    /// Embedded coordinator + worker loops in one process.
    pub fn embedded(mut self) -> Self {
        self.deployment = DeploymentShape::Embedded;
        self
    }

    /// Coordinator-only: tick and partition assigner, no worker slots.
    pub fn coordinator_only(mut self) -> Self {
        self.deployment = DeploymentShape::CoordinatorOnly;
        self
    }

    /// Worker-only: claim and execute runs for `pool_id`.
    pub fn worker(mut self, pool_id: impl Into<String>) -> Self {
        self.deployment = DeploymentShape::Worker(pool_id.into());
        self
    }

    /// Remote client shape: no local loops; pair with [`crate::RemoteCoordinatorClient`].
    ///
    /// [`crate::Chronon::run`] returns an error for this shape — schedule via the HTTP client.
    pub fn remote_coordinator(mut self, base_url: impl Into<String>) -> Self {
        self.deployment = DeploymentShape::RemoteClient(base_url.into());
        self
    }

    /// Populate registry from `inventory` (`#[chronon::script]` link-time registration).
    ///
    /// In a coordinator–worker split, call this on **worker** binaries (that is where scripts
    /// execute).
    pub fn auto_registry(mut self) -> Self {
        self.auto_registry = true;
        self
    }

    /// Scheduler tick period in milliseconds; overrides `CHRONON_TICK_INTERVAL_MS` when set.
    pub fn tick_interval_ms(mut self, ms: u64) -> Self {
        self.tick_interval_ms = ms;
        self
    }

    /// Assemble [`crate::Chronon`]; returns [`ChrononError::Internal`] if store was not configured.
    pub fn build(self) -> Result<super::Chronon> {
        let store = self
            .store
            .ok_or_else(|| ChrononError::Internal("scheduler_store is required".into()))?;
        let context_factory = self
            .context_factory
            .unwrap_or_else(|| Arc::new(NoOpContextFactory));
        let telemetry = self
            .telemetry
            .unwrap_or_else(|| Arc::new(NoOpSink) as Arc<dyn TelemetrySink>);
        let registry = match self.registry {
            Some(registry) => registry,
            None if self.auto_registry => Arc::new(ScriptRegistry::from_inventory()),
            None => Arc::new(ScriptRegistry::new()),
        };

        let embedded_partitions = matches!(self.deployment, DeploymentShape::Embedded);
        let instance_id = self
            .instance_id
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        let scheduler = Arc::new(Scheduler::new(
            SchedulerConfig {
                tick_interval_ms: self.tick_interval_ms,
                instance_id,
                num_partitions: chronon_scheduler::num_partitions_from_env(),
                embedded: embedded_partitions,
            },
            store.clone(),
            telemetry.clone(),
        ));

        let (event_tx, event_rx) = mpsc::channel(event_channel_capacity_from_env());
        let executor = Arc::new(Executor::new(
            registry,
            context_factory,
            telemetry,
            event_tx,
            executor_concurrency_from_env(),
        ));

        Ok(super::Chronon::new(
            store,
            scheduler,
            executor,
            self.deployment,
            Arc::new(Notify::new()),
            event_rx,
        ))
    }
}

impl Default for ChrononBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Shorthand for [`ChrononBuilder::new`].
pub fn builder() -> ChrononBuilder {
    ChrononBuilder::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chronon_backend_mem::InMemorySchedulerStore;
    use chronon_telemetry::ConsoleSink;

    #[tokio::test]
    async fn builder_embedded_compiles() {
        let store = Arc::new(InMemorySchedulerStore::new());
        let chronon = ChrononBuilder::new()
            .scheduler_store(store)
            .telemetry_sink(Arc::new(ConsoleSink))
            .embedded()
            .build()
            .expect("build");
        assert_eq!(chronon.deployment, DeploymentShape::Embedded);
    }

    #[tokio::test]
    async fn builder_auto_registry_from_inventory() {
        let store = Arc::new(InMemorySchedulerStore::new());
        let chronon = ChrononBuilder::new()
            .scheduler_store(store)
            .embedded()
            .auto_registry()
            .build()
            .expect("build");
        let _ = chronon.executor().script_count();
    }

    #[tokio::test]
    async fn builder_scheduler_store_from_global() {
        let _installed = chronon_backend_mem::install_default_mem_store();
        let chronon = ChrononBuilder::new()
            .scheduler_store_from_global()
            .expect("global store")
            .embedded()
            .build()
            .expect("build");
        assert_eq!(chronon.deployment, DeploymentShape::Embedded);
    }
}
