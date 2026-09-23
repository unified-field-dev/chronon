//! Scheduler leader election integration (store + leader module).

#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::sync::Arc;

use chronon_backend_mem::InMemorySchedulerStore;
use chronon_core::store::SchedulerStore;
use chronon_scheduler::{am_i_leader, try_acquire_leader, LeaderElector, PartitionAssigner};
use chronon_telemetry::NoOpSink;

#[tokio::test]
async fn leader_module_blocks_second_instance() {
    let store: Arc<dyn SchedulerStore> = Arc::new(InMemorySchedulerStore::new());

    assert!(try_acquire_leader(&store, "coord-a").await.unwrap());
    assert!(!try_acquire_leader(&store, "coord-b").await.unwrap());
    assert!(am_i_leader(&store, "coord-a").await.unwrap());
    assert!(!am_i_leader(&store, "coord-b").await.unwrap());
}

#[tokio::test]
async fn leader_elector_wins_and_owns_all_partitions() {
    let store: Arc<dyn SchedulerStore> = Arc::new(InMemorySchedulerStore::new());
    let assigner = Arc::new(PartitionAssigner::new(
        Arc::clone(&store),
        Arc::new(NoOpSink),
        "coord-a".to_string(),
        4,
    ));
    let elector = LeaderElector::new(
        Arc::clone(&store),
        Arc::new(NoOpSink),
        "coord-a".to_string(),
    );

    elector.step_once(&assigner).await;

    assert!(elector
        .is_leader_flag()
        .load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(assigner.owned_partitions().await.len(), 4);
}

#[tokio::test]
async fn leader_elector_standby_never_owns_partitions() {
    let store: Arc<dyn SchedulerStore> = Arc::new(InMemorySchedulerStore::new());
    assert!(try_acquire_leader(&store, "rival").await.unwrap());

    let assigner = Arc::new(PartitionAssigner::new(
        Arc::clone(&store),
        Arc::new(NoOpSink),
        "self".to_string(),
        4,
    ));
    let elector = LeaderElector::new(Arc::clone(&store), Arc::new(NoOpSink), "self".to_string());

    elector.step_once(&assigner).await;

    assert!(!elector
        .is_leader_flag()
        .load(std::sync::atomic::Ordering::SeqCst));
    assert!(assigner.owned_partitions().await.is_empty());
}

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
async fn leader_elector_demotes_on_lease_theft() {
    // Short TTL so the lease actually lapses in-test instead of relying on a mock clock.
    let _ttl_guard = EnvGuard::set("CHRONON_LEADER_TTL_S", "1");

    let store: Arc<dyn SchedulerStore> = Arc::new(InMemorySchedulerStore::new());
    let assigner = Arc::new(PartitionAssigner::new(
        Arc::clone(&store),
        Arc::new(NoOpSink),
        "coord-a".to_string(),
        4,
    ));
    let elector = LeaderElector::new(
        Arc::clone(&store),
        Arc::new(NoOpSink),
        "coord-a".to_string(),
    );

    elector.step_once(&assigner).await;
    assert!(elector
        .is_leader_flag()
        .load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(assigner.owned_partitions().await.len(), 4);

    // Let the 1s lease lapse, then a rival steals the row (simulates the leader crashing
    // and a standby winning after the TTL window).
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    assert!(try_acquire_leader(&store, "rival").await.unwrap());

    elector.step_once(&assigner).await;

    assert!(!elector
        .is_leader_flag()
        .load(std::sync::atomic::Ordering::SeqCst));
    assert!(assigner.owned_partitions().await.is_empty());
}
