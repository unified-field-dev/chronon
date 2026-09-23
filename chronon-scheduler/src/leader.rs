//! Scheduler leader election via [`SchedulerStore`].

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use chrono::Utc;
use chronon_core::store::SchedulerStore;
use chronon_core::Result;
use chronon_telemetry::TelemetrySink;
use tokio::sync::Notify;

use crate::partition_assigner::PartitionAssigner;

const DEFAULT_LEADER_TTL_SECS: i64 = 30;
const DEFAULT_LEADER_RENEW_SECS: u64 = 5;

/// Reads `CHRONON_LEADER_TTL_S` (default 30 seconds).
///
/// Lease duration passed to [`try_acquire_leader`] and [`renew_leader_lease`].
pub fn leader_ttl_secs_from_env() -> i64 {
    std::env::var("CHRONON_LEADER_TTL_S")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n: &i64| n >= 1)
        .unwrap_or(DEFAULT_LEADER_TTL_SECS)
}

/// Reads `CHRONON_LEADER_RENEW_S` (default 5 seconds).
///
/// Interval [`LeaderElector::run_election_loop`] uses for both standby campaign
/// attempts and leader lease renewals.
pub fn leader_renew_interval_secs_from_env() -> u64 {
    std::env::var("CHRONON_LEADER_RENEW_S")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n: &u64| n >= 1)
        .unwrap_or(DEFAULT_LEADER_RENEW_SECS)
}

/// Attempts to become the cluster leader, returning `true` on success.
///
/// Called at coordinator boot before partition assignment in distributed mode.
pub async fn try_acquire_leader(
    store: &Arc<dyn SchedulerStore>,
    instance_id: &str,
) -> Result<bool> {
    store
        .try_acquire_leader(instance_id, leader_ttl_secs_from_env())
        .await
}

/// Renews the leader lease for `instance_id` if it currently holds leadership.
///
/// Fails when another instance has taken over or the lease expired.
pub async fn renew_leader_lease(store: &Arc<dyn SchedulerStore>, instance_id: &str) -> Result<()> {
    store
        .renew_leader_lease(instance_id, leader_ttl_secs_from_env())
        .await
}

/// Returns the current leader instance id and lease expiry, if any row exists.
pub async fn current_leader(
    store: &Arc<dyn SchedulerStore>,
) -> Result<Option<(String, chrono::DateTime<Utc>)>> {
    Ok(store
        .get_leader()
        .await?
        .map(|l| (l.leader_instance_id, l.leader_lease_until)))
}

/// Returns `true` when `instance_id` holds a non-expired leader lease.
pub async fn am_i_leader(store: &Arc<dyn SchedulerStore>, instance_id: &str) -> Result<bool> {
    let Some((id, until)) = current_leader(store).await? else {
        return Ok(false);
    };
    Ok(until > Utc::now() && id == instance_id)
}

/// Background leader-election loop for split (`CoordinatorOnly`) deployments.
///
/// Races for the singleton leader lease and drives [`PartitionAssigner`] ownership to
/// match: the winner owns every partition (mirrors [`PartitionAssigner::assign_all_embedded`],
/// no lease churn), and a standby owns none. Only the elected leader's coordinator tick
/// loop should run — see [`is_leader_flag`](Self::is_leader_flag).
pub struct LeaderElector {
    store: Arc<dyn SchedulerStore>,
    telemetry: Arc<dyn TelemetrySink>,
    instance_id: String,
    is_leader: Arc<AtomicBool>,
}

impl LeaderElector {
    /// Builds an elector for `instance_id`, initially in standby.
    pub fn new(
        store: Arc<dyn SchedulerStore>,
        telemetry: Arc<dyn TelemetrySink>,
        instance_id: String,
    ) -> Self {
        Self {
            store,
            telemetry,
            instance_id,
            is_leader: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Shared flag the coordinator tick loop reads; `true` only while this instance
    /// holds a live leader lease.
    pub fn is_leader_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.is_leader)
    }

    /// One election attempt: try to acquire (if standby) or renew (if leader), then
    /// reconcile `assigner`'s owned-partition set and the flag with ground truth.
    ///
    /// Side-effect only, no sleep, so tests can call it deterministically. [`renew_leader_lease`]
    /// returns `Ok(())` even when it silently no-ops because a rival already holds the
    /// row, so a renew is always re-confirmed with [`am_i_leader`] before trusting it.
    pub async fn step_once(&self, assigner: &Arc<PartitionAssigner>) {
        let was_leader = self.is_leader.load(Ordering::SeqCst);
        if was_leader {
            if let Err(e) = renew_leader_lease(&self.store, &self.instance_id).await {
                self.telemetry.log_event(
                    "chronon_scheduler_warn",
                    &[("component", "leader_elector"), ("message", &e.to_string())],
                );
            }
            let still_leader = am_i_leader(&self.store, &self.instance_id)
                .await
                .unwrap_or(false);
            if !still_leader {
                assigner.release_all().await;
                self.is_leader.store(false, Ordering::SeqCst);
                self.telemetry.log_event(
                    "chronon_scheduler_leader",
                    &[
                        ("instance_id", self.instance_id.as_str()),
                        ("event", "lost"),
                    ],
                );
            }
        } else {
            match try_acquire_leader(&self.store, &self.instance_id).await {
                Ok(true) => {
                    assigner.assign_all_embedded().await;
                    self.is_leader.store(true, Ordering::SeqCst);
                    self.telemetry.log_event(
                        "chronon_scheduler_leader",
                        &[
                            ("instance_id", self.instance_id.as_str()),
                            ("event", "acquired"),
                        ],
                    );
                }
                Ok(false) => {}
                Err(e) => self.telemetry.log_event(
                    "chronon_scheduler_warn",
                    &[("component", "leader_elector"), ("message", &e.to_string())],
                ),
            }
        }
    }

    /// Runs [`Self::step_once`] on a `leader_renew_interval_secs_from_env`-driven interval
    /// until `shutdown` fires; on shutdown, relinquishes owned partitions if currently leader.
    pub async fn run_election_loop(
        self: Arc<Self>,
        assigner: Arc<PartitionAssigner>,
        shutdown: Arc<Notify>,
    ) {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(
            leader_renew_interval_secs_from_env(),
        ));
        loop {
            tokio::select! {
                () = shutdown.notified() => break,
                _ = interval.tick() => {
                    self.step_once(&assigner).await;
                }
            }
        }
        if self.is_leader.load(Ordering::SeqCst) {
            assigner.release_all().await;
        }
    }
}
