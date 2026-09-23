//! Per-partition lease renewals for coordinator sharding.

use std::collections::HashSet;
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use chronon_core::models::PartitionAssignment;
use chronon_core::store::SchedulerStore;
use chronon_core::Result;
use chronon_telemetry::TelemetrySink;
use tokio::sync::{Notify, RwLock};

use crate::partitioning;

/// Owns a cached view of partition ids leased to this coordinator instance.
///
/// Distributed coordinators call [`Self::refresh_leases`] or [`Self::run_lease_loop`];
/// embedded hosts use [`Self::assign_all_embedded`] instead.
pub struct PartitionAssigner {
    store: Arc<dyn SchedulerStore>,
    telemetry: Arc<dyn TelemetrySink>,
    instance_id: String,
    num_partitions: u32,
    owned: Arc<RwLock<Vec<u32>>>,
}

impl PartitionAssigner {
    /// Creates an assigner for `instance_id` over `num_partitions` (minimum 1).
    pub fn new(
        store: Arc<dyn SchedulerStore>,
        telemetry: Arc<dyn TelemetrySink>,
        instance_id: String,
        num_partitions: u32,
    ) -> Self {
        Self {
            store,
            telemetry,
            instance_id,
            num_partitions: num_partitions.max(1),
            owned: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Returns a snapshot of partition ids currently leased to this instance.
    pub async fn owned_partitions(&self) -> Vec<u32> {
        self.owned.read().await.clone()
    }

    async fn ensure_row(&self, pid: u32) -> Result<()> {
        let key = pid.to_string();
        let rows = self.store.list_partition_assignments().await?;
        if rows.iter().any(|r| r.partition_id == key) {
            return Ok(());
        }
        let now = Utc::now();
        self.store
            .upsert_partition_assignment(&PartitionAssignment {
                partition_id: key,
                owner_instance_id: String::new(),
                lease_until: DateTime::<Utc>::UNIX_EPOCH,
                updated_at: now,
            })
            .await
    }

    async fn try_steal_partition(&self, pid: u32) -> Result<bool> {
        let key = pid.to_string();
        let now = Utc::now();
        let ttl = Duration::seconds(partitioning::partition_lease_ttl_secs());
        let until = now + ttl;
        let rows = self.store.list_partition_assignments().await?;
        let Some(row) = rows.into_iter().find(|r| r.partition_id == key) else {
            return Ok(false);
        };
        if row.lease_until > now && row.owner_instance_id != self.instance_id {
            return Ok(false);
        }
        self.store
            .upsert_partition_assignment(&PartitionAssignment {
                partition_id: key,
                owner_instance_id: self.instance_id.clone(),
                lease_until: until,
                updated_at: now,
            })
            .await?;
        Ok(true)
    }

    async fn renew_owned(&self, pid: u32) -> Result<()> {
        let key = pid.to_string();
        let rows = self.store.list_partition_assignments().await?;
        let Some(row) = rows.into_iter().find(|r| r.partition_id == key) else {
            return Ok(());
        };
        if row.owner_instance_id != self.instance_id {
            return Ok(());
        }
        let now = Utc::now();
        let until = now + Duration::seconds(partitioning::partition_lease_ttl_secs());
        self.store
            .upsert_partition_assignment(&PartitionAssignment {
                partition_id: key,
                owner_instance_id: self.instance_id.clone(),
                lease_until: until,
                updated_at: now,
            })
            .await
    }

    /// Rebalances partition leases and updates the in-memory owned set.
    ///
    /// Steals expired or unowned partitions until this instance reaches its fair share
    /// of live coordinators.
    pub async fn refresh_leases(&self) -> Result<()> {
        for pid in 0..self.num_partitions {
            self.ensure_row(pid).await?;
        }

        let rows = self.store.list_partition_assignments().await?;
        let mut alive_owners = HashSet::new();
        for row in &rows {
            if row.lease_until > Utc::now() && !row.owner_instance_id.is_empty() {
                alive_owners.insert(row.owner_instance_id.clone());
            }
        }
        let live = alive_owners.len().max(1);
        let target = (self.num_partitions as usize).div_ceil(live);

        let mut mine: Vec<u32> = rows
            .iter()
            .filter(|r| {
                r.owner_instance_id == self.instance_id
                    && r.lease_until > Utc::now()
                    && r.partition_id.parse::<u32>().is_ok()
            })
            .filter_map(|r| r.partition_id.parse().ok())
            .collect();

        for &pid in &mine {
            let _ = self.renew_owned(pid).await;
        }

        if mine.len() < target {
            for pid in 0..self.num_partitions {
                if mine.len() >= target {
                    break;
                }
                if mine.contains(&pid) {
                    continue;
                }
                if self.try_steal_partition(pid).await? {
                    mine.push(pid);
                    mine.sort_unstable();
                }
            }
        }

        mine.sort_unstable();
        *self.owned.write().await = mine;
        Ok(())
    }

    /// Background task that renews partition leases until `shutdown` is notified.
    ///
    /// Interval follows `CHRONON_PARTITION_LEASE_RENEW_S` (default 5 seconds); logs warnings on
    /// refresh failure.
    pub async fn run_lease_loop(self: Arc<Self>, shutdown: Arc<Notify>) {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(
            partitioning::partition_lease_renew_interval_secs(),
        ));
        loop {
            tokio::select! {
                () = shutdown.notified() => break,
                _ = interval.tick() => {
                    if let Err(e) = self.refresh_leases().await {
                        self.telemetry.log_event(
                            "chronon_scheduler_warn",
                            &[("component", "partition_assigner"), ("message", &e.to_string())],
                        );
                    } else {
                        for partition in self.owned_partitions().await {
                            self.telemetry.record_counter(
                                "chronon_partition_assignments",
                                &[("partition", &partition.to_string())],
                                1,
                            );
                        }
                    }
                }
            }
        }
    }

    /// Embedded mode: assign all partitions to this instance without lease churn.
    pub async fn assign_all_embedded(&self) {
        let all: Vec<u32> = (0..self.num_partitions).collect();
        *self.owned.write().await = all;
    }

    /// Demotion: drop local partition ownership without touching stored leases.
    ///
    /// Called by [`crate::leader::LeaderElector`] when this instance loses the leader lease.
    pub async fn release_all(&self) {
        self.owned.write().await.clear();
    }
}
