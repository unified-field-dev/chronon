//! Macro to forward [`SchedulerStore`](chronon_core::store::SchedulerStore) to an inner adapter.

/// Delegate every [`SchedulerStore`](chronon_core::store::SchedulerStore) method to `$field`.
#[macro_export]
macro_rules! delegate_scheduler_store {
    ($wrapper:ty, $field:ident) => {
        #[::async_trait::async_trait]
        impl ::chronon_core::store::SchedulerStore for $wrapper {
            async fn upsert_job(
                &self,
                job: &::chronon_core::models::Job,
            ) -> ::chronon_core::Result<()> {
                self.$field.upsert_job(job).await
            }

            async fn get_job(
                &self,
                job_id: &str,
            ) -> ::chronon_core::Result<Option<::chronon_core::models::Job>> {
                self.$field.get_job(job_id).await
            }

            async fn get_job_by_name(
                &self,
                job_name: &str,
            ) -> ::chronon_core::Result<Option<::chronon_core::models::Job>> {
                self.$field.get_job_by_name(job_name).await
            }

            async fn list_jobs(&self) -> ::chronon_core::Result<Vec<::chronon_core::models::Job>> {
                self.$field.list_jobs().await
            }

            async fn list_due_jobs(
                &self,
                before: ::chrono::DateTime<::chrono::Utc>,
            ) -> ::chronon_core::Result<Vec<::chronon_core::models::Job>> {
                self.$field.list_due_jobs(before).await
            }

            async fn pause_job(&self, job_id: &str) -> ::chronon_core::Result<()> {
                self.$field.pause_job(job_id).await
            }

            async fn resume_job(&self, job_id: &str) -> ::chronon_core::Result<()> {
                self.$field.resume_job(job_id).await
            }

            async fn delete_job(&self, job_id: &str) -> ::chronon_core::Result<bool> {
                self.$field.delete_job(job_id).await
            }

            async fn create_run(
                &self,
                run: &::chronon_core::models::Run,
            ) -> ::chronon_core::Result<()> {
                self.$field.create_run(run).await
            }

            async fn update_run(
                &self,
                run: &::chronon_core::models::Run,
            ) -> ::chronon_core::Result<()> {
                self.$field.update_run(run).await
            }

            async fn get_run(
                &self,
                run_id: &str,
            ) -> ::chronon_core::Result<Option<::chronon_core::models::Run>> {
                self.$field.get_run(run_id).await
            }

            async fn list_runs_for_job(
                &self,
                job_id: &str,
                limit: usize,
            ) -> ::chronon_core::Result<Vec<::chronon_core::models::Run>> {
                self.$field.list_runs_for_job(job_id, limit).await
            }

            async fn list_runs_filtered(
                &self,
                job_id: Option<&str>,
                status: Option<::chronon_core::models::RunStatus>,
                offset: usize,
                limit: usize,
            ) -> ::chronon_core::Result<Vec<::chronon_core::models::Run>> {
                self.$field
                    .list_runs_filtered(job_id, status, offset, limit)
                    .await
            }

            async fn list_runs_after(
                &self,
                job_id: Option<&str>,
                status: Option<::chronon_core::models::RunStatus>,
                after: Option<&::chronon_core::models::RunKey>,
                limit: usize,
            ) -> ::chronon_core::Result<Vec<::chronon_core::models::Run>> {
                self.$field
                    .list_runs_after(job_id, status, after, limit)
                    .await
            }

            async fn claim_next_queued(
                &self,
                pool_id: &str,
                worker_id: &str,
                now: ::chrono::DateTime<::chrono::Utc>,
                lease_ttl_secs: i64,
            ) -> ::chronon_core::Result<Option<::chronon_core::models::Run>> {
                self.$field
                    .claim_next_queued(pool_id, worker_id, now, lease_ttl_secs)
                    .await
            }

            async fn claim_run_by_id(
                &self,
                run_id: &str,
                pool_id: &str,
                worker_id: &str,
                now: ::chrono::DateTime<::chrono::Utc>,
                lease_ttl_secs: i64,
            ) -> ::chronon_core::Result<Option<::chronon_core::models::Run>> {
                self.$field
                    .claim_run_by_id(run_id, pool_id, worker_id, now, lease_ttl_secs)
                    .await
            }

            async fn claim_runs_by_ids(
                &self,
                run_ids: &[&str],
                pool_id: &str,
                worker_id: &str,
                now: ::chrono::DateTime<::chrono::Utc>,
                lease_ttl_secs: i64,
            ) -> ::chronon_core::Result<Vec<::chronon_core::models::Run>> {
                self.$field
                    .claim_runs_by_ids(run_ids, pool_id, worker_id, now, lease_ttl_secs)
                    .await
            }

            async fn renew_run_lease(
                &self,
                run_id: &str,
                worker_id: &str,
                now: ::chrono::DateTime<::chrono::Utc>,
                lease_ttl_secs: i64,
            ) -> ::chronon_core::Result<bool> {
                self.$field
                    .renew_run_lease(run_id, worker_id, now, lease_ttl_secs)
                    .await
            }

            async fn reclaim_expired_run_leases(
                &self,
                now: ::chrono::DateTime<::chrono::Utc>,
            ) -> ::chronon_core::Result<Vec<String>> {
                self.$field.reclaim_expired_run_leases(now).await
            }

            async fn append_revision(
                &self,
                revision: &::chronon_core::models::JobRevision,
            ) -> ::chronon_core::Result<()> {
                self.$field.append_revision(revision).await
            }

            async fn list_revisions(
                &self,
                job_id: &str,
            ) -> ::chronon_core::Result<Vec<::chronon_core::models::JobRevision>> {
                self.$field.list_revisions(job_id).await
            }

            async fn upsert_script(
                &self,
                script: &::chronon_core::models::Script,
            ) -> ::chronon_core::Result<()> {
                self.$field.upsert_script(script).await
            }

            async fn get_script(
                &self,
                script_name: &str,
            ) -> ::chronon_core::Result<Option<::chronon_core::models::Script>> {
                self.$field.get_script(script_name).await
            }

            async fn try_claim_run_once(
                &self,
                job_id: &str,
                claimed_by: &str,
                now: ::chrono::DateTime<::chrono::Utc>,
                claim_ttl_secs: i64,
            ) -> ::chronon_core::Result<bool> {
                self.$field
                    .try_claim_run_once(job_id, claimed_by, now, claim_ttl_secs)
                    .await
            }

            async fn mark_run_once_completed(
                &self,
                job_id: &str,
                completed_at: ::chrono::DateTime<::chrono::Utc>,
            ) -> ::chronon_core::Result<()> {
                self.$field
                    .mark_run_once_completed(job_id, completed_at)
                    .await
            }

            async fn release_run_once_claim(
                &self,
                job_id: &str,
                claimed_by: &str,
                now: ::chrono::DateTime<::chrono::Utc>,
            ) -> ::chronon_core::Result<()> {
                self.$field
                    .release_run_once_claim(job_id, claimed_by, now)
                    .await
            }

            async fn find_due_job_ids_in_partitions(
                &self,
                owned_partitions: &[u32],
                due_until: ::chrono::DateTime<::chrono::Utc>,
                limit: u32,
            ) -> ::chronon_core::Result<Vec<String>> {
                self.$field
                    .find_due_job_ids_in_partitions(owned_partitions, due_until, limit)
                    .await
            }

            async fn min_next_run_at_in_partitions(
                &self,
                owned_partitions: &[u32],
            ) -> ::chronon_core::Result<Option<::chrono::DateTime<::chrono::Utc>>> {
                self.$field
                    .min_next_run_at_in_partitions(owned_partitions)
                    .await
            }

            async fn claim_job_for_tick(
                &self,
                job_id: &str,
                claim_id: &str,
                now: ::chrono::DateTime<::chrono::Utc>,
                lease_ttl_secs: i64,
            ) -> ::chronon_core::Result<bool> {
                self.$field
                    .claim_job_for_tick(job_id, claim_id, now, lease_ttl_secs)
                    .await
            }

            async fn release_job_tick_claim(&self, job_id: &str) -> ::chronon_core::Result<()> {
                self.$field.release_job_tick_claim(job_id).await
            }

            async fn persist_post_tick_job_state(
                &self,
                job_id: &str,
                next_run_at: Option<::chrono::DateTime<::chrono::Utc>>,
            ) -> ::chronon_core::Result<()> {
                self.$field
                    .persist_post_tick_job_state(job_id, next_run_at)
                    .await
            }

            async fn try_acquire_leader(
                &self,
                instance_id: &str,
                ttl_secs: i64,
            ) -> ::chronon_core::Result<bool> {
                self.$field.try_acquire_leader(instance_id, ttl_secs).await
            }

            async fn renew_leader_lease(
                &self,
                instance_id: &str,
                ttl_secs: i64,
            ) -> ::chronon_core::Result<()> {
                self.$field.renew_leader_lease(instance_id, ttl_secs).await
            }

            async fn get_leader(
                &self,
            ) -> ::chronon_core::Result<Option<::chronon_core::models::SchedulerLeader>> {
                self.$field.get_leader().await
            }

            async fn upsert_partition_assignment(
                &self,
                assignment: &::chronon_core::models::PartitionAssignment,
            ) -> ::chronon_core::Result<()> {
                self.$field.upsert_partition_assignment(assignment).await
            }

            async fn list_partition_assignments(
                &self,
            ) -> ::chronon_core::Result<Vec<::chronon_core::models::PartitionAssignment>> {
                self.$field.list_partition_assignments().await
            }

            async fn register_worker(
                &self,
                worker: &::chronon_core::models::Worker,
            ) -> ::chronon_core::Result<()> {
                self.$field.register_worker(worker).await
            }

            async fn heartbeat_worker(
                &self,
                worker_id: &str,
                at: ::chrono::DateTime<::chrono::Utc>,
            ) -> ::chronon_core::Result<()> {
                self.$field.heartbeat_worker(worker_id, at).await
            }
        }
    };
}
