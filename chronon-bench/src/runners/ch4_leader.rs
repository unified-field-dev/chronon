use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use chrono::Utc;
use chronon_core::{Job, ScheduleKind};
use chronon_runtime::ChrononBuilder;
use chronon_testkit::BootstrapSession;
use tokio::time::{sleep, Duration};

use crate::report::BenchReport;
use crate::runners::RunContext;
use crate::stats::MetricStats;

/// BM-CH4: measure end-to-end leader failover recovery time.
///
/// Boots two real `CoordinatorOnly` [`chronon_runtime::Chronon`] instances against the
/// shared matrix store (the same `ChrononBuilder` + `LeaderElector` wiring production
/// hosts use), kills whichever one wins the lease, and times wall-clock from that kill to
/// the survivor resuming ticking — not just how fast the store-level lease CAS itself is
/// once the dead leader's lease has already been waited out.
///
/// This is bound below by `CHRONON_LEADER_TTL_S` (a standby cannot safely take over before
/// the dead leader's lease lapses) plus roughly one `CHRONON_LEADER_RENEW_S` campaign
/// interval, not by the tick interval — an earlier version of this bench measured only the
/// CAS call after externally waiting out the TTL, which understated real recovery time.
pub async fn run(ctx: &RunContext) -> Result<BenchReport> {
    let ttl_secs = 1_i64;
    let renew_secs = 1_i64;
    std::env::set_var("CHRONON_LEADER_TTL_S", ttl_secs.to_string());
    std::env::set_var("CHRONON_LEADER_RENEW_S", renew_secs.to_string());
    let post_round_wait = Duration::from_millis((ttl_secs * 1000 + 100) as u64);

    let mut session = BootstrapSession::new(ctx.matrix.clone());
    session.install().await?;
    let store = session.store_dyn()?;

    let mut failover_samples = Vec::with_capacity(ctx.plan.default_ops);

    for round in 0..ctx.plan.default_ops {
        let mut coord_a = ChrononBuilder::new()
            .scheduler_store(Arc::clone(&store))
            .instance_id(format!("bm-ch4-a-{round}"))
            .coordinator_only()
            .build()
            .map_err(|e| anyhow::anyhow!("build coord-a: {e}"))?;
        let mut coord_b = ChrononBuilder::new()
            .scheduler_store(Arc::clone(&store))
            .instance_id(format!("bm-ch4-b-{round}"))
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

        // Let leadership settle (one of them wins on the election loop's first tick).
        sleep(Duration::from_millis(150)).await;
        let leader = store
            .get_leader()
            .await?
            .ok_or_else(|| anyhow::anyhow!("bm-ch4 round {round}: no leader elected"))?;
        let (leader_shutdown, survivor_shutdown) =
            if leader.leader_instance_id.starts_with("bm-ch4-a") {
                (&shutdown_a, &shutdown_b)
            } else {
                (&shutdown_b, &shutdown_a)
            };

        // Kill the leader (simulates a crash), then seed a job only after that so the run
        // is unambiguously attributable to the survivor taking over — and start the clock
        // at the same instant so the sample includes real detection + re-election latency.
        leader_shutdown.notify_waiters();
        let mut job = Job::new(format!("bm-ch4-job-{round}"), "bm_ch4_noop");
        job.schedule_kind = ScheduleKind::Cron;
        job.cron_expr = Some("* * * * *".to_string());
        job.next_run_at = Some(Utc::now() - chrono::Duration::seconds(1));
        job.partition_hash = Some(0);
        job.enabled = true;
        let job_id = job.job_id.clone();

        let start = Instant::now();
        store.upsert_job(&job).await?;
        loop {
            if !store.list_runs_for_job(&job_id, 10).await?.is_empty() {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        failover_samples.push(start.elapsed().as_secs_f64() * 1000.0);

        survivor_shutdown.notify_waiters();
        // Let this round's lease lapse before the next round's coordinators campaign.
        sleep(post_round_wait).await;
    }

    let stats = MetricStats::summarize(failover_samples);
    let failover_budget_ms = ((ttl_secs + renew_secs) * 1000) as f64;
    let mut report = BenchReport::base(&ctx.plan.id, &ctx.matrix);
    report.ops = Some(ctx.plan.default_ops);
    report.failover_ms = Some(stats);
    report.pass_notes = Some(format!(
        "failover p95 {:.3} ms (budget {:.0} ms = leader TTL + one campaign interval); \
         measured end-to-end through ChrononBuilder + LeaderElector production wiring, not \
         store primitives directly",
        stats.p95, failover_budget_ms
    ));
    Ok(report)
}
