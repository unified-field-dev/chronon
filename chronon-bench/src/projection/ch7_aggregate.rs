//! Aggregate per-client BM-CH7 multibench reports into fleet totals.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{bail, Result};
use serde_json;

use crate::report::BenchReport;
use crate::stats::MetricStats;

/// Wall-clock fleet claim rate from per-client ops and drain durations.
#[must_use]
pub fn fleet_wall_claim_rate(reports: &[&BenchReport]) -> Option<f64> {
    let mut total_ops = 0_usize;
    let mut max_drain = 0.0_f64;
    for rep in reports {
        total_ops += rep.ops.unwrap_or(0);
        max_drain = max_drain.max(rep.drain_elapsed_secs.unwrap_or(0.0));
    }
    if total_ops == 0 || max_drain <= f64::EPSILON {
        return None;
    }
    Some(total_ops as f64 / max_drain)
}

/// Sum per-client claim rates for one multibench cell and write aggregate JSON.
pub fn ch7_aggregate(
    storage: &str,
    hardware: &str,
    reports_dir: &Path,
    cell_prefix: &str,
) -> Result<()> {
    let mut by_bc: HashMap<u32, HashMap<u32, (BenchReport, String)>> = HashMap::new();

    for entry in std::fs::read_dir(reports_dir)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let fname = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default()
            .to_string();
        if !fname.starts_with(cell_prefix) || fname.contains("-aggregate-") {
            continue;
        }
        let text = std::fs::read_to_string(&path)?;
        let report: BenchReport = serde_json::from_str(&text)?;
        if report.storage != storage || report.hardware != hardware {
            continue;
        }
        if report.aggregate.unwrap_or(false) {
            continue;
        }
        let dims = report.sweep_dimensions.as_ref();
        let bc = dims.and_then(|d| d.bench_client_count).unwrap_or(1);
        if bc <= 1 {
            continue;
        }
        let idx = dims.and_then(|d| d.bench_client_index).unwrap_or(0);
        by_bc.entry(bc).or_default().insert(idx, (report, fname));
    }

    if by_bc.is_empty() {
        bail!("no multibench reports matching prefix {cell_prefix}");
    }

    for (bc, clients) in &by_bc {
        if clients.len() != *bc as usize {
            bail!(
                "bc={bc}: expected {bc} client reports, found {}",
                clients.len()
            );
        }
        let mut total_rate = 0.0_f64;
        let mut template = clients.values().next().expect("non-empty").0.clone();
        let client_refs: Vec<&BenchReport> = clients.values().map(|(r, _)| r).collect();
        for rep in &client_refs {
            total_rate += rep.claim_ops_per_sec.as_ref().map_or(0.0, |s| s.max);
        }
        let wall_rate = fleet_wall_claim_rate(&client_refs);
        template.aggregate = Some(true);
        template.fleet_claim_ops_per_sec = Some(total_rate);
        template.fleet_wall_claim_ops_per_sec = wall_rate;
        template.claim_ops_per_sec = Some(MetricStats {
            count: *bc as usize,
            p50: total_rate,
            p95: total_rate,
            p99: total_rate,
            min: total_rate,
            max: total_rate,
        });
        let wall_note = wall_rate.map_or_else(String::new, |w| format!(", wall-clock {w:.1}/s"));
        template.pass_notes = Some(format!(
            "aggregate {total_rate:.1} claims/s (sum of client rates){wall_note} across bc={bc} clients (prefix {cell_prefix})"
        ));
        let out_name = format!("{cell_prefix}-aggregate-bc{bc}-{storage}-{hardware}.json");
        let out_path = reports_dir.join(out_name);
        std::fs::write(&out_path, serde_json::to_string_pretty(&template)?)?;
        println!("wrote {}", out_path.display());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::fleet_wall_claim_rate;
    use crate::report::BenchReport;
    use chronon_testkit::MatrixSpec;

    #[test]
    fn wall_rate_uses_max_drain_and_sum_ops() {
        let mut a = BenchReport::base("bm-ch7", &MatrixSpec::default());
        a.ops = Some(85_153);
        a.drain_elapsed_secs = Some(76.5);
        let mut b = BenchReport::base("bm-ch7", &MatrixSpec::default());
        b.ops = Some(14_847);
        b.drain_elapsed_secs = Some(15.0);
        let rate = fleet_wall_claim_rate(&[&a, &b]).expect("rate");
        assert!((rate - 100_000.0 / 76.5).abs() < 1.0);
    }
}
