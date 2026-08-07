# chronon-bench

Performance CLI and experiment registry for BM-CH* (scheduler layer) and BM-CH7-D hyperscale campaigns — run benchmark sweeps, matrix slices, scaling curves, and fleet aggregation.

**Docs:** [`PERFORMANCE.md`](PERFORMANCE.md) · [`PERFORMANCE.md`](PERFORMANCE.md)

## Subcommands

| Command | Purpose |
|---------|---------|
| `experiments` | List BM-CH* / BM-CHL* / BM-CH7D IDs |
| `run` | Single experiment with sweep knobs (W, Q, bc, pools, worker hosts) |
| `matrix` | Run a named slice × storage backend |
| `scaling-curve` | Project JSON reports into sweep curves |
| `aggregate` | Sum multibench BM-CH7 per-client reports into one fleet cell |

**Curve kinds:** `ch7-worker-curve`, `ch7-pool-curve`, `ch7-data-curve`, `ch7-multibench-curve`, `ch7d-fleet-curve`, `ch1-job-curve`, `chl-sustain-curve`

## Verify

```bash
export CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=../../target-chronon-bench
cargo run -p chronon-bench -- experiments
cargo run -p chronon-bench -- run --experiment bm-ch0 --storage mem --ops 1000 --warmup 5
cargo run -p chronon-bench -- matrix --slice adapter-floor --storage mem
cargo run -p chronon-bench -- scaling-curve ch7-worker-curve --storage mem --reports-dir profiling/chronon-bench/reports
cargo test -p chronon-bench --all-targets
```

## Campaign scripts

Local sweeps: [`scripts/`](scripts/) — `run-ch7-d0-worker-sweep.sh`, `run-ch7-pool-sweep.sh`, `run-ch7-multibench-sweep.sh`, `run-ch7d-fleet-sweep.sh`, `run-ch7-multibench-smoke.sh`.

AWS hyperscale (CH7-D0–D4): provision, deploy, full campaign, and fetch reports on AWS EC2 (operator campaign).

**Reports:** `profiling/chronon-bench/reports/` — baseline 85 JSON on `aws-t3.medium`; CH7-D curves on `aws-c6i-large` label.
