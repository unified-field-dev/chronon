# chronon-bench

Performance CLI and experiment registry for BM-CH* (scheduler layer) and BM-CH7-D hyperscale campaigns. Run benchmark sweeps, matrix slices, scaling curves, and fleet aggregation.

**Docs:** [`PERFORMANCE.md`](PERFORMANCE.md).

## Subcommands

| Command | Purpose |
|---------|---------|
| `experiments` | List BM-CH* / BM-CHL* / BM-CH7D IDs |
| `run` | Single experiment with sweep knobs (W, Q, bc, pools, worker hosts) |
| `matrix` | Run a named slice × storage backend |
| `scaling-curve` | Project JSON reports into sweep curves |
| `aggregate` | Sum multibench BM-CH7 per-client reports into one fleet cell |

**Curve kinds:** `ch7-worker-curve`, `ch7-pool-curve`, `ch7-data-curve`, `ch7-multibench-curve`, `ch7d-fleet-curve`, `ch1-job-curve`, `chl-sustain-curve`

## Embedded burst (bm-ch-embed-burst)

Measures completed runs after a midnight-shaped due-job wave on the embedded scheduler: tick, claim, and worker execution together. BM-CH5 remains the sequential lifecycle-overhead experiment (one job at a time). Store claim rates stay on BM-CH7.

```bash
CHRONON_BENCH_HARDWARE=aws-t3-medium \
CHRONON_WORKER_CONCURRENCY=4 \
cargo run -p chronon-bench -- run \
  --experiment bm-ch-embed-burst \
  --storage sqlite --deployment embedded \
  --telemetry off --jobs 500
```

Worker sweep (4/8/16/32) and sqlite campaign: [`scripts/run-embed-burst.sh`](scripts/run-embed-burst.sh).

## Fleet burst (bm-ch-fleet-burst)

Same 500 due jobs plus colliding recurring cohort, run on a coordinator process plus distinct worker hosts against a shared Postgres+Redis store. The published scale axis is worker host count (1/2/4). On AWS, set `CHRONON_BENCH_COORDINATOR_ONLY=1` on the coordinator and `CHRONON_BENCH_DRAIN_ONLY=1` on each worker with a shared `CHRONON_BENCH_CELL_ID`. Quote a public number only from a postgres-redis AWS report with `status=ok`.

```bash
CHRONON_BENCH_HARDWARE=aws-c6i-large \
cargo run -p chronon-bench -- run \
  --experiment bm-ch-fleet-burst \
  --storage postgres-redis --deployment coordinator-worker \
  --topology split-chronon-server --jobs 500 --worker-hosts 1
```

## Verify

```bash
export CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=../../target-chronon-bench
cargo run -p chronon-bench -- experiments
cargo run -p chronon-bench -- run --experiment bm-ch0 --storage mem --ops 1000 --warmup 5
cargo run -p chronon-bench -- run --experiment bm-ch-embed-burst --storage mem --jobs 8
cargo run -p chronon-bench -- matrix --slice adapter-floor --storage mem
cargo run -p chronon-bench -- scaling-curve ch7-worker-curve --storage mem --reports-dir profiling/chronon-bench/reports
cargo test -p chronon-bench --all-targets
```

## Campaign scripts

Local sweeps: [`scripts/`](scripts/) — `run-embed-burst.sh`, `run-ch7-d0-worker-sweep.sh`, `run-ch7-pool-sweep.sh`, `run-ch7-multibench-sweep.sh`, `run-ch7d-fleet-sweep.sh`, `run-ch7-multibench-smoke.sh`.

AWS hyperscale (CH7-D0–D4): provision, deploy, full campaign, and fetch reports on AWS EC2 (operator campaign).

**Reports:** runs write to `profiling/chronon-bench/reports/` as local scratch (gitignored). Decision-grade AWS results (baseline on `aws-t3.medium`, CH7-D curves on the `aws-c6i-large` label) aren't committed to this repo.
