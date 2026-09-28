//! Per-run log capture under tokio's multi-thread runtime.
//!
//! The process installs a global `Registry` subscriber, the way a host does. Scripts
//! yield repeatedly while other tasks open, enter, and close spans under that global
//! subscriber on the same worker threads. The capture must stay scoped to the script
//! future: no span-ref panics, no foreign events in the run logs, and no script events
//! leaking to the global subscriber.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Once};
use std::time::Duration;

use chronon_core::{ChrononError, ContextFactory, NoOpContextFactory, Result, ScriptContext};
use chronon_executor::{
    execute_script, ExecuteScriptOutcome, ExecuteScriptRequest, ScriptDescriptor, ScriptRegistry,
};
use chronon_telemetry::{NoOpSink, TelemetrySink};
use serde_json::Value;
use tokio::task::JoinHandle;
use tracing::field::{Field, Visit};
use tracing::{Event, Instrument, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::Registry;

const SCRIPT_TICKS: usize = 200;
const FOREIGN_TASKS: usize = 8;
const PROBES: usize = 64;

static GLOBAL_EVENTS: Mutex<Vec<String>> = Mutex::new(Vec::new());
static INSTALL: Once = Once::new();

struct GlobalRecorder;

impl<S> Layer<S> for GlobalRecorder
where
    S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);
        if let Some(message) = visitor.0 {
            GLOBAL_EVENTS.lock().unwrap().push(message);
        }
    }
}

#[derive(Default)]
struct MessageVisitor(Option<String>);

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = Some(format!("{value:?}"));
        }
    }
}

fn install_global_subscriber() {
    INSTALL.call_once(|| {
        tracing::subscriber::set_global_default(Registry::default().with(GlobalRecorder))
            .expect("global subscriber installs once per test binary");
    });
}

fn global_events_containing(needle: &str) -> usize {
    GLOBAL_EVENTS
        .lock()
        .unwrap()
        .iter()
        .filter(|m| m.contains(needle))
        .count()
}

/// Simulates host request handlers: spans created under the global subscriber, entered
/// and exited on whichever worker polls them, with yields in between.
fn spawn_foreign_load(marker: &'static str, stop: &Arc<AtomicBool>) -> Vec<JoinHandle<()>> {
    (0..FOREIGN_TASKS)
        .map(|task| {
            let stop = Arc::clone(stop);
            let task_span = tracing::info_span!("foreign_task", marker, task);
            tokio::spawn(
                async move {
                    while !stop.load(Ordering::Relaxed) {
                        let request = tracing::info_span!("foreign_request", marker);
                        async {
                            tracing::info!("{marker} request start");
                            tokio::task::yield_now().await;
                            tracing::info!("{marker} request end");
                        }
                        .instrument(request)
                        .await;
                        tokio::task::yield_now().await;
                    }
                }
                .instrument(task_span),
            )
        })
        .collect()
}

async fn stop_foreign_load(stop: &AtomicBool, handles: Vec<JoinHandle<()>>) {
    stop.store(true, Ordering::Relaxed);
    for handle in handles {
        if let Err(e) = handle.await {
            assert!(!e.is_panic(), "foreign task panicked: {e}");
        }
    }
}

fn spawn_script(
    name: &'static str,
    invoke: chronon_executor::InvokeFn,
) -> JoinHandle<ExecuteScriptOutcome> {
    tokio::spawn(async move {
        let mut registry = ScriptRegistry::new();
        registry.register(&ScriptDescriptor::new(name, invoke));
        let factory: Arc<dyn ContextFactory> = Arc::new(NoOpContextFactory);
        let telemetry: Arc<dyn TelemetrySink> = Arc::new(NoOpSink);
        execute_script(ExecuteScriptRequest {
            registry: &registry,
            context_factory: &factory,
            telemetry: &telemetry,
            script_name: name,
            actor_json: &Value::Null,
            params_json: Value::Object(serde_json::Map::default()),
            job_name: name,
            run_id: "run-mt",
        })
        .await
    })
}

async fn run_script_on_worker(
    name: &'static str,
    invoke: chronon_executor::InvokeFn,
) -> ExecuteScriptOutcome {
    spawn_script(name, invoke)
        .await
        .expect("script task must not panic")
}

async fn yield_or_sleep(i: usize) {
    if i.is_multiple_of(10) {
        tokio::time::sleep(Duration::from_millis(1)).await;
    } else {
        tokio::task::yield_now().await;
    }
}

fn yielding_script(
    _ctx: Box<dyn ScriptContext>,
    _params: Value,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
    Box::pin(async {
        for i in 0..SCRIPT_TICKS {
            async {
                tracing::info!("yielding-script tick {i}");
                yield_or_sleep(i).await;
            }
            .instrument(tracing::info_span!("script_step", i))
            .await;
        }
        Ok(())
    })
}

fn yielding_then_failing_script(
    _ctx: Box<dyn ScriptContext>,
    _params: Value,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
    Box::pin(async {
        for i in 0..SCRIPT_TICKS {
            async {
                tracing::warn!("failing-script tick {i}");
                yield_or_sleep(i).await;
            }
            .instrument(tracing::info_span!("script_step", i))
            .await;
        }
        Err(ChrononError::Internal("failing-script gave up".into()))
    })
}

fn never_finishing_script(
    _ctx: Box<dyn ScriptContext>,
    _params: Value,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
    Box::pin(async {
        let mut i = 0usize;
        loop {
            async {
                tracing::info!("cancelled-script tick {i}");
                yield_or_sleep(i).await;
            }
            .instrument(tracing::info_span!("script_step", i))
            .await;
            i += 1;
        }
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn capture_stays_scoped_to_script_across_awaits_and_thread_hops() {
    install_global_subscriber();
    let stop = Arc::new(AtomicBool::new(false));
    let foreign = spawn_foreign_load("happy-foreign", &stop);

    let outcome = run_script_on_worker("yielding_script", yielding_script).await;

    stop_foreign_load(&stop, foreign).await;
    assert!(outcome.result.is_ok(), "result={:?}", outcome.result);

    let stdout = outcome.logs.stdout_text.unwrap_or_default();
    let ticks = stdout
        .lines()
        .filter(|l| l.starts_with("yielding-script tick "))
        .count();
    assert_eq!(ticks, SCRIPT_TICKS, "every script event is captured");
    assert!(
        !stdout.contains("happy-foreign"),
        "foreign events leaked into the run capture"
    );
    assert!(outcome.logs.stderr_text.is_none());

    assert!(
        global_events_containing("happy-foreign") > 0,
        "foreign load ran against the global subscriber"
    );
    assert_eq!(
        global_events_containing("yielding-script tick"),
        0,
        "script events leaked to the global subscriber"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failing_script_keeps_its_own_logs_under_concurrent_load() {
    install_global_subscriber();
    let stop = Arc::new(AtomicBool::new(false));
    let foreign = spawn_foreign_load("sad-foreign", &stop);

    let outcome = run_script_on_worker("failing_script", yielding_then_failing_script).await;

    stop_foreign_load(&stop, foreign).await;
    assert!(
        matches!(&outcome.result, Err(ChrononError::Internal(m)) if m.contains("gave up")),
        "result={:?}",
        outcome.result
    );

    let stderr = outcome.logs.stderr_text.unwrap_or_default();
    let ticks = stderr
        .lines()
        .filter(|l| l.starts_with("failing-script tick "))
        .count();
    assert_eq!(ticks, SCRIPT_TICKS, "every warn event is captured");
    assert!(stderr.contains("failing-script gave up"));
    assert!(
        !stderr.contains("sad-foreign"),
        "foreign events leaked into the run capture"
    );
    assert!(outcome.logs.stdout_text.is_none());
    assert_eq!(global_events_containing("failing-script tick"), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_script_does_not_strand_its_subscriber_on_a_worker() {
    install_global_subscriber();
    let stop = Arc::new(AtomicBool::new(false));
    let foreign = spawn_foreign_load("cancel-foreign", &stop);

    let script = spawn_script("never_finishing_script", never_finishing_script);
    tokio::time::sleep(Duration::from_millis(50)).await;
    script.abort();
    assert!(script.await.expect_err("script was aborted").is_cancelled());

    stop_foreign_load(&stop, foreign).await;
    assert_eq!(global_events_containing("cancelled-script tick"), 0);

    // Every worker must be back on the global subscriber once the run future is gone.
    let probes: Vec<_> = (0..PROBES)
        .map(|n| {
            tokio::spawn(async move {
                tokio::task::yield_now().await;
                tracing::info!("after-cancel probe {n}");
            })
        })
        .collect();
    for probe in probes {
        probe.await.expect("probe task must not panic");
    }
    assert_eq!(global_events_containing("after-cancel probe"), PROBES);
}
