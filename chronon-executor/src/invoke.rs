//! Synchronous script invocation (registry lookup, context build, invoke).

use std::sync::Arc;

use chronon_core::{ChrononError, ContextFactory, Result};
use chronon_telemetry::{
    CapturedLogs, ChrononLogCapture, TelemetrySink, DEFAULT_MAX_CAPTURE_BYTES,
};
use serde_json::Value;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;

use crate::registry::ScriptRegistry;

/// Inputs for a single script execution attempt.
pub struct ExecuteScriptRequest<'a> {
    /// Script registry containing the target handler.
    pub registry: &'a ScriptRegistry,
    /// Factory that rebuilds [`ScriptContext`](chronon_core::ScriptContext) from stored actor JSON.
    pub context_factory: &'a Arc<dyn ContextFactory>,
    /// Sink for executor metrics and error events.
    pub telemetry: &'a Arc<dyn TelemetrySink>,
    /// Registered script name to invoke.
    pub script_name: &'a str,
    /// Actor JSON persisted on the job at schedule time.
    pub actor_json: &'a Value,
    /// Run-specific parameters JSON.
    pub params_json: Value,
    /// Human-readable job name for telemetry.
    pub job_name: &'a str,
    /// Run identifier for telemetry correlation.
    pub run_id: &'a str,
}

/// Outcome of [`execute_script`]: handler result plus captured tracing text.
#[derive(Debug)]
pub struct ExecuteScriptOutcome {
    /// Handler / lookup / context-build result.
    pub result: Result<()>,
    /// Tracing capture for persistence on the run row (including on failure).
    pub logs: CapturedLogs,
}

fn record_executor_error(
    telemetry: &Arc<dyn TelemetrySink>,
    job_name: &str,
    run_id: &str,
    script_name: &str,
    phase: &str,
    message: &str,
) {
    telemetry.log_event(
        "chronon_executor_error",
        &[
            ("job_name", job_name),
            ("run_id", run_id),
            ("script_name", script_name),
            ("phase", phase),
            ("message", message),
        ],
    );
}

/// Execute a script and capture tracing output for the run record.
///
/// Scopes a [`ChrononLogCapture`] dispatcher to the invoke future (not the calling thread)
/// so the script's info/warn/error events are buffered and other tasks' events are not.
/// Logs are returned on **both** success and failure (failure also ensures `stderr_text`
/// includes the error message).
#[tracing::instrument(
    skip(req),
    fields(
        script_name = %req.script_name,
        job_name = %req.job_name,
        run_id = %req.run_id,
    )
)]
pub async fn execute_script(req: ExecuteScriptRequest<'_>) -> ExecuteScriptOutcome {
    let ExecuteScriptRequest {
        registry,
        context_factory,
        telemetry,
        script_name,
        actor_json,
        params_json,
        job_name,
        run_id,
    } = req;

    let capture = ChrononLogCapture::new(DEFAULT_MAX_CAPTURE_BYTES);
    let subscriber = Registry::default().with(capture.clone());
    let dispatch = tracing::dispatcher::Dispatch::new(subscriber);
    let scope = capture.enter();

    // Must stay scoped to the future: a thread-local default held across `.await` leaks into
    // other tasks sharing the worker thread and misses the script after it migrates.
    let result = invoke_inner(ExecuteScriptRequest {
        registry,
        context_factory,
        telemetry,
        script_name,
        actor_json,
        params_json,
        job_name,
        run_id,
    })
    .with_subscriber(dispatch)
    .await;

    let mut logs = scope.finish();

    if let Err(ref e) = result {
        logs.ensure_stderr_message(&e.to_string());
    }

    ExecuteScriptOutcome { result, logs }
}

async fn invoke_inner(
    ExecuteScriptRequest {
        registry,
        context_factory,
        telemetry,
        script_name,
        actor_json,
        params_json,
        job_name,
        run_id,
    }: ExecuteScriptRequest<'_>,
) -> Result<()> {
    let descriptor = registry.get_or_err(script_name).inspect_err(|e| {
        record_executor_error(
            telemetry,
            job_name,
            run_id,
            script_name,
            "registry_lookup",
            &e.to_string(),
        );
    })?;

    let ctx = context_factory.build(actor_json).inspect_err(|e| {
        record_executor_error(
            telemetry,
            job_name,
            run_id,
            script_name,
            "context_build",
            &e.to_string(),
        );
    })?;

    (descriptor.invoke)(ctx, params_json).await.map_err(|e| {
        record_executor_error(
            telemetry,
            job_name,
            run_id,
            script_name,
            "script_invoke",
            &e.to_string(),
        );
        map_invoke_error(e)
    })
}

fn map_invoke_error(err: ChrononError) -> ChrononError {
    match err {
        ChrononError::ParamError(_)
        | ChrononError::ScriptNotFound(_)
        | ChrononError::Identity(_)
        | ChrononError::InvalidCron(_)
        | ChrononError::InvalidTimezone(_)
        | ChrononError::ScriptMismatch { .. } => err,
        ChrononError::Internal(message) if is_likely_param_error(&message) => {
            ChrononError::ParamError(message)
        }
        other => other,
    }
}

fn is_likely_param_error(message: &str) -> bool {
    const PARAM_ERROR_HINTS: [&str; 6] = [
        "missing field",
        "invalid type",
        "expected",
        "unknown field",
        "parameter error",
        "deserializing",
    ];
    let lower = message.to_ascii_lowercase();
    PARAM_ERROR_HINTS.iter().any(|h| lower.contains(h))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::descriptor::ScriptDescriptor;
    use chronon_core::{NoOpContextFactory, Result, ScriptContext};
    use serde_json::Value;
    use std::future::Future;
    use std::pin::Pin;

    fn noop_invoke(
        _ctx: Box<dyn ScriptContext>,
        _params: Value,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
        Box::pin(async {
            tracing::info!("noop ran");
            Ok(())
        })
    }

    fn fail_invoke(
        _ctx: Box<dyn ScriptContext>,
        _params: Value,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
        Box::pin(async {
            tracing::warn!("about to fail");
            Err(ChrononError::Internal("probe failure".into()))
        })
    }

    #[tokio::test]
    async fn execute_registered_script() {
        let mut registry = ScriptRegistry::new();
        registry.register(&ScriptDescriptor::new("test_script", noop_invoke));
        let factory: Arc<dyn chronon_core::ContextFactory> = Arc::new(NoOpContextFactory);
        let telemetry: Arc<dyn chronon_telemetry::TelemetrySink> =
            Arc::new(chronon_telemetry::NoOpSink);
        let outcome = execute_script(ExecuteScriptRequest {
            registry: &registry,
            context_factory: &factory,
            telemetry: &telemetry,
            script_name: "test_script",
            actor_json: &Value::Null,
            params_json: Value::Object(serde_json::Map::default()),
            job_name: "job",
            run_id: "run-1",
        })
        .await;
        assert!(outcome.result.is_ok());
        assert!(
            outcome
                .logs
                .stdout_text
                .as_deref()
                .is_some_and(|s| s.contains("noop ran")),
            "stdout={:?}",
            outcome.logs.stdout_text
        );
    }

    #[tokio::test]
    async fn failed_script_still_returns_captured_logs() {
        let mut registry = ScriptRegistry::new();
        registry.register(&ScriptDescriptor::new("fail_script", fail_invoke));
        let factory: Arc<dyn chronon_core::ContextFactory> = Arc::new(NoOpContextFactory);
        let telemetry: Arc<dyn chronon_telemetry::TelemetrySink> =
            Arc::new(chronon_telemetry::NoOpSink);
        let outcome = execute_script(ExecuteScriptRequest {
            registry: &registry,
            context_factory: &factory,
            telemetry: &telemetry,
            script_name: "fail_script",
            actor_json: &Value::Null,
            params_json: Value::Object(serde_json::Map::default()),
            job_name: "job",
            run_id: "run-fail",
        })
        .await;
        assert!(outcome.result.is_err());
        assert!(
            outcome
                .logs
                .stderr_text
                .as_deref()
                .is_some_and(|s| s.contains("about to fail") || s.contains("probe failure")),
            "stderr={:?}",
            outcome.logs.stderr_text
        );
    }
}
