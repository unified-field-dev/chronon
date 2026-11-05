//! Error types for Chronon.

use std::error::Error;

use thiserror::Error;

/// Result type alias for Chronon operations.
pub type Result<T> = std::result::Result<T, ChrononError>;

/// Errors that can occur in Chronon operations.
///
/// Returned by [`SchedulerStore`](crate::store::SchedulerStore) implementations, the runtime
/// builder, and script dispatch. Hosts typically map these to HTTP status codes or log events.
#[derive(Debug, Error)]
pub enum ChrononError {
    /// No script with the requested name is registered or persisted.
    #[error("script not found: {0}")]
    ScriptNotFound(String),

    /// No job with the requested id or name exists in storage.
    #[error("job not found: {0}")]
    JobNotFound(String),

    /// No run with the requested id exists in storage.
    #[error("run not found: {0}")]
    RunNotFound(String),

    /// Cron expression failed validation (syntax or unsupported field).
    #[error("invalid cron expression: {0}")]
    InvalidCron(String),

    /// IANA timezone string could not be parsed.
    #[error("invalid timezone: {0}")]
    InvalidTimezone(String),

    /// Job parameters, actor JSON, or handler inputs failed validation or deserialization.
    #[error("parameter error: {0}")]
    ParamError(String),

    /// Job references a script name that does not match the registered script identity.
    #[error("script mismatch for job '{job_name}': expected '{expected}', got '{actual}'")]
    ScriptMismatch {
        /// Script name recorded on the job revision.
        expected: String,
        /// Script name resolved from the live registry or request.
        actual: String,
        /// Human-readable job name for error messages.
        job_name: String,
    },

    /// Another job already uses this [`Job::job_name`](crate::models::Job::job_name) under a
    /// different `job_id`. Reuse `existing_job_id` to update that job, or delete it first.
    #[error("job name '{job_name}' is already used by job '{existing_job_id}'")]
    JobNameTaken {
        /// The job name that collided.
        job_name: String,
        /// `job_id` of the job that already holds the name.
        existing_job_id: String,
    },

    /// Underlying storage backend failed or returned an unexpected condition.
    #[error("storage error: {message}")]
    StorageError {
        /// Human-readable summary (stable for logs and HTTP bodies).
        message: String,
        /// Optional underlying backend error for `Error::source` chains.
        #[source]
        source: Option<Box<dyn Error + Send + Sync>>,
    },

    /// Identity / actor reconstruction failed when building script context.
    #[error("identity error: {0}")]
    Identity(String),

    /// Catch-all for invariant violations and bugs.
    #[error("internal error: {0}")]
    Internal(String),

    /// Internal failure that preserves an underlying [`Error::source`] chain (e.g. transport).
    #[error("internal error: {message}")]
    InternalSource {
        /// Human-readable summary.
        message: String,
        /// Underlying cause.
        #[source]
        source: Box<dyn Error + Send + Sync>,
    },
}

impl ChrononError {
    /// Storage failure without an underlying source.
    pub fn storage(message: impl Into<String>) -> Self {
        Self::StorageError {
            message: message.into(),
            source: None,
        }
    }

    /// Storage failure wrapping an underlying error.
    pub fn storage_source(
        message: impl Into<String>,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self::StorageError {
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }

    /// Internal failure wrapping an underlying error (preserves `Error::source`).
    pub fn internal_source(
        message: impl Into<String>,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self::InternalSource {
            message: message.into(),
            source: Box::new(source),
        }
    }
}

impl From<serde_json::Error> for ChrononError {
    fn from(err: serde_json::Error) -> Self {
        Self::ParamError(err.to_string())
    }
}
