//! JSON envelope shared by all Chronon API responses.

use axum::{http::StatusCode, Json};
use chronon_core::{redact_credentials_in_text, sanitize_error_message, ChrononError};
use serde::{Deserialize, Serialize};

/// Standard API wrapper: `success`, optional `data`, optional `error`.
///
/// Clients should check `success` before reading `data`; HTTP status may still be 200 on logical errors for some routes.
#[derive(Debug, Serialize, Deserialize)]
pub struct ApiResponse<T> {
    /// `true` when `data` is populated and no error occurred.
    pub success: bool,
    /// Payload on success.
    pub data: Option<T>,
    /// Human-readable error when `success` is false.
    pub error: Option<String>,
}

impl<T> ApiResponse<T> {
    /// Success response with payload.
    pub fn ok(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
        }
    }

    /// Error response with message and no data.
    pub fn err(error: impl Into<String>) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(sanitize_error_message(&redact_credentials_in_text(
                &error.into(),
            ))),
        }
    }
}

/// Map a [`ChrononError`] to an HTTP status code for Chronon API routes.
pub fn chronon_status(err: &ChrononError) -> StatusCode {
    match err {
        ChrononError::JobNotFound(_) | ChrononError::RunNotFound(_) => StatusCode::NOT_FOUND,
        ChrononError::ScriptNotFound(_)
        | ChrononError::InvalidCron(_)
        | ChrononError::InvalidTimezone(_)
        | ChrononError::ParamError(_)
        | ChrononError::Identity(_) => StatusCode::BAD_REQUEST,
        ChrononError::ScriptMismatch { .. } | ChrononError::JobNameTaken { .. } => {
            StatusCode::CONFLICT
        }
        ChrononError::StorageError { .. }
        | ChrononError::Internal(_)
        | ChrononError::InternalSource { .. } => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// Error envelope with status derived from [`chronon_status`].
pub fn chronon_err<T>(err: &ChrononError) -> (StatusCode, Json<ApiResponse<T>>) {
    (chronon_status(err), Json(ApiResponse::err(err.to_string())))
}
