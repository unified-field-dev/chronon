//! Optional policy for validating opaque `actor_json` at enqueue / upsert time.
//!
//! Chronon stores actor JSON as opaque data and rebuilds script context via
//! [`ContextFactory`](crate::ContextFactory). Hosts that map JSON to privileged
//! identities (for example a `System` shape) should install an [`ActorJsonPolicy`]
//! so untrusted HTTP paths cannot mint elevated actors.

use serde_json::Value;

use crate::error::{ChrononError, Result};

/// Trust level for an enqueue / upsert call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueueTrust {
    /// In-process / bootstrapped callers (may mint elevated actor shapes when policy allows).
    Internal,
    /// HTTP admin or other externally reachable surfaces.
    External,
}

/// Validates `actor_json` before a job is persisted.
///
/// # Errors
///
/// Implementations return [`ChrononError::ParamError`] when the actor is rejected.
pub trait ActorJsonPolicy: Send + Sync {
    /// Validate actor JSON for the given trust level.
    ///
    /// # Errors
    ///
    /// Returns an error when the actor must not be stored.
    fn validate(&self, trust: EnqueueTrust, actor_json: &Value) -> Result<()>;
}

/// Rejects System- and ServiceUser-shaped actors on [`EnqueueTrust::External`] paths.
///
/// Recognizes the `{"System": ...}` and `{"ServiceUser": ...}` object keys (case-sensitive),
/// matching common UF actor JSON. [`EnqueueTrust::Internal`] callers are never rejected.
#[derive(Debug, Default, Clone, Copy)]
pub struct RejectExternalPrivilegedActor;

/// Deprecated name for [`RejectExternalPrivilegedActor`].
#[deprecated(note = "renamed to `RejectExternalPrivilegedActor`")]
pub use RejectExternalPrivilegedActor as RejectExternalSystemActor;

impl ActorJsonPolicy for RejectExternalPrivilegedActor {
    fn validate(&self, trust: EnqueueTrust, actor_json: &Value) -> Result<()> {
        if trust == EnqueueTrust::External
            && (is_system_shaped_actor(actor_json) || is_service_user_shaped_actor(actor_json))
        {
            return Err(ChrononError::ParamError(
                "external actor_json cannot use a System or ServiceUser actor".into(),
            ));
        }
        Ok(())
    }
}

/// Default HTTP / external upsert actor (non-System service marker).
///
/// Shape: `{"Service":{"name":"chronon_api"}}`. Hosts that elevate privileges from actor JSON
/// must not treat this marker as System.
#[must_use]
pub fn default_http_enqueue_actor() -> Value {
    serde_json::json!({"Service": {"name": "chronon_api"}})
}

/// True when `actor_json` uses the well-known System object key.
#[must_use]
pub fn is_system_shaped_actor(actor_json: &Value) -> bool {
    actor_json.get("System").is_some()
}

/// True when `actor_json` uses the well-known ServiceUser object key.
#[must_use]
pub fn is_service_user_shaped_actor(actor_json: &Value) -> bool {
    actor_json.get("ServiceUser").is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reject_external_system() {
        let policy = RejectExternalPrivilegedActor;
        let system = serde_json::json!({"System": {"operation": "x"}});
        assert!(policy
            .validate(EnqueueTrust::External, &system)
            .unwrap_err()
            .to_string()
            .contains("System"));
        assert!(policy.validate(EnqueueTrust::Internal, &system).is_ok());
    }

    #[test]
    fn reject_external_service_user() {
        let policy = RejectExternalPrivilegedActor;
        let registered = serde_json::json!({"ServiceUser": {"service_name": "svc:7c9e6679-7425-40de-944b-e07fc1f90ae7"}});
        let infra = serde_json::json!({"ServiceUser": {"service_name": "metrics-ingest"}});
        for actor in [&registered, &infra] {
            assert!(is_service_user_shaped_actor(actor));
            assert!(policy
                .validate(EnqueueTrust::External, actor)
                .unwrap_err()
                .to_string()
                .contains("ServiceUser"));
            assert!(policy.validate(EnqueueTrust::Internal, actor).is_ok());
        }
    }

    #[test]
    fn allow_external_user_shapes() {
        let policy = RejectExternalPrivilegedActor;
        let user = serde_json::json!({"User": {"user_id": "u1"}});
        assert!(!is_service_user_shaped_actor(&user));
        assert!(policy.validate(EnqueueTrust::External, &user).is_ok());
    }

    #[test]
    fn allow_external_service() {
        let policy = RejectExternalPrivilegedActor;
        let service = default_http_enqueue_actor();
        assert!(!is_system_shaped_actor(&service));
        assert!(policy.validate(EnqueueTrust::External, &service).is_ok());
    }
}
