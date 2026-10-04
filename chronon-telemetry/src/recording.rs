//! In-memory [`TelemetrySink`] for tests.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::TelemetrySink;

/// Captured counter increment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedCounter {
    /// Metric name (for example `chronon_runs_started`).
    pub name: String,
    /// Label key/value pairs attached to the sample.
    pub labels: Vec<(String, String)>,
    /// Increment applied in this record.
    pub delta: u64,
}

/// Captured gauge sample.
#[derive(Debug, Clone, PartialEq)]
pub struct RecordedGauge {
    /// Metric name.
    pub name: String,
    /// Label key/value pairs attached to the sample.
    pub labels: Vec<(String, String)>,
    /// Absolute gauge value at record time.
    pub value: f64,
}

/// Captured structured log event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedEvent {
    /// Event schema identifier (for example `chronon_run_failed`).
    pub schema: String,
    /// String field map for the event payload.
    pub fields: HashMap<String, String>,
}

#[derive(Debug, Default)]
struct Inner {
    counters: Vec<RecordedCounter>,
    gauges: Vec<RecordedGauge>,
    events: Vec<RecordedEvent>,
}

/// Append-only in-memory sink for assertions in unit and integration tests.
#[derive(Debug, Clone)]
pub struct RecordingSink {
    inner: Arc<Mutex<Inner>>,
}

impl RecordingSink {
    /// Create an empty recording sink.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner::default())),
        }
    }

    /// Discard all captured counters, gauges, and events.
    pub fn clear(&self) {
        let mut inner = self.inner.lock();
        inner.counters.clear();
        inner.gauges.clear();
        inner.events.clear();
    }

    /// Clone of all recorded counter increments (append order).
    pub fn counters(&self) -> Vec<RecordedCounter> {
        self.inner.lock().counters.clone()
    }

    /// Clone of all recorded gauge samples (append order).
    pub fn gauges(&self) -> Vec<RecordedGauge> {
        self.inner.lock().gauges.clone()
    }

    /// Clone of all recorded events (append order).
    pub fn events(&self) -> Vec<RecordedEvent> {
        self.inner.lock().events.clone()
    }

    /// Filter counters by metric name and required label subset.
    pub fn recorded_counters_matching(
        &self,
        name: &str,
        label_subset: &[(&str, &str)],
    ) -> Vec<RecordedCounter> {
        self.counters()
            .into_iter()
            .filter(|c| c.name == name && labels_contain(&c.labels, label_subset))
            .collect()
    }

    /// Filter events by schema name.
    pub fn recorded_events_for(&self, schema: &str) -> Vec<RecordedEvent> {
        self.events()
            .into_iter()
            .filter(|e| e.schema == schema)
            .collect()
    }
}

fn labels_contain(labels: &[(String, String)], subset: &[(&str, &str)]) -> bool {
    subset.iter().all(|(k, v)| {
        labels
            .iter()
            .any(|(lk, lv)| lk.as_str() == *k && lv.as_str() == *v)
    })
}

impl TelemetrySink for RecordingSink {
    fn record_counter(&self, name: &str, labels: &[(&str, &str)], delta: u64) {
        let labels: Vec<(String, String)> = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        self.inner.lock().counters.push(RecordedCounter {
            name: name.to_string(),
            labels,
            delta,
        });
    }

    fn record_gauge(&self, name: &str, labels: &[(&str, &str)], value: f64) {
        let labels: Vec<(String, String)> = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        self.inner.lock().gauges.push(RecordedGauge {
            name: name.to_string(),
            labels,
            value,
        });
    }

    fn log_event(&self, schema: &str, fields: &[(&str, &str)]) {
        let fields: HashMap<String, String> = fields
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        self.inner.lock().events.push(RecordedEvent {
            schema: schema.to_string(),
            fields,
        });
    }
}

impl Default for RecordingSink {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn captures_counters_and_events() {
        let sink = RecordingSink::new();
        sink.record_counter(
            "chronon_runs_started",
            &[("script", "noop"), ("job", "daily")],
            1,
        );
        sink.log_event("chronon_run_failed", &[("status", "failed")]);

        assert_eq!(sink.counters().len(), 1);
        assert_eq!(sink.events().len(), 1);
        let hits = sink.recorded_counters_matching("chronon_runs_started", &[("job", "daily")]);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].delta, 1);
    }

    #[test]
    fn clear_resets_buffers() {
        let sink = RecordingSink::new();
        sink.record_gauge("chronon_active_runs", &[], 2.0);
        sink.clear();
        assert_eq!(sink.gauges(), [] as [RecordedGauge; 0]);
    }
}
